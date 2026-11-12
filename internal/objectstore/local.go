package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"
	"time"
)

// Local is a filesystem-backed store for development. os.Root confines accesses
// to the configured directory, including symbolic links. Atomic rename and fsync
// make uploaded files durable; flock serializes writers across processes.
// Its no-overwrite check is a development safeguard, not a BlobStore guarantee.
type Local struct {
	root  *os.Root
	max   int64
	stats counters
}

func NewLocal(dir string, maxBytes int64) (*Local, error) {
	if maxBytes == 0 {
		maxBytes = DefaultMaxObjectBytes
	}
	if maxBytes < 1 || maxBytes > 1<<30 {
		return nil, fmt.Errorf("max object bytes must be in [1, 1GiB]")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	return &Local{root: r, max: maxBytes}, nil
}

func (s *Local) Close() error    { return s.root.Close() }
func (s *Local) Stats() Snapshot { return s.stats.snapshot() }

func (s *Local) get(key string) ([]byte, error) {
	f, err := s.root.Open(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || fi.Size() > s.max {
		return nil, fmt.Errorf("invalid or oversized object")
	}
	b, err := readBounded(f, s.max)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Local) Get(ctx context.Context, key string) ([]byte, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.stats.gets.Add(1)
	b, err := s.get(key)
	s.stats.downloaded.Add(uint64(len(b)))
	return b, err
}

func (s *Local) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	if err := checkRange(offset, length, s.max); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.stats.ranges.Add(1)
	f, err := s.root.Open(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || offset > fi.Size() || length > fi.Size()-offset {
		return nil, fmt.Errorf("range exceeds object length")
	}
	b := make([]byte, int(length))
	n, err := f.ReadAt(b, offset)
	s.stats.downloaded.Add(uint64(n))
	if err != nil {
		return nil, err
	}
	if int64(n) != length {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

func (s *Local) lock(ctx context.Context) (func(), error) {
	f, err := s.root.OpenFile(".writer.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *Local) Put(ctx context.Context, key string, data []byte) error {
	if err := validKey(key); err != nil {
		return err
	}
	if int64(len(data)) > s.max {
		return fmt.Errorf("object exceeds %d byte limit", s.max)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	old, err := s.get(key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil {
		if bytes.Equal(old, data) {
			// A previous attempt may have renamed successfully but failed while
			// syncing its directory. Re-establish durability before success.
			return s.syncDirs(key)
		}
		return ErrKeyReuse
	}
	if err := s.root.MkdirAll(path.Dir(key), 0700); err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	tmp := path.Join(path.Dir(key), ".upload-"+hex.EncodeToString(nonce[:]))
	f, err := s.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer s.root.Remove(tmp)
	s.stats.puts.Add(1)
	n, err := f.Write(data)
	s.stats.uploaded.Add(uint64(n))
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.Rename(tmp, key); err != nil {
		return err
	}
	return s.syncDirs(key)
}

var _ Deleter = (*Local)(nil)

// Delete removes one object under the writer lock and persists the removal.
// A missing object counts as deleted. An attempt directory left empty is
// removed too; directories that still hold objects are kept.
func (s *Local) Delete(ctx context.Context, key string) error {
	if err := validKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	s.stats.deletes.Add(1)
	info, err := s.root.Lstat(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to delete a non-regular object")
	}
	if err := s.root.Remove(key); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := path.Dir(key)
	if dir != "." && s.root.Remove(dir) == nil {
		dir = path.Dir(dir)
	}
	parent, err := s.root.Open(dir)
	if err != nil {
		return err
	}
	err = parent.Sync()
	closeErr := parent.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (s *Local) syncDirs(key string) error {
	// Persist newly created parent directory entries as well as the object rename.
	for name := path.Dir(key); ; name = path.Dir(name) {
		dir, err := s.root.Open(name)
		if err != nil {
			return err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return err
		}
		if name == "." {
			break
		}
	}
	return nil
}
