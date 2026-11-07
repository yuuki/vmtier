package catalog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// Local is one authoritative catalog on a local persistent filesystem. flock
// protects compare-and-publish across processes sharing this directory. This is
// not an NFS/distributed-lock implementation and must not be replicated by copying
// its directory. The ID is supplied by configuration, never inferred from disk.
type Local struct {
	root   *os.Root
	id     string
	broken atomic.Bool
}

// NewLocal explicitly creates an empty authority when create is true, refusing
// an existing catalog. Opening an absent/corrupt catalog never bootstraps one.
func NewLocal(dir, id string, create bool) (*Local, error) {
	if !validID(id) {
		return nil, errors.New("invalid catalog ID")
	}
	if create {
		if e := os.MkdirAll(dir, 0700); e != nil {
			return nil, e
		}
	}
	r, e := os.OpenRoot(dir)
	if e != nil {
		if errors.Is(e, os.ErrNotExist) {
			return nil, ErrNotInitialized
		}
		return nil, e
	}
	s := &Local{root: r, id: id}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
	unlock, e := s.lock(context.Background())
	if e != nil {
		return nil, e
	}
	defer unlock()
	if create {
		if _, e := r.Lstat(Key); e == nil {
			return nil, errors.New("catalog already exists; initialization refuses overwrite")
		} else if !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
		b, e := marshal(Fresh(id))
		if e != nil {
			return nil, e
		}
		if e = s.write(context.Background(), b); e != nil {
			return nil, e
		}
		// Persist the newly-created authority directory itself as well.
		parent, e := os.Open(filepath.Dir(filepath.Clean(dir)))
		if e != nil {
			return nil, e
		}
		e = parent.Sync()
		parent.Close()
		if e != nil {
			return nil, e
		}
	} else {
		if _, e := s.read(); e != nil {
			return nil, e
		}
		if e := s.syncDir(); e != nil {
			return nil, e
		}
	}
	ok = true
	return s, nil
}
func (s *Local) Close() error { return s.root.Close() }
func (s *Local) lock(ctx context.Context) (func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	f, e := s.root.OpenFile(".authority.lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	for {
		e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
		}
		if !errors.Is(e, syscall.EWOULDBLOCK) && !errors.Is(e, syscall.EAGAIN) {
			f.Close()
			return nil, e
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func (s *Local) read() (*Manifest, error) {
	if s.broken.Load() {
		return nil, errors.New("catalog durability failure: authority requires recovery")
	}
	f, e := s.root.Open(Key)
	if errors.Is(e, os.ErrNotExist) {
		return nil, ErrNotInitialized
	}
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() || info.Size() > MaxBytes {
		return nil, errors.New("invalid catalog file")
	}
	b, e := io.ReadAll(io.LimitReader(f, MaxBytes+1))
	if e != nil {
		return nil, e
	}
	m, e := decode(b)
	if e != nil {
		return nil, e
	}
	if m.ID != s.id {
		return nil, errors.New("catalog identity mismatch")
	}
	return m, nil
}
func (s *Local) Read(ctx context.Context) (*Manifest, error) {
	unlock, e := s.lock(ctx)
	if e != nil {
		return nil, e
	}
	defer unlock()
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	m, e := s.read()
	if e != nil {
		return nil, e
	}
	if e := s.syncDir(); e != nil {
		return nil, e
	}
	return m, nil
}
func (s *Local) Publish(ctx context.Context, expected uint64, next *Manifest) error {
	// Encode first to snapshot caller-owned memory and bound persistent state.
	b, e := marshal(next)
	if e != nil {
		return e
	}
	candidate, e := decode(b)
	if e != nil {
		return e
	}
	unlock, e := s.lock(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	old, e := s.read()
	if e != nil {
		return e
	}
	same, e := checkTransition(old, candidate, expected)
	if e != nil {
		return e
	}
	if same {
		return s.syncDir()
	}
	return s.write(ctx, b)
}
func (s *Local) write(ctx context.Context, b []byte) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return e
	}
	tmp := ".upload-" + hex.EncodeToString(nonce[:])
	f, e := s.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	defer s.root.Remove(tmp)
	n, e := f.Write(b)
	if e == nil && n != len(b) {
		e = io.ErrShortWrite
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	if e = s.root.Rename(tmp, Key); e != nil {
		return e
	}
	return s.syncDir()
}

func (s *Local) syncDir() error {
	d, e := s.root.Open(".")
	if e != nil {
		s.broken.Store(true)
		return fmt.Errorf("catalog directory durability: %w", e)
	}
	e = d.Sync()
	d.Close()
	if e != nil {
		s.broken.Store(true)
		return fmt.Errorf("catalog directory durability: %w", e)
	}
	return nil
}
