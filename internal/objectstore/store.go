// Package objectstore supplies blob I/O, without catalog or CAS operations.
// Callers allocate a fresh key for each archive attempt, retry only identical
// bytes under that key, and verify application-level SHA-256 checksums.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync/atomic"
)

var (
	ErrNotFound = errors.New("object not found")
	ErrKeyReuse = errors.New("blob key reused with different bytes")
)

const DefaultMaxObjectBytes int64 = 64 << 20

// BlobStore deliberately has no compare-and-swap, ETag, list or delete API.
// Put is an ordinary upload, not a conditional create. Callers must never use
// it to overwrite published objects or mutable catalog state. Local catches
// accidental key reuse; S3 does not promise to reject an existing key.
type BlobStore interface {
	Get(context.Context, string) ([]byte, error)
	GetRange(context.Context, string, int64, int64) ([]byte, error)
	Put(context.Context, string, []byte) error
}

type Snapshot struct {
	Gets            uint64 `json:"gets"`
	RangeGets       uint64 `json:"range_gets"`
	Puts            uint64 `json:"puts"`
	DownloadedBytes uint64 `json:"downloaded_bytes"`
	UploadedBytes   uint64 `json:"uploaded_bytes"`
}

type counters struct {
	gets, ranges, puts, downloaded, uploaded atomic.Uint64
}

func (c *counters) snapshot() Snapshot {
	return Snapshot{c.gets.Load(), c.ranges.Load(), c.puts.Load(), c.downloaded.Load(), c.uploaded.Load()}
}

func validKey(key string) error {
	if key == "" || len(key) > 1024 || strings.HasPrefix(key, "/") || path.Clean(key) != key || strings.ContainsAny(key, "\\\x00\r\n") {
		return fmt.Errorf("invalid object key")
	}
	for _, part := range strings.Split(key, "/") {
		if strings.HasPrefix(part, ".") {
			return fmt.Errorf("invalid object key component")
		}
	}
	return nil
}

func checkRange(offset, length, max int64) error {
	if offset < 0 || length <= 0 || length > max || offset > int64(^uint64(0)>>1)-length {
		return fmt.Errorf("invalid or oversized object range")
	}
	return nil
}

func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("object exceeds %d byte limit", max)
	}
	return b, nil
}
