package objectstore

import (
	"path"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzKeyUnderPrefix checks the key rule shared by Local and S3: an accepted
// key is local to the store root, and the S3 object name built from a
// configured prefix (normalized as NewS3 does) stays under that prefix.
func FuzzKeyUnderPrefix(f *testing.F) {
	for _, seed := range []struct{ prefix, key string }{
		{"", "tenants/1:2/attempts/00/aa.pack"},
		{"archive/vmtier/", "tenants/1:2/x.json"},
		{"archive", "../escape"},
		{"a/b", "tenants/./x"},
		{"a", "tenants/.upload-1"},
		{"a", "tenants/x\\y"},
	} {
		f.Add(seed.prefix, seed.key)
	}
	f.Fuzz(func(t *testing.T, prefix, key string) {
		if validKey(key) != nil {
			return
		}
		if !filepath.IsLocal(key) || path.Clean(key) != key {
			t.Fatalf("accepted key %q is not a clean local path", key)
		}
		for _, part := range strings.Split(key, "/") {
			if strings.HasPrefix(part, ".") {
				t.Fatalf("accepted key %q has a hidden or relative component", key)
			}
		}
		prefix = strings.TrimSuffix(prefix, "/")
		if prefix == "" || validKey(prefix) != nil {
			return
		}
		name := prefix + "/" + key
		if path.Clean(name) != name || !strings.HasPrefix(name, prefix+"/") {
			t.Fatalf("object %q escapes prefix %q", name, prefix)
		}
	})
}

// FuzzCheckRange checks that an accepted range is within bounds and that its
// end offset cannot overflow.
func FuzzCheckRange(f *testing.F) {
	f.Add(int64(0), int64(1), int64(1))
	f.Add(int64(1<<62), int64(1<<62), DefaultMaxObjectBytes)
	f.Add(int64(-1), int64(1), DefaultMaxObjectBytes)
	f.Add(int64(9223372036854775806), int64(2), DefaultMaxObjectBytes)
	f.Fuzz(func(t *testing.T, offset, length, max int64) {
		if checkRange(offset, length, max) != nil {
			return
		}
		if offset < 0 || length < 1 || length > max {
			t.Fatalf("accepted range offset=%d length=%d max=%d", offset, length, max)
		}
		if end := offset + length; end < offset {
			t.Fatalf("range end overflows: %d+%d", offset, length)
		}
	})
}
