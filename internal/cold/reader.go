// Package cold reads only committed native packs. It never restores a VM data directory.
package cold

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

type Reader struct {
	// Blob storage is not a catalog authority. Published keys are immutable by
	// allocation policy; a fresh nonce namespaces each archive attempt.
	Store              objectstore.BlobStore
	CatalogStore       catalog.Store
	Tenants            map[string]bool
	Sources            []string
	SourceFingerprints map[string]string
	CoverageStart      int64
	DedupMS            int64
	LatenessMS         int64
	Age, Window        time.Duration
	MaxQueryBytes      int64
	CacheBytes         int64
	mu                 sync.Mutex
	cache              map[string][]byte
	used               int64
	CacheHits          atomic.Uint64
	CacheMisses        atomic.Uint64
}

func (r *Reader) RequiredEnd(now time.Time) int64 {
	w := r.Window.Milliseconds()
	if w <= 0 {
		return r.CoverageStart
	}
	return (now.Add(-r.Age).UnixMilli() / w) * w
}
func (r *Reader) Catalog(ctx context.Context) (*catalog.Manifest, error) {
	if r.Store == nil || r.CatalogStore == nil || r.Window.Milliseconds() <= 0 || r.Window%time.Millisecond != 0 || r.Age <= 0 || r.Age%time.Millisecond != 0 || r.LatenessMS < 0 || r.Age.Milliseconds() <= r.LatenessMS || len(r.Sources) == 0 || len(r.Tenants) == 0 || len(r.SourceFingerprints) != len(r.Sources) || r.CoverageStart < 0 || r.DedupMS <= 0 || r.MaxQueryBytes < 0 || r.CacheBytes < 0 {
		return nil, errors.New("invalid Cold reader configuration")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Every request consults the authoritative store. Never use a cached
	// manifest or an S3 snapshot if the authority cannot be reached.
	m, generation, e := catalog.Load(ctx, r.CatalogStore)
	if e != nil {
		return nil, e
	}
	if m == nil || generation == 0 {
		return nil, errors.New("catalog unbound: archive has not committed")
	}
	want := catalog.Contract{CoverageStart: r.CoverageStart, WindowMS: r.Window.Milliseconds(), DedupMS: r.DedupMS, AgeMS: r.Age.Milliseconds(), LatenessMS: r.LatenessMS}
	for tenant, enabled := range r.Tenants {
		if !enabled {
			return nil, errors.New("configured tenant must be enabled")
		}
		want.Tenants = append(want.Tenants, tenant)
	}
	seen := make(map[string]bool, len(r.Sources))
	for _, source := range r.Sources {
		if seen[source] {
			return nil, errors.New("duplicate configured source")
		}
		seen[source] = true
		want.Sources = append(want.Sources, catalog.Source{ID: source, Fingerprint: r.SourceFingerprints[source]})
	}
	if !catalog.EqualContract(m.Contract, want) {
		return nil, errors.New("catalog immutable contract differs from reader configuration")
	}
	return m, nil
}
func (r *Reader) Ready(ctx context.Context) (map[string]int64, error) {
	m, e := r.Catalog(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string]int64{}
	need := r.RequiredEnd(time.Now())
	for t := range r.Tenants {
		end := catalog.Coverage(m, t, r.Sources, r.CoverageStart)
		out[t] = end
		if end < need {
			return out, fmt.Errorf("archive coverage for %s ends at %d; required %d", t, end, need)
		}
	}
	return out, nil
}
func (r *Reader) cached(ctx context.Context, key string, offset, length int64, sum string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ck := fmt.Sprintf("%s:%d:%d:%s", key, offset, length, sum)
	r.mu.Lock()
	b, ok := r.cache[ck]
	r.mu.Unlock()
	if ok {
		r.CacheHits.Add(1)
		return b, nil
	}
	r.CacheMisses.Add(1)
	var e error
	if length < 0 {
		b, e = r.Store.Get(ctx, key)
	} else {
		b, e = r.Store.GetRange(ctx, key, offset, length)
	}
	if e != nil {
		return nil, e
	}
	if length >= 0 && int64(len(b)) != length {
		return nil, errors.New("Range GET length differs from directory")
	}
	if pack.Sum(b) != sum {
		return nil, errors.New("immutable object checksum mismatch")
	}
	if r.CacheBytes > 0 && int64(len(b)) <= r.CacheBytes {
		r.mu.Lock()
		if r.cache == nil {
			r.cache = map[string][]byte{}
		}
		if _, ok := r.cache[ck]; !ok {
			if r.used+int64(len(b)) > r.CacheBytes {
				r.cache = map[string][]byte{}
				r.used = 0
			}
			r.cache[ck] = b
			r.used += int64(len(b))
		}
		r.mu.Unlock()
	}
	return b, nil
}
func (r *Reader) Search(ctx context.Context, sq *storage.SearchQuery) ([]storage.MetricBlock, error) {
	a, p, e := gateway.Tenant(sq)
	if e != nil {
		return nil, e
	}
	if sq.MinTimestamp > sq.MaxTimestamp {
		return nil, errors.New("invalid query interval")
	}
	tenant := fmt.Sprintf("%d:%d", a, p)
	if !r.Tenants[tenant] {
		return nil, errors.New("tenant not configured")
	}
	m, e := r.Catalog(ctx)
	if e != nil {
		return nil, e
	}
	coverage := catalog.Coverage(m, tenant, r.Sources, r.CoverageStart)
	required := r.RequiredEnd(time.Now())
	if coverage < required && sq.MaxTimestamp >= coverage && sq.MinTimestamp < required {
		return nil, fmt.Errorf("unarchived required interval [%d,%d)", coverage, required)
	}
	matcher, e := gateway.CompileMatcher(sq.TagFilterss)
	if e != nil {
		return nil, e
	}
	limit := r.MaxQueryBytes
	if limit <= 0 {
		limit = 128 << 20
	}
	var used int64
	var result []storage.MetricBlock
	for _, entry := range m.Entries {
		if entry.Tenant != tenant || entry.End <= sq.MinTimestamp || entry.Start > sq.MaxTimestamp {
			continue
		}
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		data, e := r.cached(ctx, entry.IndexKey, 0, -1, entry.IndexSHA256)
		if e != nil {
			return nil, e
		}
		used += int64(len(data))
		if used > limit {
			return nil, errors.New("query index/read byte limit exceeded")
		}
		idx, e := pack.ParseIndex(data, pack.Limits{})
		if e != nil {
			return nil, e
		}
		if idx.Tenant.String() != tenant || idx.Start != entry.Start || idx.End != entry.End {
			return nil, errors.New("index disagrees with committed catalog")
		}
		for i, s := range idx.Series {
			if !matcher.MatchLabels(s.Labels) {
				continue
			}
			for j, b := range s.Blocks {
				if b.MaxTimestamp < sq.MinTimestamp || b.MinTimestamp > sq.MaxTimestamp {
					continue
				}
				used += b.Length + int64(b.Rows)*16
				if used > limit || len(result) >= 100000 {
					return nil, errors.New("query decoded/read budget exceeded")
				}
				raw, e := r.cached(ctx, entry.DataKey, b.Offset, b.Length, b.SHA256)
				if e != nil {
					return nil, e
				}
				mb, e := pack.LoadBlock(idx, i, j, raw)
				if e != nil {
					return nil, e
				}
				if mb != nil {
					result = append(result, *mb)
				}
			}
		}
	}
	return result, nil
}
