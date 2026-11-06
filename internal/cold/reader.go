// Package cold reads only committed native packs. It never restores a VM data directory.
package cold

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	Repository         catalog.Repository
	BlobLocator        catalog.BlobLocator
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
func (r *Reader) expectedContract() (catalog.Contract, error) {
	if r.Store == nil || (r.CatalogStore == nil) == (r.Repository == nil) || r.Window.Milliseconds() <= 0 || r.Window%time.Millisecond != 0 || r.Age <= 0 || r.Age%time.Millisecond != 0 || r.LatenessMS < 0 || r.Age.Milliseconds() <= r.LatenessMS || len(r.Sources) == 0 || len(r.Tenants) == 0 || len(r.SourceFingerprints) != len(r.Sources) || r.CoverageStart < 0 || r.DedupMS <= 0 || r.MaxQueryBytes < 0 || r.CacheBytes < 0 {
		return catalog.Contract{}, errors.New("invalid Cold reader configuration")
	}
	want := catalog.Contract{CoverageStart: r.CoverageStart, WindowMS: r.Window.Milliseconds(), DedupMS: r.DedupMS, AgeMS: r.Age.Milliseconds(), LatenessMS: r.LatenessMS}
	for tenant, enabled := range r.Tenants {
		if !enabled {
			return catalog.Contract{}, errors.New("configured tenant must be enabled")
		}
		want.Tenants = append(want.Tenants, tenant)
	}
	seen := make(map[string]bool, len(r.Sources))
	for _, source := range r.Sources {
		if seen[source] {
			return catalog.Contract{}, errors.New("duplicate configured source")
		}
		seen[source] = true
		want.Sources = append(want.Sources, catalog.Source{ID: source, Fingerprint: r.SourceFingerprints[source]})
	}
	return want, nil
}

func (r *Reader) Catalog(ctx context.Context) (*catalog.Manifest, error) {
	want, e := r.expectedContract()
	if e != nil {
		return nil, e
	}
	if r.Repository != nil {
		return nil, errors.New("repository readers use bounded selection, not a full manifest")
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
	if !catalog.EqualContract(m.Contract, want) {
		return nil, errors.New("catalog immutable contract differs from reader configuration")
	}
	return m, nil
}

// CatalogStatus validates the fixed authority and blob location. Contract and
// locator cannot change during this catalog's lifetime, so a subsequent Select
// can safely take a newer publication snapshot without reinterpreting its keys.
func (r *Reader) CatalogStatus(ctx context.Context) (catalog.Status, error) {
	want, err := r.expectedContract()
	if err != nil {
		return catalog.Status{}, err
	}
	if r.Repository == nil {
		return catalog.Status{}, errors.New("catalog repository is not configured")
	}
	state, err := r.Repository.Status(ctx)
	if err != nil {
		return state, err
	}
	if !catalog.EqualContract(state.Contract, want) {
		return state, errors.New("catalog immutable contract differs from reader configuration")
	}
	blob, err := catalog.NormalizeBlobLocator(r.BlobLocator)
	if err != nil {
		return state, err
	}
	if state.Blob != blob {
		return state, errors.New("catalog blob locator differs from reader configuration")
	}
	return state, nil
}

func repositoryCoverage(state catalog.Status) map[string]int64 {
	out := make(map[string]int64, len(state.Contract.Tenants))
	for _, stream := range state.Coverage {
		previous, ok := out[stream.Tenant]
		if !ok || stream.End < previous {
			out[stream.Tenant] = stream.End
		}
	}
	return out
}

func (r *Reader) Ready(ctx context.Context) (map[string]int64, error) {
	if r.Repository != nil {
		state, err := r.CatalogStatus(ctx)
		if err != nil {
			return nil, err
		}
		out := repositoryCoverage(state)
		need := r.RequiredEnd(time.Now())
		for tenant, end := range out {
			if end < need {
				return out, fmt.Errorf("archive coverage for %s ends at %d; required %d", tenant, end, need)
			}
		}
		return out, nil
	}
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
	limit := r.MaxQueryBytes
	if limit <= 0 {
		limit = 128 << 20
	}
	selected, coverage, e := r.selectPacks(ctx, tenant, sq, limit)
	if e != nil {
		return nil, e
	}
	required := r.RequiredEnd(time.Now())
	if coverage < required && sq.MaxTimestamp >= coverage && sq.MinTimestamp < required {
		return nil, fmt.Errorf("unarchived required interval [%d,%d)", coverage, required)
	}
	matcher, e := gateway.CompileMatcher(sq.TagFilterss)
	if e != nil {
		return nil, e
	}
	var used int64
	var result []storage.MetricBlock
	for _, entry := range selected {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		ref := entry.Ref
		if entry.CheckDescriptor && ref.IndexBytes > limit-used {
			return nil, errors.New("query index/read byte limit exceeded")
		}
		data, e := r.cached(ctx, ref.IndexKey, 0, -1, ref.IndexSHA256)
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
		if entry.CheckDescriptor && (int64(len(data)) != ref.IndexBytes || idx.Size != ref.DataBytes || idx.SHA256 != ref.DataSHA256) {
			return nil, errors.New("index disagrees with committed pack descriptor")
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
				raw, e := r.cached(ctx, ref.DataKey, b.Offset, b.Length, b.SHA256)
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

type selectedPack struct {
	Start, End      int64
	Ref             catalog.PackRef
	CheckDescriptor bool
}

func (r *Reader) selectPacks(ctx context.Context, tenant string, sq *storage.SearchQuery, limit int64) ([]selectedPack, int64, error) {
	var out []selectedPack
	if r.Repository == nil {
		m, err := r.Catalog(ctx)
		if err != nil {
			return nil, 0, err
		}
		for _, entry := range m.Entries {
			if entry.Tenant == tenant && entry.End > sq.MinTimestamp && entry.Start <= sq.MaxTimestamp {
				out = append(out, selectedPack{Start: entry.Start, End: entry.End, Ref: catalog.PackRef{
					DataKey: entry.DataKey, IndexKey: entry.IndexKey, IndexSHA256: entry.IndexSHA256,
				}})
			}
		}
		return out, catalog.Coverage(m, tenant, r.Sources, r.CoverageStart), nil
	}
	state, err := r.CatalogStatus(ctx)
	if err != nil {
		return nil, 0, err
	}
	start := max(sq.MinTimestamp, r.CoverageStart)
	end := sq.MaxTimestamp
	if end < math.MaxInt64 {
		end++
	}
	// A committed half-open window can never contain MaxInt64 itself. Metadata
	// requests commonly use that inclusive upper bound; avoid overflowing it.
	if start >= end {
		return nil, repositoryCoverage(state)[tenant], nil
	}
	plan, err := r.Repository.Select(ctx, catalog.Selection{
		Tenant: tenant, Start: start, End: end, MaxPlanBytes: min(limit, 16<<20),
	})
	if err != nil {
		return nil, 0, err
	}
	for _, window := range plan.Windows {
		for _, ref := range window.Packs {
			out = append(out, selectedPack{Start: window.Start, End: window.End, Ref: ref, CheckDescriptor: true})
		}
	}
	return out, plan.CoverageEnd, nil
}
