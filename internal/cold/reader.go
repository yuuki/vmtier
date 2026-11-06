// Package cold reads only committed native packs. It never restores a VM data directory.
package cold

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
	"github.com/yuuki/vmtier/internal/querylimit"
)

type Reader struct {
	// Blob storage is not a catalog authority. Published keys are immutable by
	// allocation policy; a fresh nonce namespaces each archive attempt.
	Store              objectstore.BlobStore
	CatalogStore       catalog.Store
	SQLiteCatalog      *catalog.SQLite
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
	cache              immutableCache
	work               workCounters
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
	if r.Store == nil || (r.CatalogStore == nil) == (r.SQLiteCatalog == nil) || r.Window.Milliseconds() <= 0 || r.Window%time.Millisecond != 0 || r.Age <= 0 || r.Age%time.Millisecond != 0 || r.LatenessMS < 0 || r.Age.Milliseconds() <= r.LatenessMS || len(r.Sources) == 0 || len(r.Tenants) == 0 || len(r.SourceFingerprints) != len(r.Sources) || r.CoverageStart < 0 || r.DedupMS <= 0 || r.MaxQueryBytes < 0 || r.CacheBytes < 0 {
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
	if r.SQLiteCatalog != nil {
		return nil, errors.New("SQLite readers use bounded selection, not a full manifest")
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

// SQLiteStatus validates the fixed authority and blob location. Contract and
// locator cannot change during this catalog's lifetime, so a subsequent Select
// can safely take a newer publication snapshot without reinterpreting its keys.
func (r *Reader) SQLiteStatus(ctx context.Context) (catalog.SQLiteStatus, error) {
	want, err := r.expectedContract()
	if err != nil {
		return catalog.SQLiteStatus{}, err
	}
	if r.SQLiteCatalog == nil {
		return catalog.SQLiteStatus{}, errors.New("SQLite catalog is not configured")
	}
	state, err := r.SQLiteCatalog.Status(ctx)
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

func sqliteCoverage(state catalog.SQLiteStatus) map[string]int64 {
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
	if r.SQLiteCatalog != nil {
		state, err := r.SQLiteStatus(ctx)
		if err != nil {
			return nil, err
		}
		out := sqliteCoverage(state)
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
	ck := cacheKey(key, offset, length, sum)
	b, ok := r.cacheGet(ck)
	if ok {
		r.CacheHits.Add(1)
		return b, nil
	}
	r.CacheMisses.Add(1)
	return r.sharedRead(ctx, ck, func(readCtx context.Context) ([]byte, error) {
		// A previous flight may have filled the cache after our first lookup.
		if cached, ok := r.cacheGet(ck); ok {
			return cached, nil
		}
		var data []byte
		var err error
		if length < 0 {
			data, err = r.Store.Get(readCtx, key)
		} else {
			data, err = r.Store.GetRange(readCtx, key, offset, length)
		}
		if err != nil {
			return nil, err
		}
		if length >= 0 && int64(len(data)) != length {
			return nil, errors.New("Range GET length differs from directory")
		}
		if pack.Sum(data) != sum {
			return nil, errors.New("immutable object checksum mismatch")
		}
		if err := readCtx.Err(); err != nil {
			return nil, err
		}
		r.cachePut(ck, data)
		return data, nil
	})
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
	var work queryWork
	defer r.recordWork(&work)
	var result []storage.MetricBlock
	for _, entry := range selected {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		ref := entry.Ref
		if entry.CheckDescriptor && ref.IndexBytes > limit-work.used() {
			return nil, querylimit.Errorf(querylimit.ReadBytes, "query index/read byte limit exceeded")
		}
		data, e := r.cached(ctx, ref.IndexKey, 0, -1, ref.IndexSHA256)
		if e != nil {
			return nil, e
		}
		work.read += int64(len(data))
		work.observe()
		if work.used() > limit {
			return nil, querylimit.Errorf(querylimit.ReadBytes, "query index/read byte limit exceeded")
		}
		idx, e := pack.ParseIndex(data, pack.Limits{})
		if e != nil {
			return nil, e
		}
		// Only one parsed directory is needed at a time. Charge its structural
		// estimate while processing this pack, rather than summing historical
		// directories that are no longer retained by the query.
		work.index = pack.IndexMemoryEstimate(idx)
		work.observe()
		if work.used() > limit {
			return nil, querylimit.Errorf(querylimit.DecodedBytes, "query parsed-index work budget exceeded")
		}
		if idx.Tenant.String() != tenant || idx.Start != entry.Start || idx.End != entry.End {
			return nil, errors.New("index disagrees with committed catalog")
		}
		if entry.CheckDescriptor && (int64(len(data)) != ref.IndexBytes || idx.Size != ref.DataBytes || idx.SHA256 != ref.DataSHA256) {
			return nil, errors.New("index disagrees with committed pack descriptor")
		}
		var blocks []selectedBlock
		for i, s := range idx.Series {
			if !matcher.MatchLabels(s.Labels) {
				continue
			}
			for j, b := range s.Blocks {
				if b.MaxTimestamp < sq.MinTimestamp || b.MinTimestamp > sq.MaxTimestamp {
					continue
				}
				// No gaps are fetched: the sum of original lengths is exactly the
				// combined response size. Reserve all bytes before any payload I/O,
				// including cache hits and physical rows before clipping.
				// Decoding temporarily owns portable timestamp/value arrays as
				// well as the retained VM arrays. Charge both, plus copied native
				// names and per-block/selection/response bookkeeping allowances.
				read, decoded := b.Length, int64(b.Rows)*32
				resultEstimate := int64(len(s.NativeName))*2 + 512
				if read+decoded+resultEstimate > limit-work.used() || len(result)+len(blocks) >= 100000 {
					return nil, querylimit.Errorf(querylimit.DecodedBytes, "query decoded/read budget exceeded")
				}
				work.read += read
				work.decoded += decoded
				work.result += resultEstimate
				work.observe()
				blocks = append(blocks, selectedBlock{i, j, b})
			}
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].ref.Offset < blocks[j].ref.Offset })
		decoded, e := r.readBlocks(ctx, ref.DataKey, idx, blocks)
		if e != nil {
			return nil, e
		}
		result = append(result, decoded...)
		work.index = 0
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return result, nil
}

// Range groups stay within one immutable object and never overfetch gaps.
// A single block is bounded to at most 512 KiB by ParseIndex's default limits.
const maxCombinedRangeBytes int64 = 4 << 20

type selectedBlock struct {
	series, block int
	ref           pack.BlockRef
}

func (r *Reader) readBlocks(ctx context.Context, key string, idx *pack.Index, blocks []selectedBlock) ([]storage.MetricBlock, error) {
	var result []storage.MetricBlock
	for first := 0; first < len(blocks); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b := blocks[first].ref
		raw, hit := r.cacheGet(cacheKey(key, b.Offset, b.Length, b.SHA256))
		last := first + 1
		if hit {
			r.CacheHits.Add(1)
		} else {
			length := b.Length
			for last < len(blocks) {
				next := blocks[last].ref
				if next.Offset != b.Offset+length || next.Length > maxCombinedRangeBytes-length {
					break
				}
				if _, cached := r.cacheGet(cacheKey(key, next.Offset, next.Length, next.SHA256)); cached {
					break
				}
				length += next.Length
				last++
			}
			r.CacheMisses.Add(uint64(last - first))
			var err error
			group := blocks[first:last]
			// Hash the original block checksums as part of the immutable flight
			// identity: equal offsets with a changed directory cannot share data.
			sums := sha256.New()
			for _, selected := range group {
				_, _ = sums.Write([]byte(selected.ref.SHA256))
			}
			flightKey := cacheKey(key, b.Offset, length, hex.EncodeToString(sums.Sum(nil)))
			raw, err = r.sharedRead(ctx, flightKey, func(readCtx context.Context) ([]byte, error) {
				// Recheck every block after admission. A flight that just finished
				// may have populated all of this range while we were planning it.
				var cached []byte
				for _, selected := range group {
					ref := selected.ref
					part, ok := r.cacheGet(cacheKey(key, ref.Offset, ref.Length, ref.SHA256))
					if !ok {
						cached = nil
						break
					}
					cached = append(cached, part...)
				}
				if cached != nil {
					return cached, nil
				}
				data, err := r.Store.GetRange(readCtx, key, b.Offset, length)
				if err != nil {
					return nil, err
				}
				if int64(len(data)) != length {
					return nil, errors.New("Range GET length differs from directory")
				}
				// Verify the entire combined response before publishing any cache
				// entries or waking followers. Native decoding remains per query.
				for _, selected := range group {
					ref := selected.ref
					offset := ref.Offset - b.Offset
					if pack.Sum(data[offset:offset+ref.Length]) != ref.SHA256 {
						return nil, errors.New("immutable object checksum mismatch")
					}
				}
				if err := readCtx.Err(); err != nil {
					return nil, err
				}
				for _, selected := range group {
					ref := selected.ref
					offset := ref.Offset - b.Offset
					r.cachePut(cacheKey(key, ref.Offset, ref.Length, ref.SHA256), data[offset:offset+ref.Length])
				}
				return data, nil
			})
			if err != nil {
				return nil, err
			}
		}
		for _, selected := range blocks[first:last] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			ref := selected.ref
			offset := ref.Offset - b.Offset
			data := raw[offset : offset+ref.Length]
			// LoadBlock verifies each original checksum and native payload,
			// then clips to committed coverage. A combined checksum is not a
			// substitute, and any bad block discards the entire query result.
			mb, err := pack.LoadBlock(idx, selected.series, selected.block, data)
			if err != nil {
				return nil, err
			}
			if mb != nil {
				result = append(result, *mb)
			}
		}
		first = last
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
	if r.SQLiteCatalog == nil {
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
	state, err := r.SQLiteStatus(ctx)
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
		return nil, sqliteCoverage(state)[tenant], nil
	}
	plan, err := r.SQLiteCatalog.Select(ctx, catalog.Selection{
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
