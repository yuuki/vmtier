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
	// Streams added later make the stored contract cover this configuration.
	if !catalog.ContractCovers(state.Contract, want) {
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

// configuredCoverage is the coverage end of each configured tenant over its
// configured sources. Streams added to the catalog after this process's
// configuration do not make it unready: a process configured with them
// reports their lag (ADR 0008).
func (r *Reader) configuredCoverage(state catalog.Status) map[string]int64 {
	sources := make(map[string]bool, len(r.Sources))
	for _, source := range r.Sources {
		sources[source] = true
	}
	configured := state
	configured.Coverage = nil
	for _, stream := range state.Coverage {
		if r.Tenants[stream.Tenant] && sources[stream.SourceID] {
			configured.Coverage = append(configured.Coverage, stream)
		}
	}
	return repositoryCoverage(configured)
}

func (r *Reader) Ready(ctx context.Context) (map[string]int64, error) {
	if r.Repository != nil {
		state, err := r.CatalogStatus(ctx)
		if err != nil {
			return nil, err
		}
		out := r.configuredCoverage(state)
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
	search := packSearch{r: r, sq: sq, tenant: tenant, matcher: matcher, limit: limit}
	defer r.recordWork(&search.work)
	for _, entry := range selected {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		if e := search.readPack(ctx, entry); e != nil {
			return nil, e
		}
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return search.result, nil
}

// packSearch collects the blocks of one Search, charging its logical work
// against limit.
type packSearch struct {
	r       *Reader
	sq      *storage.SearchQuery
	tenant  string
	matcher *gateway.Matcher
	limit   int64
	work    queryWork
	result  []storage.MetricBlock
}

// readPack appends the matching blocks of one selected pack to the result.
func (s *packSearch) readPack(ctx context.Context, entry selectedPack) error {
	idx, err := s.readIndex(ctx, entry)
	if err != nil {
		return err
	}
	blocks, err := s.selectBlocks(idx)
	if err != nil {
		return err
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].ref.Offset < blocks[j].ref.Offset })
	decoded, err := s.r.readBlocks(ctx, entry.Ref.DataKey, idx, blocks)
	if err != nil {
		return err
	}
	s.result = append(s.result, decoded...)
	s.work.index = 0
	return nil
}

// readIndex reads and parses the directory of a selected pack, charging both
// its bytes and its parsed size, and checks it against the catalog.
func (s *packSearch) readIndex(ctx context.Context, entry selectedPack) (*pack.Index, error) {
	ref := entry.Ref
	if entry.CheckDescriptor && ref.IndexBytes > s.limit-s.work.used() {
		return nil, querylimit.Errorf(querylimit.ReadBytes, "query index/read byte limit exceeded")
	}
	data, err := s.r.cached(ctx, ref.IndexKey, 0, -1, ref.IndexSHA256)
	if err != nil {
		return nil, err
	}
	s.work.read += int64(len(data))
	s.work.observe()
	if s.work.used() > s.limit {
		return nil, querylimit.Errorf(querylimit.ReadBytes, "query index/read byte limit exceeded")
	}
	idx, err := pack.ParseIndex(data, pack.Limits{})
	if err != nil {
		return nil, err
	}
	// Only one parsed directory is needed at a time. Charge its structural
	// estimate while processing this pack, rather than summing historical
	// directories that are no longer retained by the query.
	s.work.index = pack.IndexMemoryEstimate(idx)
	s.work.observe()
	if s.work.used() > s.limit {
		return nil, querylimit.Errorf(querylimit.DecodedBytes, "query parsed-index work budget exceeded")
	}
	if idx.Tenant.String() != s.tenant || idx.Start != entry.Start || idx.End != entry.End {
		return nil, errors.New("index disagrees with committed catalog")
	}
	if entry.CheckDescriptor && (int64(len(data)) != ref.IndexBytes || idx.Size != ref.DataBytes || idx.SHA256 != ref.DataSHA256) {
		return nil, errors.New("index disagrees with committed pack descriptor")
	}
	return idx, nil
}

// selectBlocks returns the blocks of matching series that overlap the query
// interval, reserving their work before any payload I/O.
func (s *packSearch) selectBlocks(idx *pack.Index) ([]selectedBlock, error) {
	var blocks []selectedBlock
	for i, series := range idx.Series {
		if !s.matcher.MatchLabels(series.Labels) {
			continue
		}
		for j, b := range series.Blocks {
			if b.MaxTimestamp < s.sq.MinTimestamp || b.MinTimestamp > s.sq.MaxTimestamp {
				continue
			}
			// No gaps are fetched: the sum of original lengths is exactly the
			// combined response size. Reserve all bytes before any payload I/O,
			// including cache hits and physical rows before clipping.
			// Decoding temporarily owns portable timestamp/value arrays as
			// well as the retained VM arrays. Charge both, plus copied native
			// names and per-block/selection/response bookkeeping allowances.
			read, decoded := b.Length, int64(b.Rows)*32
			resultEstimate := int64(len(series.NativeName))*2 + 512
			if read+decoded+resultEstimate > s.limit-s.work.used() || len(s.result)+len(blocks) >= 100000 {
				return nil, querylimit.Errorf(querylimit.DecodedBytes, "query decoded/read budget exceeded")
			}
			s.work.read += read
			s.work.decoded += decoded
			s.work.result += resultEstimate
			s.work.observe()
			blocks = append(blocks, selectedBlock{i, j, b})
		}
	}
	return blocks, nil
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
		group, raw, err := r.readGroup(ctx, key, blocks, first)
		if err != nil {
			return nil, err
		}
		if result, err = loadGroup(ctx, result, idx, group, raw); err != nil {
			return nil, err
		}
		first += len(group)
	}
	return result, nil
}

// readGroup returns the blocks starting at first that share one read, and
// their contiguous bytes: a cached block alone, or the adjacent uncached
// blocks that fit in one combined range.
func (r *Reader) readGroup(ctx context.Context, key string, blocks []selectedBlock, first int) ([]selectedBlock, []byte, error) {
	b := blocks[first].ref
	if raw, hit := r.cacheGet(cacheKey(key, b.Offset, b.Length, b.SHA256)); hit {
		r.CacheHits.Add(1)
		return blocks[first : first+1], raw, nil
	}
	last := first + 1
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
	group := blocks[first:last]
	raw, err := r.readRange(ctx, key, group, b.Offset, length)
	if err != nil {
		return nil, nil, err
	}
	return group, raw, nil
}

// readRange reads the combined range [start,start+length) of group's blocks
// once for all concurrent callers, verifying each block before caching it.
func (r *Reader) readRange(ctx context.Context, key string, group []selectedBlock, start, length int64) ([]byte, error) {
	// Hash the original block checksums as part of the immutable flight
	// identity: equal offsets with a changed directory cannot share data.
	sums := sha256.New()
	for _, selected := range group {
		_, _ = sums.Write([]byte(selected.ref.SHA256))
	}
	flightKey := cacheKey(key, start, length, hex.EncodeToString(sums.Sum(nil)))
	return r.sharedRead(ctx, flightKey, func(readCtx context.Context) ([]byte, error) {
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
		data, err := r.Store.GetRange(readCtx, key, start, length)
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
			offset := ref.Offset - start
			if pack.Sum(data[offset:offset+ref.Length]) != ref.SHA256 {
				return nil, errors.New("immutable object checksum mismatch")
			}
		}
		if err := readCtx.Err(); err != nil {
			return nil, err
		}
		for _, selected := range group {
			ref := selected.ref
			offset := ref.Offset - start
			r.cachePut(cacheKey(key, ref.Offset, ref.Length, ref.SHA256), data[offset:offset+ref.Length])
		}
		return data, nil
	})
}

// loadGroup decodes each block of a group from raw, the group's contiguous
// bytes, and appends the blocks with rows in committed coverage to result.
func loadGroup(ctx context.Context, result []storage.MetricBlock, idx *pack.Index, group []selectedBlock, raw []byte) ([]storage.MetricBlock, error) {
	start := group[0].ref.Offset
	for _, selected := range group {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ref := selected.ref
		offset := ref.Offset - start
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
