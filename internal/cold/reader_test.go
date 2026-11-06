package cold

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

var ctx = context.Background()

const fp = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func writePack(t *testing.T, s objectstore.BlobStore, tenant pack.Tenant, start, value int64) catalog.Entry {
	t.Helper()
	name := pack.MetricName(tenant, map[string]string{"__name__": "cold", "job": "test"}).MarshalNoAccountIDProjectID(nil)
	var b storage.Block
	b.Init(&storage.TSID{}, []int64{start + 10, start + 20, start + 90}, []int64{value, value, value}, 0, 64)
	payload := b.MarshalPortable(nil)
	native := encoding.MarshalInt64(nil, start)
	native = encoding.MarshalInt64(native, start+99)
	native = encoding.MarshalUint32(native, uint32(len(name)))
	native = append(native, name...)
	native = encoding.MarshalUint32(native, uint32(len(payload)))
	native = append(native, payload...)
	idx, data, err := pack.Build(bytes.NewReader(native), tenant, start, start+100, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := pack.MarshalIndex(idx, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "tenants/" + tenant.String() + "/attempts/" + hex.EncodeToString(nonce[:]) + "/"
	e := catalog.Entry{Tenant: tenant.String(), Start: start, End: start + 100, SourceID: "hot", SourceFingerprint: fp, IndexKey: prefix + "index.json", DataKey: prefix + "data.pack", IndexSHA256: pack.Sum(index)}
	if err := s.Put(ctx, e.DataKey, data); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, e.IndexKey, index); err != nil {
		t.Fatal(err)
	}
	return e
}

type testFixture struct {
	reader    *Reader
	blobs     *objectstore.Local
	authority *catalog.Local
	blobDir   string
	entries   []catalog.Entry
}

func newAuthority(t *testing.T) *catalog.Local {
	t.Helper()
	s, err := catalog.NewLocal(t.TempDir(), "cold-reader-test", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func fixture(t *testing.T, tenants ...pack.Tenant) *testFixture {
	t.Helper()
	return fixtureWithAge(t, 100*365*24*time.Hour, tenants...)
}
func fixtureWithAge(t *testing.T, age time.Duration, tenants ...pack.Tenant) *testFixture {
	t.Helper()
	dir := t.TempDir()
	blobs, err := objectstore.NewLocal(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	authority := newAuthority(t)
	if len(tenants) == 0 {
		tenants = []pack.Tenant{{AccountID: 1, ProjectID: 2}}
	}
	m, generation, err := catalog.Load(ctx, authority)
	if err != nil {
		t.Fatal(err)
	}
	m.Contract = catalog.Contract{CoverageStart: 0, WindowMS: 100, DedupMS: 1, AgeMS: age.Milliseconds(), Sources: []catalog.Source{{ID: "hot", Fingerprint: fp}}}
	configured := map[string]bool{}
	for i, tenant := range tenants {
		m.Contract.Tenants = append(m.Contract.Tenants, tenant.String())
		configured[tenant.String()] = true
		m.Entries = append(m.Entries, writePack(t, blobs, tenant, 0, 5+int64(i)*4))
	}
	if _, err := catalog.Commit(ctx, authority, m, generation); err != nil {
		t.Fatal(err)
	}
	r := &Reader{Store: blobs, CatalogStore: authority, Tenants: configured, Sources: []string{"hot"}, SourceFingerprints: map[string]string{"hot": fp}, CoverageStart: 0, DedupMS: 1, Age: age, Window: 100 * time.Millisecond, MaxQueryBytes: 1 << 20, CacheBytes: 1 << 20}
	return &testFixture{r, blobs, authority, dir, m.Entries}
}
func query() *storage.SearchQuery { return storage.NewSearchQuery(1, 2, 0, 99, nil, 100) }
func values(t *testing.T, blocks []storage.MetricBlock) []float64 {
	t.Helper()
	var out []float64
	for i := range blocks {
		if err := blocks[i].Block.UnmarshalData(); err != nil {
			t.Fatal(err)
		}
		_, v := blocks[i].Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: 0, MaxTimestamp: 1000000})
		out = append(out, v...)
	}
	return out
}
func assertValues(t *testing.T, got []storage.MetricBlock, want ...float64) {
	t.Helper()
	vs := values(t, got)
	if len(vs) != len(want) {
		t.Fatalf("values=%v, want %v", vs, want)
	}
	for i := range want {
		if vs[i] != want[i] {
			t.Fatalf("values=%v, want %v", vs, want)
		}
	}
}

func TestReaderRangeCacheAndTenantIsolation(t *testing.T) {
	f := fixture(t, pack.Tenant{AccountID: 1, ProjectID: 2}, pack.Tenant{AccountID: 2, ProjectID: 1})
	r := f.reader
	for i := 0; i < 2; i++ {
		blocks, err := r.Search(ctx, query())
		if err != nil {
			t.Fatal(err)
		}
		assertValues(t, blocks, 5, 5, 5)
	}
	if f.blobs.Stats().RangeGets != 1 || r.CacheHits.Load() < 2 {
		t.Fatalf("range cache unused: %+v", f.blobs.Stats())
	}
	blocks, err := r.Search(ctx, storage.NewSearchQuery(2, 1, 0, 99, nil, 100))
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, blocks, 9, 9, 9)
	if _, err := r.Search(ctx, storage.NewSearchQuery(9, 9, 0, 99, nil, 100)); err == nil {
		t.Fatal("unknown tenant accepted")
	}
}
func TestReaderFiltersBeforeRangeGet(t *testing.T) {
	f := fixture(t)
	r := f.reader
	q := query()
	q.TagFilterss = [][]storage.TagFilter{{{Key: []byte("job"), Value: []byte("other")}}}
	blocks, err := r.Search(ctx, q)
	if err != nil || len(blocks) != 0 {
		t.Fatalf("mismatch search: %v %v", blocks, err)
	}
	if f.blobs.Stats().RangeGets != 0 {
		t.Fatal("unmatched payload downloaded")
	}
	q = query()
	q.MinTimestamp = 101
	q.MaxTimestamp = 199
	blocks, err = r.Search(ctx, q)
	if err != nil || len(blocks) != 0 {
		t.Fatalf("time search: %v %v", blocks, err)
	}
	if f.blobs.Stats().RangeGets != 0 {
		t.Fatal("out-of-range payload downloaded")
	}
}
func TestReaderCorruptionAndAbsentObjectsFailClosed(t *testing.T) {
	for _, target := range []string{"data", "index"} {
		t.Run(target, func(t *testing.T) {
			f := fixture(t)
			r := f.reader
			r.CacheBytes = 0
			e := f.entries[0]
			key := e.DataKey
			if target == "index" {
				key = e.IndexKey
			}
			if err := os.WriteFile(filepath.Join(f.blobDir, key), []byte("corrupt"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Search(ctx, query()); err == nil {
				t.Fatal("corrupt object became success")
			}
			if err := os.Remove(filepath.Join(f.blobDir, key)); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Search(ctx, query()); err == nil {
				t.Fatal("absent object became success")
			}
		})
	}
}
func TestReaderBudgetCancellationAndCoverage(t *testing.T) {
	f := fixture(t)
	r := f.reader
	r.MaxQueryBytes = 1
	if _, err := r.Search(ctx, query()); err == nil {
		t.Fatal("byte budget not enforced")
	}
	if f.blobs.Stats().RangeGets != 0 {
		t.Fatal("payload read after index exceeded budget")
	}
	r.MaxQueryBytes = 1 << 20
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.Search(c, query()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	r = fixtureWithAge(t, time.Millisecond).reader
	q := query()
	q.MinTimestamp = 100
	q.MaxTimestamp = 200
	if _, err := r.Search(ctx, q); err == nil {
		t.Fatal("unarchived old coverage became empty success")
	}
	if _, err := r.Ready(ctx); err == nil {
		t.Fatal("incomplete coverage reported ready")
	}
	r.Window = 0
	if _, err := r.Search(ctx, query()); err == nil {
		t.Fatal("zero window accepted")
	}
}
func TestReaderRejectsEveryContractMismatch(t *testing.T) {
	changes := map[string]func(*Reader){
		"source fingerprint": func(r *Reader) { r.SourceFingerprints["hot"] = strings.Repeat("b", 64) },
		"source set":         func(r *Reader) { r.Sources = []string{"other"}; r.SourceFingerprints = map[string]string{"other": fp} },
		"tenant set":         func(r *Reader) { r.Tenants["7:8"] = true },
		"start":              func(r *Reader) { r.CoverageStart = 100 },
		"window":             func(r *Reader) { r.Window = 200 * time.Millisecond },
		"dedup":              func(r *Reader) { r.DedupMS = 2 },
		"archive age":        func(r *Reader) { r.Age += time.Hour },
		"lateness":           func(r *Reader) { r.LatenessMS += 1000 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := fixture(t)
			if _, err := f.reader.Search(ctx, query()); err != nil {
				t.Fatal(err)
			}
			change(f.reader)
			if _, err := f.reader.Search(ctx, query()); err == nil {
				t.Fatal("immutable contract mismatch accepted")
			}
			if _, err := f.reader.Ready(ctx); err == nil {
				t.Fatal("mismatched reader ready")
			}
		})
	}
}
func TestReaderIndexTenantMismatch(t *testing.T) {
	f := fixture(t)
	e := f.entries[0]
	data, err := f.blobs.Get(ctx, e.IndexKey)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := pack.ParseIndex(data, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	idx.Tenant = pack.Tenant{AccountID: 8, ProjectID: 9}
	bad, err := pack.MarshalIndex(idx, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e.IndexKey = "tenants/1:2/different-tenant-index.json"
	e.IndexSHA256 = pack.Sum(bad)
	if err := f.blobs.Put(ctx, e.IndexKey, bad); err != nil {
		t.Fatal(err)
	}
	authority := newAuthority(t)
	m, generation, err := catalog.Load(ctx, authority)
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := catalog.Load(ctx, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	m.Contract = old.Contract
	m.Entries = []catalog.Entry{e}
	if _, err := catalog.Commit(ctx, authority, m, generation); err != nil {
		t.Fatal(err)
	}
	f.reader.CatalogStore = authority
	if _, err := f.reader.Search(ctx, query()); err == nil {
		t.Fatal("index tenant mismatch accepted")
	}
}

type switchingBlobs struct {
	objectstore.BlobStore
	once          sync.Once
	key           string
	switchCatalog func()
}

func (s *switchingBlobs) Get(c context.Context, k string) ([]byte, error) {
	if k == s.key {
		s.once.Do(s.switchCatalog)
	}
	return s.BlobStore.Get(c, k)
}
func TestGenerationSnapshotAndCacheIdentity(t *testing.T) {
	f := fixture(t)
	r := f.reader
	old := f.entries[0]
	next := writePack(t, f.blobs, pack.Tenant{AccountID: 1, ProjectID: 2}, 100, 9)
	wrapped := &switchingBlobs{BlobStore: f.blobs, key: old.IndexKey}
	wrapped.switchCatalog = func() {
		m, generation, err := catalog.Load(ctx, f.authority)
		if err != nil {
			t.Fatal(err)
		}
		m.Entries = append(m.Entries, next)
		if _, err := catalog.Commit(ctx, f.authority, m, generation); err != nil {
			t.Fatal(err)
		}
	}
	r.Store = wrapped
	q := query()
	q.MaxTimestamp = 199
	first, err := r.Search(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, first, 5, 5, 5)
	second, err := r.Search(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, second, 5, 5, 5, 9, 9, 9)
	if f.blobs.Stats().RangeGets != 2 {
		t.Fatalf("new generation did not load new immutable payload: %+v", f.blobs.Stats())
	}
}
func TestLosingWriterUploadsCannotAlterPublishedResults(t *testing.T) {
	f := fixture(t)
	winner, generation, err := catalog.Load(ctx, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	loser, loserGeneration, err := catalog.Load(ctx, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	winnerEntry := writePack(t, f.blobs, pack.Tenant{AccountID: 1, ProjectID: 2}, 100, 9)
	loserEntry := writePack(t, f.blobs, pack.Tenant{AccountID: 1, ProjectID: 2}, 100, 99)
	if winnerEntry.DataKey == loserEntry.DataKey || winnerEntry.IndexKey == loserEntry.IndexKey {
		t.Fatal("writers reused nonce keys")
	}
	winner.Entries = append(winner.Entries, winnerEntry)
	loser.Entries = append(loser.Entries, loserEntry)
	if _, err := catalog.Commit(ctx, f.authority, winner, generation); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Commit(ctx, f.authority, loser, loserGeneration); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("losing writer did not conflict: %v", err)
	}
	// Uploaded loser blobs remain as orphans; the reader consults only authority.
	q := query()
	q.MinTimestamp = 100
	q.MaxTimestamp = 199
	blocks, err := f.reader.Search(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, blocks, 9, 9, 9)
	current, _, err := catalog.Load(ctx, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Entries) != 2 || current.Entries[1].DataKey != winnerEntry.DataKey {
		t.Fatal("loser changed publication")
	}
}

type outageAuthority struct {
	catalog.Store
	fail  atomic.Bool
	reads atomic.Uint64
}

func (s *outageAuthority) Read(c context.Context) (*catalog.Manifest, error) {
	s.reads.Add(1)
	if s.fail.Load() {
		return nil, errors.New("injected catalog authority outage")
	}
	return s.Store.Read(c)
}
func TestCatalogOutageFailsClosedEvenWithWarmBlobCache(t *testing.T) {
	f := fixture(t)
	r := f.reader
	authority := &outageAuthority{Store: f.authority}
	r.CatalogStore = authority
	for i := 0; i < 2; i++ {
		blocks, err := r.Search(ctx, query())
		if err != nil {
			t.Fatal(err)
		}
		assertValues(t, blocks, 5, 5, 5)
	}
	before := f.blobs.Stats()
	hits := r.CacheHits.Load()
	if hits == 0 {
		t.Fatal("cache not warmed")
	}
	// An S3-hosted snapshot is deliberately present and valid. It is not authority.
	m, _, err := catalog.Load(ctx, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.blobs.Put(ctx, "catalog.json", snapshot); err != nil {
		t.Fatal(err)
	}
	authority.fail.Store(true)
	if _, err := r.Search(ctx, query()); err == nil {
		t.Fatal("warm cache masked catalog outage")
	}
	if _, err := r.Ready(ctx); err == nil {
		t.Fatal("authority outage reported ready")
	}
	after := f.blobs.Stats()
	if after.Gets != before.Gets || after.RangeGets != before.RangeGets || r.CacheHits.Load() != hits {
		t.Fatal("reader used blob fallback after authority outage")
	}
	if authority.reads.Load() != 4 {
		t.Fatalf("each query/readiness must consult authority: %d", authority.reads.Load())
	}
}
func TestReadinessNeverFetchesSnapshotFromBlobStore(t *testing.T) {
	f := fixture(t)
	before := f.blobs.Stats()
	if err := f.blobs.Put(ctx, "catalog.json", []byte("not a catalog")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reader.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	after := f.blobs.Stats()
	if after.Gets != before.Gets || after.RangeGets != before.RangeGets {
		t.Fatal("readiness consulted blob storage")
	}
}
func TestUnboundGenerationIsNotReady(t *testing.T) {
	f := fixture(t)
	f.reader.CatalogStore = newAuthority(t)
	if _, err := f.reader.Ready(ctx); err == nil {
		t.Fatal("generation zero reported ready")
	}
	if _, err := f.reader.Search(ctx, query()); err == nil {
		t.Fatal("unbound catalog became empty success")
	}
}
func TestEmptyCommittedCoverage(t *testing.T) {
	f := fixture(t)
	e := f.entries[0]
	idx := pack.Index{Version: pack.FormatVersion, VMVersion: pack.VMVersion, VMRevision: pack.VMRevision, Tenant: pack.Tenant{AccountID: 1, ProjectID: 2}, Start: 0, End: 100, SHA256: pack.Sum(nil), Series: []pack.Series{}}
	data, err := pack.MarshalIndex(&idx, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e.IndexKey = "tenants/1:2/empty.json"
	e.IndexSHA256 = pack.Sum(data)
	if err := f.blobs.Put(ctx, e.IndexKey, data); err != nil {
		t.Fatal(err)
	}
	authority := newAuthority(t)
	m, generation, err := catalog.Load(ctx, authority)
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := catalog.Load(ctx, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	m.Contract = old.Contract
	m.Entries = []catalog.Entry{e}
	if _, err := catalog.Commit(ctx, authority, m, generation); err != nil {
		t.Fatal(err)
	}
	f.reader.CatalogStore = authority
	if blocks, err := f.reader.Search(ctx, query()); err != nil || len(blocks) != 0 {
		t.Fatalf("empty committed window: %v %v", blocks, err)
	}
	if _, err := f.reader.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentSearchCacheIsImmutable(t *testing.T) {
	f := fixture(t)
	r := f.reader
	var wg sync.WaitGroup
	for n := 0; n < 12; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				blocks, err := r.Search(ctx, query())
				if err != nil {
					t.Errorf("concurrent search: %v", err)
					return
				}
				if len(blocks) != 1 {
					t.Errorf("wrong block count %d", len(blocks))
					return
				}
				blocks[0].MetricName[0] ^= 1
				blocks[0].Block.Reset()
			}
		}()
	}
	wg.Wait()
	blocks, err := r.Search(ctx, query())
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, blocks, 5, 5, 5)
}
