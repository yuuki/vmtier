package cold

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

type sqliteFixture struct {
	reader *Reader
	blobs  *objectstore.Local
	db     *catalog.SQLite
}

func sqliteFixtureFor(t *testing.T, age time.Duration, sources ...string) *sqliteFixture {
	t.Helper()
	root := t.TempDir()
	blobDir := filepath.Join(root, "blobs")
	blobs, err := objectstore.NewLocal(blobDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	if len(sources) == 0 {
		sources = []string{"hot"}
	}
	contract := catalog.Contract{CoverageStart: 0, WindowMS: 100, DedupMS: 1, AgeMS: age.Milliseconds(), Tenants: []string{"1:2", "2:1"}}
	fingerprints := map[string]string{}
	for _, source := range sources {
		contract.Sources = append(contract.Sources, catalog.Source{ID: source, Fingerprint: fp})
		fingerprints[source] = fp
	}
	locator := catalog.BlobLocator{Type: "local", Path: blobDir}
	db, err := catalog.CreateSQLite(ctx, filepath.Join(root, "catalog"), "sqlite-reader", contract, locator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := &Reader{Store: blobs, Repository: db, BlobLocator: locator, Tenants: map[string]bool{"1:2": true, "2:1": true},
		Sources: sources, SourceFingerprints: fingerprints, CoverageStart: 0, DedupMS: 1,
		Age: age, Window: 100 * time.Millisecond, MaxQueryBytes: 1 << 20, CacheBytes: 1 << 20}
	return &sqliteFixture{reader: r, blobs: blobs, db: db}
}

func sqlitePackRef(t *testing.T, f *sqliteFixture, tenant pack.Tenant, start, value int64) catalog.PackRef {
	t.Helper()
	e := writePack(t, f.blobs, tenant, start, value)
	data, err := f.blobs.Get(ctx, e.IndexKey)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := pack.ParseIndex(data, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return catalog.PackRef{DataKey: e.DataKey, IndexKey: e.IndexKey, DataSHA256: idx.SHA256,
		IndexSHA256: e.IndexSHA256, DataBytes: idx.Size, IndexBytes: int64(len(data))}
}

func publishSQLite(t *testing.T, f *sqliteFixture, tenant, source string, start int64, refs ...catalog.PackRef) {
	t.Helper()
	s, err := f.db.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, ref := range refs {
		keys = append(keys, ref.DataKey, ref.IndexKey)
	}
	if len(keys) > 0 {
		if err = f.db.RecordPendingObjects(ctx, tenant, keys); err != nil {
			t.Fatal(err)
		}
	}
	_, err = f.db.PublishWindow(ctx, s.Generation, catalog.Publication{Tenant: tenant, SourceID: source, Start: start, End: start + 100, Packs: refs})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteReaderMultiplePacksEmptyWindowsAndTenantIsolation(t *testing.T) {
	f := sqliteFixtureFor(t, 100*365*24*time.Hour)
	first := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 5)
	second := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 7)
	publishSQLite(t, f, "1:2", "hot", 0, first, second)
	publishSQLite(t, f, "1:2", "hot", 100)
	other := sqlitePackRef(t, f, pack.Tenant{AccountID: 2, ProjectID: 1}, 0, 9)
	publishSQLite(t, f, "2:1", "hot", 0, other)
	for i := 0; i < 2; i++ {
		got, err := f.reader.Search(ctx, query())
		if err != nil {
			t.Fatal(err)
		}
		assertValues(t, got, 5, 5, 5, 7, 7, 7)
	}
	if f.blobs.Stats().RangeGets != 2 || f.reader.CacheHits.Load() < 4 {
		t.Fatal("SQLite reader did not reuse immutable pack cache")
	}
	got, err := f.reader.Search(ctx, storage.NewSearchQuery(2, 1, 0, 99, nil, 100))
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, got, 9, 9, 9)
	for _, bounds := range [][2]int64{{100, 199}, {100, 100}, {math.MaxInt64, math.MaxInt64}} {
		got, err := f.reader.Search(ctx, storage.NewSearchQuery(1, 2, bounds[0], bounds[1], nil, 100))
		if err != nil || len(got) != 0 {
			t.Fatalf("empty interval %v: %v %v", bounds, got, err)
		}
	}
	got, err = f.reader.Search(ctx, storage.NewSearchQuery(1, 2, 0, math.MaxInt64, nil, 100))
	if err != nil {
		t.Fatalf("metadata timestamp bound overflowed: %v", err)
	}
	assertValues(t, got, 5, 5, 5, 7, 7, 7)
	if _, err := f.reader.Search(ctx, storage.NewSearchQuery(9, 9, 0, 99, nil, 100)); err == nil {
		t.Fatal("unknown tenant accepted")
	}
}

func TestSQLiteReaderRequiresEverySourceCoverage(t *testing.T) {
	f := sqliteFixtureFor(t, time.Millisecond, "hot", "other")
	for _, source := range []string{"hot", "other"} {
		ref := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 5)
		publishSQLite(t, f, "1:2", source, 0, ref)
	}
	ref := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 100, 8)
	publishSQLite(t, f, "1:2", "hot", 100, ref)
	if _, err := f.reader.Search(ctx, query()); err != nil {
		t.Fatalf("covered old interval failed: %v", err)
	}
	q := storage.NewSearchQuery(1, 2, 100, 199, nil, 100)
	if _, err := f.reader.Search(ctx, q); err == nil {
		t.Fatal("one complete source hid another source's coverage gap")
	}
	if _, err := f.reader.Ready(ctx); err == nil {
		t.Fatal("incomplete current coverage reported ready")
	}
	publishSQLite(t, f, "1:2", "other", 100)
	got, err := f.reader.Search(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, got, 8, 8, 8)
	if _, err := f.reader.Search(ctx, storage.NewSearchQuery(2, 1, 0, 99, nil, 100)); err == nil {
		t.Fatal("an initialized but unpublished stream became an empty success")
	}
}

func TestSQLiteReaderWarmCacheStillRequiresAuthority(t *testing.T) {
	f := sqliteFixtureFor(t, 100*365*24*time.Hour)
	ref := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 5)
	publishSQLite(t, f, "1:2", "hot", 0, ref)
	if _, err := f.reader.Search(ctx, query()); err != nil {
		t.Fatal(err)
	}
	before := f.blobs.Stats()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.reader.Search(ctx, query()); err == nil {
		t.Fatal("warm-cache query ignored unavailable authority")
	}
	if _, err := f.reader.Ready(ctx); err == nil {
		t.Fatal("closed SQLite authority reported ready")
	}
	if f.blobs.Stats() != before {
		t.Fatal("blob I/O happened after authority failure")
	}
}

func TestSQLiteReaderRejectsChangedContractAndLocator(t *testing.T) {
	cases := map[string]func(*Reader){
		"age":    func(r *Reader) { r.Age += time.Millisecond },
		"late":   func(r *Reader) { r.LatenessMS = 1 },
		"window": func(r *Reader) { r.Window *= 2 },
		"source": func(r *Reader) { r.SourceFingerprints["hot"] = strings.Repeat("b", 64) },
		"tenant": func(r *Reader) { r.Tenants["3:4"] = true },
		"blob":   func(r *Reader) { r.BlobLocator.Path += "-different" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := sqliteFixtureFor(t, 100*365*24*time.Hour)
			ref := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 5)
			publishSQLite(t, f, "1:2", "hot", 0, ref)
			if _, err := f.reader.Search(ctx, query()); err != nil {
				t.Fatal(err)
			}
			before := f.blobs.Stats()
			change(f.reader)
			if _, err := f.reader.Search(ctx, query()); err == nil {
				t.Fatal("changed contract/location accepted with warm cache")
			}
			if _, err := f.reader.Ready(ctx); err == nil {
				t.Fatal("changed contract/location reported ready")
			}
			if f.blobs.Stats() != before {
				t.Fatal("blob I/O before validating fixed contract")
			}
		})
	}
}

func TestSQLiteReaderValidatesPackDescriptorsAndBudgets(t *testing.T) {
	for name, alter := range map[string]func(*catalog.PackRef){
		"index size": func(ref *catalog.PackRef) { ref.IndexBytes++ },
		"data size":  func(ref *catalog.PackRef) { ref.DataBytes++ },
		"data sum":   func(ref *catalog.PackRef) { ref.DataSHA256 = strings.Repeat("b", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			f := sqliteFixtureFor(t, 100*365*24*time.Hour)
			ref := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 5)
			alter(&ref)
			publishSQLite(t, f, "1:2", "hot", 0, ref)
			if _, err := f.reader.Search(ctx, query()); err == nil {
				t.Fatal("index/descriptor disagreement became successful query")
			}
			if f.blobs.Stats().RangeGets != 0 {
				t.Fatal("read payload before checking its descriptor")
			}
		})
	}
	f := sqliteFixtureFor(t, 100*365*24*time.Hour)
	ref := sqlitePackRef(t, f, pack.Tenant{AccountID: 1, ProjectID: 2}, 0, 5)
	publishSQLite(t, f, "1:2", "hot", 0, ref)
	before := f.blobs.Stats()
	f.reader.MaxQueryBytes = 1
	if _, err := f.reader.Search(ctx, query()); err == nil {
		t.Fatal("catalog/query byte budget ignored")
	}
	if f.blobs.Stats() != before {
		t.Fatal("read blobs for an over-budget plan")
	}
	f.reader.MaxQueryBytes = 1 << 20
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.reader.Search(canceled, query()); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
}
