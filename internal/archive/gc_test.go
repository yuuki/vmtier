package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
)

// storedObjects lists the object keys under a local blob directory.
func storedObjects(t *testing.T, dir string) []string {
	t.Helper()
	var keys []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		keys = append(keys, filepath.ToSlash(rel))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	return keys
}

// publishedObjects lists every key the catalog references for a tenant.
func publishedObjects(t *testing.T, repo catalog.Repository, cfg Config, tenant string) []string {
	t.Helper()
	plan, err := repo.Select(context.Background(), catalog.Selection{Tenant: tenant, Start: cfg.CoverageStart, End: testNow.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, window := range plan.Windows {
		for _, ref := range window.Packs {
			keys = append(keys, ref.DataKey, ref.IndexKey)
		}
	}
	sort.Strings(keys)
	return keys
}

// collectAll runs the collector after every recorded key is old enough.
func collectAll(t *testing.T, store objectstore.BlobStore, repo catalog.Repository) CollectResult {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	c := &Collector{Store: store.(objectstore.Deleter), Catalog: repo, MinAge: time.Millisecond, Batch: 2}
	result, err := c.RunPass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	counts, err := repo.UnpublishedObjects(context.Background())
	if err != nil || counts != (catalog.UnpublishedObjectCounts{}) {
		t.Fatalf("collection left ledger records: %+v %v", counts, err)
	}
	return result
}

// After any upload fault and a successful retry, collection deletes exactly
// the objects of the failed attempt: the bucket then holds the published
// objects only, and every published window stays readable.
func TestCollectorDeletesOnlyFailedAttemptObjects(t *testing.T) {
	for _, mode := range []string{"error", "torn", "stall", "lost-ack"} {
		for _, failAt := range []int{1, 4, 6} {
			t.Run(fmt.Sprintf("%s/put%d", mode, failAt), func(t *testing.T) {
				server, _ := newSQLiteSource(t, "multipack")
				cfg := baseConfig(server.URL)
				cfg.PackLimits.MaxBlocks = 1
				a, catalogPath := newSQLiteArchiver(t, cfg)
				blobDir := filepath.Join(filepath.Dir(catalogPath), "blobs")
				clean := a.Store
				a.Store = &faultyBlobs{BlobStore: clean, mode: mode, failAt: failAt}
				a.WindowTimeout = 2 * time.Second
				ctx := context.Background()
				if result, err := a.RunPass(ctx, testNow); err == nil || result.CommittedWindows != 0 {
					t.Fatalf("faulted attempt committed: %+v %v", result, err)
				}
				a.Store = clean
				for i := 0; i < 5; i++ {
					if _, err := (&Archiver{Store: clean, Catalog: a.Catalog, Config: cfg}).RunPass(ctx, testNow); err != nil {
						t.Fatal(err)
					}
				}
				published := publishedObjects(t, a.Catalog, cfg, "7:3")
				if len(published) == 0 {
					t.Fatal("nothing was published")
				}
				before := storedObjects(t, blobDir)
				result := collectAll(t, clean, a.Catalog)
				after := storedObjects(t, blobDir)
				if strings.Join(after, "\n") != strings.Join(published, "\n") {
					t.Fatalf("collection left %v, published %v", after, published)
				}
				if result.Deleted < len(before)-len(after) {
					t.Fatalf("result %+v does not account for %d deleted objects", result, len(before)-len(after))
				}
				if rows := requireReadablePublication(t, a, cfg, "7:3"); rows == 0 {
					t.Fatal("published window unreadable after collection")
				}
			})
		}
	}
}

// A claim that lands after an attempt verified its uploads, but before its
// publication, makes that publication fail: the catalog never references a
// claimed object. The next attempt publishes with fresh keys.
func TestCollectorClaimBeforePublicationFailsTheAttempt(t *testing.T) {
	server, _ := newSQLiteSource(t, "multipack")
	cfg := baseConfig(server.URL)
	cfg.PackLimits.MaxBlocks = 1
	a, catalogPath := newSQLiteArchiver(t, cfg)
	blobDir := filepath.Join(filepath.Dir(catalogPath), "blobs")
	ctx := context.Background()
	collector := &Collector{Store: a.Store.(objectstore.Deleter), Catalog: a.Catalog, MinAge: time.Millisecond}
	cfg.Hook = func(stage string) error {
		if stage != "verified" {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
		result, err := collector.RunPass(ctx)
		if err == nil && result.Deleted == 0 {
			err = errors.New("nothing was collected")
		}
		return err
	}
	a.Config = cfg
	result, err := a.RunPass(ctx, testNow)
	if !errors.Is(err, catalog.ErrUnrecordedObject) || result.CommittedWindows != 0 {
		t.Fatalf("publication of claimed objects: %+v %v", result, err)
	}
	if state, err := a.Catalog.Status(ctx); err != nil || state.Generation != 0 {
		t.Fatalf("claimed attempt changed the catalog: %+v %v", state, err)
	}
	if objects := storedObjects(t, blobDir); len(objects) != 0 {
		t.Fatalf("claimed attempt left objects: %v", objects)
	}
	cfg.Hook = nil
	retry := &Archiver{Store: a.Store, Catalog: a.Catalog, Config: cfg}
	if result, err := retry.RunPass(ctx, testNow); err != nil || result.CommittedWindows != 1 {
		t.Fatalf("retry with fresh keys: %+v %v", result, err)
	}
	if rows := requireReadablePublication(t, retry, cfg, "7:3"); rows == 0 {
		t.Fatal("retry published no readable rows")
	}
	if result := collectAll(t, a.Store, a.Catalog); result.Claimed != 0 {
		t.Fatalf("published attempt was claimed: %+v", result)
	}
}

// failedAttempt leaves the six objects of one three-pack attempt as orphans.
func failedAttempt(t *testing.T) (*Archiver, string) {
	t.Helper()
	server, _ := newSQLiteSource(t, "multipack")
	cfg := baseConfig(server.URL)
	cfg.PackLimits.MaxBlocks = 1
	a, catalogPath := newSQLiteArchiver(t, cfg)
	blobDir := filepath.Join(filepath.Dir(catalogPath), "blobs")
	cfg.Hook = func(stage string) error {
		if stage == "verified" {
			return errors.New("injected failure before publication")
		}
		return nil
	}
	a.Config = cfg
	if _, err := a.RunPass(context.Background(), testNow); err == nil {
		t.Fatal("failed attempt succeeded")
	}
	if objects := storedObjects(t, blobDir); len(objects) != 6 {
		t.Fatalf("orphans: %v", objects)
	}
	time.Sleep(5 * time.Millisecond)
	return a, blobDir
}

// A deletion failure neither stops the rest of the batch nor blocks later
// passes: the key is deferred behind the other claimed keys.
func TestCollectorDefersKeysThatFailToDelete(t *testing.T) {
	a, blobDir := failedAttempt(t)
	ctx := context.Background()
	failing := &failingDeleter{Deleter: a.Store.(objectstore.Deleter), fail: map[int]bool{3: true}}
	c := &Collector{Store: failing, Catalog: a.Catalog, MinAge: time.Millisecond, Batch: 4}
	result, err := c.RunPass(ctx)
	if err == nil || result.Deleted != 5 {
		t.Fatalf("failed deletion: %+v %v", result, err)
	}
	if counts, err := a.Catalog.UnpublishedObjects(ctx); err != nil || counts != (catalog.UnpublishedObjectCounts{Deleting: 1}) {
		t.Fatalf("ledger after failed deletion: %+v %v", counts, err)
	}
	result = collectAll(t, a.Store, a.Catalog)
	if result.Deleted != 1 {
		t.Fatalf("resumed collection: %+v", result)
	}
	if objects := storedObjects(t, blobDir); len(objects) != 0 {
		t.Fatalf("collection left orphans: %v", objects)
	}
}

// A key that can never be deleted does not starve the others: deferred keys
// go behind the rest, and a batch where every deletion fails ends the pass.
func TestCollectorIsNotWedgedByAnUndeletableKey(t *testing.T) {
	a, blobDir := failedAttempt(t)
	ctx := context.Background()
	stuck := &stuckDeleter{Deleter: a.Store.(objectstore.Deleter)}
	c := &Collector{Store: stuck, Catalog: a.Catalog, MinAge: time.Millisecond, Batch: 1, MaxBatches: 1}
	// Each pass claims one key; the first key never deletes.
	result, err := c.RunPass(ctx)
	if err == nil || result.Deleted != 0 || stuck.key == "" {
		t.Fatalf("first pass: %+v %v", result, err)
	}
	for i := 0; i < 10; i++ {
		if _, err := c.RunPass(ctx); err != nil && !strings.Contains(err.Error(), stuck.key) {
			t.Fatal(err)
		}
	}
	objects := storedObjects(t, blobDir)
	if len(objects) != 1 || objects[0] != stuck.key {
		t.Fatalf("collection wedged behind %s: %v", stuck.key, objects)
	}
	if counts, err := a.Catalog.UnpublishedObjects(ctx); err != nil || counts != (catalog.UnpublishedObjectCounts{Deleting: 1}) {
		t.Fatalf("ledger: %+v %v", counts, err)
	}
}

// A pass that stops at its bounds with keys left reports More.
func TestCollectorReportsBacklog(t *testing.T) {
	a, _ := failedAttempt(t)
	c := &Collector{Store: a.Store.(objectstore.Deleter), Catalog: a.Catalog, MinAge: time.Millisecond, Batch: 2, MaxBatches: 2}
	result, err := c.RunPass(context.Background())
	if err != nil || result.Deleted != 4 || !result.More {
		t.Fatalf("bounded pass: %+v %v", result, err)
	}
	result, err = c.RunPass(context.Background())
	if err != nil || result.Deleted != 2 || result.More {
		t.Fatalf("final pass: %+v %v", result, err)
	}
}

type failingDeleter struct {
	objectstore.Deleter
	calls int
	fail  map[int]bool
}

func (d *failingDeleter) Delete(ctx context.Context, key string) error {
	d.calls++
	if d.fail[d.calls] {
		return errors.New("injected DELETE failure")
	}
	return d.Deleter.Delete(ctx, key)
}

// stuckDeleter never deletes the first key it is asked to delete.
type stuckDeleter struct {
	objectstore.Deleter
	key string
}

func (d *stuckDeleter) Delete(ctx context.Context, key string) error {
	if d.key == "" {
		d.key = key
	}
	if key == d.key {
		return errors.New("AccessDenied")
	}
	return d.Deleter.Delete(ctx, key)
}

// retentionEnding is the retention whose boundary, by the catalog's clock, is
// end, with half a window of margin either way.
func retentionEnding(end int64, window time.Duration) time.Duration {
	w := window.Milliseconds()
	return time.Duration(time.Now().UnixMilli()-end-w-w/2) * time.Millisecond
}

// Cold retention retires the windows below the boundary from the catalog and
// garbage collection then deletes their objects; later windows stay readable.
func TestCollectorRetiresWindowsBelowColdRetention(t *testing.T) {
	server, _ := newSQLiteSource(t, "multipack")
	cfg := baseConfig(server.URL)
	cfg.PackLimits.MaxBlocks = 1
	a, catalogPath := newSQLiteArchiver(t, cfg)
	blobDir := filepath.Join(filepath.Dir(catalogPath), "blobs")
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if result, err := a.RunPass(ctx, testNow); err != nil || result.CommittedWindows != 1 {
			t.Fatalf("pass %d: %+v %v", i, result, err)
		}
	}
	all := publishedObjects(t, a.Catalog, cfg, "7:3")
	first := cfg.CoverageStart + cfg.Window.Milliseconds()
	collector := &Collector{Store: a.Store.(objectstore.Deleter), Catalog: a.Catalog, MinAge: time.Hour,
		Retention: retentionEnding(first, cfg.Window), Retire: a.Catalog}
	result, err := collector.RunPass(ctx)
	if err != nil || result.Retired != 1 {
		t.Fatalf("retirement: %+v %v", result, err)
	}
	state, err := a.Catalog.Status(ctx)
	if err != nil || state.Coverage[0].Start != cfg.CoverageStart+cfg.Window.Milliseconds() || state.RetiredWindows != 1 {
		t.Fatalf("status after retirement: %+v %v", state, err)
	}
	kept := publishedObjects(t, a.Catalog, cfg, "7:3")
	if len(kept) == 0 || len(kept) >= len(all) {
		t.Fatalf("retirement kept %d of %d objects", len(kept), len(all))
	}
	// The retired objects survive until they are old enough to be claimed,
	// whatever minimum age the collector asks for.
	collector.MinAge = time.Millisecond
	if result, err := collector.RunPass(ctx); err != nil || result.Deleted != 0 {
		t.Fatalf("retired objects deleted early: %+v %v", result, err)
	}
	if objects := storedObjects(t, blobDir); len(objects) != len(all) {
		t.Fatalf("retired objects deleted before the minimum age: %v", objects)
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(catalogPath, catalog.SQLiteFile)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE pending_objects SET created_ms=created_ms-?`, catalog.MinRetiredObjectAge.Milliseconds()); err != nil {
		t.Fatal(err)
	}
	db.Close()
	result, err = collector.RunPass(ctx)
	if err != nil || result.Retired != 0 || result.Deleted != len(all)-len(kept) {
		t.Fatalf("collection of retired objects: %+v %v", result, err)
	}
	if objects := storedObjects(t, blobDir); strings.Join(objects, "\n") != strings.Join(kept, "\n") {
		t.Fatalf("bucket holds %v, catalog references %v", objects, kept)
	}
	plan, err := a.Catalog.Select(ctx, catalog.Selection{Tenant: "7:3", Start: cfg.CoverageStart, End: testNow.UnixMilli()})
	if err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != state.Coverage[0].Start {
		t.Fatalf("plan after retirement: %+v %v", plan, err)
	}
	// Archiving continues at the coverage end.
	if result, err := a.RunPass(ctx, testNow.Add(time.Hour)); err != nil || result.CommittedWindows != 1 {
		t.Fatalf("archive after retirement: %+v %v", result, err)
	}
	// Nothing is retired while the boundary has not passed the origin.
	early := *collector
	early.Retention = retentionEnding(cfg.CoverageStart, cfg.Window)
	if result, err := early.RunPass(ctx); err != nil || result.Retired != 0 {
		t.Fatalf("retired before the boundary passed the origin: %+v %v", result, err)
	}
}

// After streams are added, an archiver still configured without them keeps
// archiving the streams it knows, and one configured with them archives the
// new streams from their origin, the archive-age boundary when they were added.
func TestArchiverWithAddedStreams(t *testing.T) {
	server, source := newSQLiteSource(t, "")
	now := time.Now()
	cfg := baseConfig(server.URL)
	w := cfg.Window.Milliseconds()
	cfg.CoverageStart = (now.Add(-cfg.Age).UnixMilli()/w - 2) * w
	cfg.StartMS, cfg.EndMS = cfg.CoverageStart, cfg.CoverageStart+w
	a, _ := newSQLiteArchiver(t, cfg)
	ctx := context.Background()
	grown := cfg
	grown.Tenants = []string{"7:3", "8:1"}
	added, err := a.Catalog.(catalog.StreamAdder).AddStreams(ctx, archiveContract(grown))
	if err != nil || len(added) != 1 || added[0].Tenant != "8:1" {
		t.Fatalf("add streams: %+v %v", added, err)
	}
	origin := added[0].Start
	if origin != (now.Add(-cfg.Age).UnixMilli()/w)*w {
		t.Fatalf("origin %d is not the archive-age boundary", origin)
	}
	if result, err := a.RunPass(ctx, now); err != nil || result.CommittedWindows != 1 || source.exportCount("8:1") != 0 {
		t.Fatalf("earlier configuration: %+v %v exports=%d", result, err, source.exportCount("8:1"))
	}
	// One window later, the new stream has one window to archive.
	later := now.Add(cfg.Window)
	current := &Archiver{Store: a.Store, Catalog: a.Catalog, Config: grown}
	for i := 0; i < 4; i++ {
		if _, err := current.RunPass(ctx, later); err != nil {
			t.Fatal(err)
		}
	}
	state, err := a.Catalog.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	end := (later.Add(-cfg.Age).UnixMilli() / w) * w
	for _, stream := range state.Coverage {
		if stream.End != end {
			t.Fatalf("stream did not catch up: %+v", stream)
		}
		if stream.Tenant == "8:1" && stream.Start != origin {
			t.Fatalf("added stream start: %+v", stream)
		}
	}
	if source.exportCount("8:1") != 1 {
		t.Fatalf("added stream exported %d windows, want 1", source.exportCount("8:1"))
	}
	// A configuration that is not covered by the catalog is still refused.
	other := cfg
	other.Tenants = []string{"9:9"}
	if _, err := (&Archiver{Store: a.Store, Catalog: a.Catalog, Config: other}).RunPass(ctx, testNow); err == nil {
		t.Fatal("uncovered configuration archived")
	}
}
