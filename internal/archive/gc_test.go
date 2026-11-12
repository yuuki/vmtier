package archive

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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

// A deletion failure keeps the remaining keys claimed for the next pass.
func TestCollectorResumesAfterDeleteFailure(t *testing.T) {
	server, _ := newSQLiteSource(t, "multipack")
	cfg := baseConfig(server.URL)
	cfg.PackLimits.MaxBlocks = 1
	a, catalogPath := newSQLiteArchiver(t, cfg)
	blobDir := filepath.Join(filepath.Dir(catalogPath), "blobs")
	ctx := context.Background()
	// Fail after all six objects were uploaded: the attempt leaves six orphans.
	cfg.Hook = func(stage string) error {
		if stage == "verified" {
			return errors.New("injected failure before publication")
		}
		return nil
	}
	a.Config = cfg
	if _, err := a.RunPass(ctx, testNow); err == nil {
		t.Fatal("failed attempt succeeded")
	}
	if objects := storedObjects(t, blobDir); len(objects) != 6 {
		t.Fatalf("orphans: %v", objects)
	}
	time.Sleep(5 * time.Millisecond)
	failing := &failingDeleter{Deleter: a.Store.(objectstore.Deleter), failAt: 3}
	c := &Collector{Store: failing, Catalog: a.Catalog, MinAge: time.Millisecond, Batch: 4}
	result, err := c.RunPass(ctx)
	if err == nil || result.Claimed != 4 || result.Deleted != 2 {
		t.Fatalf("failed deletion: %+v %v", result, err)
	}
	if counts, err := a.Catalog.UnpublishedObjects(ctx); err != nil || counts != (catalog.UnpublishedObjectCounts{Pending: 2, Deleting: 2}) {
		t.Fatalf("ledger after failed deletion: %+v %v", counts, err)
	}
	result = collectAll(t, a.Store, a.Catalog)
	if result.Claimed != 4 || result.Deleted != 4 {
		t.Fatalf("resumed collection: %+v", result)
	}
	if objects := storedObjects(t, blobDir); len(objects) != 0 {
		t.Fatalf("collection left orphans: %v", objects)
	}
}

type failingDeleter struct {
	objectstore.Deleter
	calls, failAt int
}

func (d *failingDeleter) Delete(ctx context.Context, key string) error {
	d.calls++
	if d.calls == d.failAt {
		return errors.New("injected DELETE failure")
	}
	return d.Deleter.Delete(ctx, key)
}
