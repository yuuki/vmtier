package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/gateway"
)

type testCollector struct {
	calls  atomic.Int64
	result archive.CollectResult
	err    error
}

func (c *testCollector) RunPass(context.Context) (archive.CollectResult, error) {
	c.calls.Add(1)
	return c.result, c.err
}

type leaseLostRunner struct{}

func (leaseLostRunner) RunPass(context.Context, time.Time) (*archive.Result, error) {
	return &archive.Result{}, catalog.ErrLeaseLost
}

func TestCollectingWorkerRunsAtMostOncePerInterval(t *testing.T) {
	ctx := context.Background()
	collector := &testCollector{result: archive.CollectResult{Claimed: 3, Deleted: 2}}
	var stats orphanGCStats
	worker := &collectingWorker{archiver: &testPassRunner{}, collector: collector, interval: time.Hour, stats: &stats}
	result, err := worker.RunPass(ctx, time.Now())
	if err != nil || result.CommittedWindows != 1 {
		t.Fatalf("archive result was not passed through: %+v %v", result, err)
	}
	if _, err := worker.RunPass(ctx, time.Now()); err == nil {
		t.Fatal("archive error was not passed through")
	}
	if collector.calls.Load() != 1 || stats.passes.Load() != 1 || stats.claimed.Load() != 3 || stats.delete.Load() != 2 || stats.lastSuccess.Load() == 0 {
		t.Fatalf("collection did not run once: calls=%d stats=%+v", collector.calls.Load(), &stats)
	}
	failing := &testCollector{err: errors.New("injected")}
	worker = &collectingWorker{archiver: &testPassRunner{}, collector: failing, interval: time.Nanosecond, stats: &stats}
	for i := 0; i < 2; i++ {
		if _, err := worker.RunPass(ctx, time.Now()); i == 0 && err != nil {
			t.Fatal("a collection failure failed the archive pass:", err)
		}
	}
	if failing.calls.Load() != 2 || stats.failures.Load() != 2 {
		t.Fatalf("failed collections: calls=%d failures=%d", failing.calls.Load(), stats.failures.Load())
	}
	// A pass that lost the lease or was canceled does not collect.
	idle := &testCollector{}
	worker = &collectingWorker{archiver: leaseLostRunner{}, collector: idle, interval: time.Nanosecond, stats: &stats}
	if _, err := worker.RunPass(ctx, time.Now()); !errors.Is(err, catalog.ErrLeaseLost) || idle.calls.Load() != 0 {
		t.Fatalf("collected after a lost lease: %v calls=%d", err, idle.calls.Load())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	worker.archiver = &testPassRunner{}
	if _, _ = worker.RunPass(canceled, time.Now()); idle.calls.Load() != 0 {
		t.Fatal("collected after cancellation")
	}
}

// A pass that reports a backlog is followed by another after the next archive
// pass instead of waiting for the interval.
func TestCollectingWorkerDrainsBacklog(t *testing.T) {
	ctx := context.Background()
	collector := &testCollector{result: archive.CollectResult{Deleted: 4096, More: true}}
	var stats orphanGCStats
	worker := &collectingWorker{archiver: &testPassRunner{}, collector: collector, interval: time.Hour, stats: &stats}
	for i := 0; i < 3; i++ {
		_, _ = worker.RunPass(ctx, time.Now())
	}
	if collector.calls.Load() != 3 {
		t.Fatalf("backlog waited for the interval: %d passes", collector.calls.Load())
	}
	collector.result.More = false
	for i := 0; i < 3; i++ {
		_, _ = worker.RunPass(ctx, time.Now())
	}
	if collector.calls.Load() != 4 {
		t.Fatalf("collection without a backlog ignored the interval: %d passes", collector.calls.Load())
	}
}

func TestSQLiteCLIUpgradeAddsObjectLedger(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	root := t.TempDir()
	_, file := sqliteCLIConfig(t, root, true)
	dir := filepath.Join(root, "sqlite")
	if err := run([]string{"catalog-init", "-backend", "sqlite", "-config", file, "-path", dir, "-id", "sqlite-1"}); err != nil {
		t.Fatal(err)
	}
	// Reconstruct the schema 1 shape of an earlier release.
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(dir, catalog.SQLiteFile)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE pending_objects; DROP INDEX windows_start; ALTER TABLE catalog_state DROP COLUMN retired_windows; ALTER TABLE coverage DROP COLUMN start_ms; UPDATE catalog_state SET schema_version=1`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := run([]string{"catalog-inspect", "-path", dir, "-id", "sqlite-1"}); err == nil || !strings.Contains(err.Error(), "catalog-upgrade") {
		t.Fatalf("schema 1 opened without an upgrade: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := run([]string{"catalog-upgrade", "-path", dir, "-id", "sqlite-1"}); err != nil {
			t.Fatalf("upgrade %d: %v", i, err)
		}
	}
	if err := run([]string{"catalog-inspect", "-path", dir, "-id", "sqlite-1"}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"catalog-upgrade", "-path", dir},
		{"catalog-upgrade", "-path", dir, "-id", "other"},
		{"catalog-upgrade", "-backend", "mysql", "-path", dir, "-id", "sqlite-1"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

// The service worker with garbage collection deletes the objects of an attempt
// that never published, exports the outcome as metrics, and leaves objects it
// did not record alone.
func TestServiceWorkerCollectsUnpublishedObjects(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	c, _ := sqliteCLIConfig(t, root, false)
	c.Catalog.Type, c.Catalog.Path = "sqlite", filepath.Join(root, "sqlite")
	// No windows are eligible yet, so the archive passes need no VM process.
	c.CoverageStart = time.Now().Add(time.Hour).Truncate(time.Hour).UnixMilli()
	c.Service.ArchiveTimeout = "10ms"
	c.Service.OrphanGCInterval, c.Service.OrphanGCMinAge = "10ms", "10ms"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	blob, err := c.BlobLocator()
	if err != nil {
		t.Fatal(err)
	}
	db, err := catalog.CreateSQLite(ctx, c.Catalog.Path, c.Catalog.ID, c.ArchiveContract(), blob)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	orphans := []catalog.PendingObject{{Key: "tenants/1:2/attempts/failed/x.pack", Bytes: 5}, {Key: "tenants/1:2/attempts/failed/x.json", Bytes: 3}}
	if err := db.RecordPendingObjects(ctx, "1:2", orphans); err != nil {
		t.Fatal(err)
	}
	store, err := c.OpenBlobs()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(io.Closer).Close()
	for _, key := range []string{"tenants/1:2/other-catalog/keep.pack", orphans[0].Key, orphans[1].Key} {
		if err := store.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	var stats serviceStats
	newWorker, err := archiveWorkers(&c, store, &stats)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := c.RepositoryReader(store, db)
	if err != nil {
		t.Fatal(err)
	}
	scrape := func() string {
		recorder := httptest.NewRecorder()
		serveMux(&c, reader, gateway.New(metricsLimitBackend{}, gateway.Limits{}), store, &stats).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		return recorder.Body.String()
	}
	// The ledger measures the recorded objects before they are collected.
	before := scrape()
	for _, want := range []string{
		"vmtier_catalog_unpublished_objects{state=\"pending\"} 2\n", "vmtier_catalog_unpublished_object_bytes{state=\"pending\"} 8\n",
		"vmtier_catalog_unpublished_objects_unsized{state=\"pending\"} 0\n", "vmtier_catalog_unpublished_object_bytes{state=\"foreign\"} 0\n",
	} {
		if !strings.Contains(before, want) {
			t.Fatalf("metrics lack %q:\n%s", want, before)
		}
	}
	worker := newWorker(db)
	time.Sleep(20 * time.Millisecond)
	if _, err := worker.RunPass(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, orphan := range orphans {
		if _, err := os.Stat(filepath.Join(c.Store.Path, orphan.Key)); !os.IsNotExist(err) {
			t.Fatalf("orphan %s survived: %v", orphan.Key, err)
		}
	}
	if _, err := store.Get(ctx, "tenants/1:2/other-catalog/keep.pack"); err != nil {
		t.Fatalf("unrecorded object deleted: %v", err)
	}
	metrics := scrape()
	for _, want := range []string{
		"vmtier_orphan_gc_enabled 1\n", "vmtier_orphan_gc_passes_total 1\n", "vmtier_orphan_gc_claimed_objects_total 2\n",
		"vmtier_orphan_gc_deleted_objects_total 2\n", "vmtier_orphan_gc_failed_passes_total 0\n",
		"vmtier_object_delete_requests_total 2\n",
		"vmtier_catalog_unpublished_objects{state=\"pending\"} 0\n", "vmtier_catalog_unpublished_objects{state=\"retired\"} 0\n",
		"vmtier_catalog_unpublished_objects{state=\"deleting\"} 0\n", "vmtier_catalog_unpublished_object_bytes{state=\"pending\"} 0\n", "vmtier_cold_retired_windows_total 0\n", "vmtier_cold_retention_seconds 0.000\n", "vmtier_catalog_retired_windows 0\n",
		fmt.Sprintf("vmtier_stream_coverage_start_ms{tenant=\"1:2\",source=\"hot\"} %d\n", c.CoverageStart),
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics lack %q:\n%s", want, metrics)
		}
	}
	// Cold retention retires through the same collector.
	c.ColdRetention, c.Service.OrphanGCMinAge = "720h", "10m"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	newWorker, err = archiveWorkers(&c, store, &stats)
	if err != nil {
		t.Fatal(err)
	}
	retaining := newWorker(db).(*collectingWorker).collector.(*archive.Collector)
	if retaining.Retention != 720*time.Hour || retaining.Retire == nil {
		t.Fatalf("Cold retention not wired: %+v", retaining)
	}
	var exposition strings.Builder
	stats.orphanGC.write(&exposition)
	if !strings.Contains(exposition.String(), "vmtier_cold_retention_seconds 2592000.000\n") {
		t.Fatalf("configured Cold retention not exposed:\n%s", exposition.String())
	}
	c.ColdRetention = ""
	// Disabled collection runs the archiver alone and says so.
	c.Service.OrphanGCInterval, c.Service.OrphanGCMinAge = "", ""
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	var disabled serviceStats
	newWorker, err = archiveWorkers(&c, store, &disabled)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := newWorker(db).(*archive.Archiver); !ok || disabled.orphanGC.enabled.Load() {
		t.Fatal("disabled collection still wraps the archiver")
	}
}
