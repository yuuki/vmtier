package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/pack"
	"github.com/yuuki/vmtier/internal/querylimit"
)

func TestWindowFailureMetricsAreBoundedAndResetOnRestart(t *testing.T) {
	config := &config.Config{Tenants: []string{"1:2"}, Sources: []config.Source{{ID: "hot"}}}
	var m windowFailureMetrics
	m.init(config)
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			m.record("1:2", "hot", errors.New("secret dynamic error text"))
			m.record("1:2", "hot", fmt.Errorf("wrapped: %w", context.DeadlineExceeded))
			m.record("1:2", "hot", context.Canceled)
			m.record("unknown", "hot", errors.New("unknown stream"))
			m.record("1:2", "hot", nil)
		}()
	}
	workers.Wait()
	var output bytes.Buffer
	m.write(&output)
	if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "unknown") || strings.Count(output.String(), "\n") != 3 {
		t.Fatalf("unbounded metric labels: %s", output.String())
	}
	for _, reason := range []string{"error", "timeout", "canceled"} {
		if !strings.Contains(output.String(), `reason="`+reason+`"} 10`) {
			t.Fatalf("missing %s attempts: %s", reason, output.String())
		}
	}
	m.init(config)
	output.Reset()
	m.write(&output)
	if strings.Count(output.String(), "} 0\n") != 3 {
		t.Fatalf("new process counters did not reset: %s", output.String())
	}
}

type metricsLimitBackend struct{}

func (metricsLimitBackend) Search(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
	return nil, querylimit.Errorf(querylimit.ReadBytes, "fixture exceeded the read budget")
}

func TestCapacityMetricsObserveAuthorityAndQueryRefusals(t *testing.T) {
	ctx := context.Background()
	c, _ := sqliteCLIConfig(t, t.TempDir(), false)
	c.Catalog.Type, c.Catalog.Path = "sqlite", filepath.Join(t.TempDir(), "catalog")
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
	store, err := c.OpenBlobs()
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := store.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	reader, err := c.RepositoryReader(store, db)
	if err != nil {
		t.Fatal(err)
	}
	api := gateway.New(metricsLimitBackend{}, gateway.Limits{})
	if _, err := api.InitSearch(context.Background(), nil, &storage.SearchQuery{AccountID: 1, ProjectID: 2, MinTimestamp: 1, MaxTimestamp: 2}); querylimit.Reason(err) != querylimit.ReadBytes {
		t.Fatalf("fixture refusal: %v", err)
	}
	var stats serviceStats
	stats.windowFailures.init(&c)
	stats.windowFailures.record("1:2", "hot", context.DeadlineExceeded)
	mux := serveMux(&c, reader, api, store, &stats)
	scrape := func() string {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != 200 {
			t.Fatalf("scrape status=%d", recorder.Code)
		}
		return recorder.Body.String()
	}
	output := scrape()
	for _, want := range []string{
		"vmtier_catalog_capacity_available 1\n", "vmtier_catalog_file_bytes{file=\"database\"}",
		"vmtier_catalog_committed_object_bytes{kind=\"data\"} 0\n", "vmtier_cache_used_bytes 0\n",
		"vmtier_query_memory_reserved_bytes 0\n", "vmtier_query_limit_refusals_total{reason=\"read_bytes\"} 1\n",
		"vmtier_archive_failed_windows_total{tenant=\"1:2\",source=\"hot\",reason=\"timeout\"} 1\n",
		"vmtier_go_heap_objects_bytes ", "vmtier_go_heap_alloc_bytes_total ",
		"vmtier_archive_verifier_bytes 0\n", "vmtier_archive_pack_bytes 0\n",
		"vmtier_cache_evictions_total 0\n", "vmtier_query_work_peak_bytes 0\n",
		"vmtier_build_info{version=", "vm_version=\"" + pack.VMVersion + "\"} 1\n",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing %q in %s", want, output)
		}
	}
	// A backend without local-file capacity still provides catalog status and
	// query service; it must not emit invented SQLite file or object gauges.
	reader.Repository = struct{ catalog.Repository }{db}
	output = scrape()
	if !strings.Contains(output, "vmtier_catalog_capacity_available 0\n") || strings.Contains(output, "vmtier_catalog_committed_object_bytes") || strings.Contains(output, "vmtier_catalog_file_bytes") || !strings.Contains(output, "vmtier_catalog_generation 0\n") {
		t.Fatalf("unsupported capacity was not separated from catalog status: %s", output)
	}
	reader.Repository = db
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	output = scrape()
	if !strings.Contains(output, "vmtier_catalog_capacity_available 0\n") || strings.Contains(output, "vmtier_catalog_committed_object_bytes") || strings.Contains(output, "vmtier_catalog_file_bytes") {
		t.Fatalf("unavailable catalog published false or stale capacity: %s", output)
	}
}
