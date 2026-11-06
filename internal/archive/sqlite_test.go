package archive

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
)

type sqliteSource struct {
	mode       string
	failTenant string
	down       atomic.Bool
	mu         sync.Mutex
	exports    map[string]int
}

func newSQLiteSource(t *testing.T, mode string) (*httptest.Server, *sqliteSource) {
	t.Helper()
	f := &sqliteSource{mode: mode, failTenant: "7:3", exports: map[string]int{}}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 7 || parts[1] != "select" {
			t.Errorf("unexpected source path %q", r.URL.Path)
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		tenant := parts[2]
		q := r.URL.Query()
		if q.Get("deny_partial_response") != "1" {
			t.Error("partial response guard missing")
		}
		native := strings.HasSuffix(r.URL.Path, "/export/native")
		if native {
			f.mu.Lock()
			f.exports[tenant]++
			f.mu.Unlock()
		}
		if tenant == f.failTenant && f.down.Load() {
			if f.mode == "stall" {
				<-r.Context().Done()
				return
			}
			http.Error(w, "source temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		var start, end int64
		if native {
			w.Header().Set("Content-Type", "VictoriaMetrics/native")
			var err error
			start, err = rawTimestamp([]byte(q.Get("start")))
			if err != nil {
				t.Error(err)
				return
			}
			end, err = rawTimestamp([]byte(q.Get("end")))
			if err != nil {
				t.Error(err)
				return
			}
			end++
			body := nativeFixture(start, end, f.mode != "empty")
			if f.mode == "record-boundary" {
				body = body[:16]
			}
			w.Write(body)
			return
		}
		var err error
		end, err = rawTimestamp([]byte(q.Get("time")))
		if err != nil {
			t.Error(err)
			return
		}
		end++
		start = end - time.Hour.Milliseconds()
		if q.Get("nocache") != "1" {
			t.Error("oracle cache guard missing")
		}
		metric := map[string]string{"__name__": "x", "job": "test"}
		if strings.HasPrefix(q.Get("query"), "{") {
			result := []any{map[string]any{"metric": metric, "values": []any{
				[]any{float64(start) / 1000, "1"}, []any{float64(start+10) / 1000, "3"}, []any{float64(end-1) / 1000, "4"},
			}}}
			if f.mode == "empty" {
				result = nil
			}
			out := map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}}
			if f.mode == "partial" {
				out["isPartial"] = true
			}
			json.NewEncoder(w).Encode(out)
			return
		}
		value := "3"
		if strings.HasPrefix(q.Get("query"), "stale_") {
			value = "0"
		}
		result := []any{map[string]any{"metric": metric, "value": []any{float64(end-1) / 1000, value}}}
		if f.mode == "empty" {
			result = nil
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	}))
	t.Cleanup(s.Close)
	return s, f
}

func (f *sqliteSource) exportCount(tenant string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exports[tenant]
}

func newSQLiteArchiver(t *testing.T, cfg Config) (*SQLiteArchiver, string) {
	t.Helper()
	root := t.TempDir()
	blobPath, catalogPath := filepath.Join(root, "blobs"), filepath.Join(root, "catalog")
	blobs, err := objectstore.NewLocal(blobPath, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blobs.Close() })
	db, err := catalog.CreateSQLite(context.Background(), catalogPath, "archive-test", archiveContract(cfg), catalog.BlobLocator{Type: "local", Path: blobPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &SQLiteArchiver{Store: blobs, Catalog: db, Config: cfg, RetryInterval: 30 * time.Second}, catalogPath
}

func TestSQLiteArchiverResumesOneWindowPerStream(t *testing.T) {
	server, source := newSQLiteSource(t, "")
	cfg := baseConfig(server.URL)
	a, path := newSQLiteArchiver(t, cfg)
	ctx := context.Background()
	for pass := 1; pass <= 2; pass++ {
		result, err := a.RunPass(ctx, testNow)
		if err != nil || result.CommittedWindows != 1 || result.Samples != 3 || result.Bytes == 0 {
			t.Fatalf("pass %d: %+v %v", pass, result, err)
		}
	}
	result, err := a.RunPass(ctx, testNow)
	if err != nil || result.CommittedWindows != 0 || source.exportCount("7:3") != 2 {
		t.Fatalf("completed archive re-exported data: %+v %v", result, err)
	}
	end := testNow.Add(-cfg.Age).UnixMilli()
	plan, err := a.Catalog.Select(ctx, catalog.Selection{Tenant: "7:3", Start: cfg.CoverageStart, End: end})
	if err != nil || plan.CoverageEnd != end || len(plan.Windows) != 2 {
		t.Fatalf("unexpected coverage: %+v %v", plan, err)
	}
	for _, window := range plan.Windows {
		if len(window.Packs) != 1 {
			t.Fatal("nonempty stream did not publish one pack")
		}
	}
	if err := a.Catalog.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := catalog.OpenSQLite(ctx, path, "archive-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restarted := &SQLiteArchiver{Store: a.Store, Catalog: db, Config: cfg}
	result, err = restarted.RunPass(ctx, testNow)
	if err != nil || result.CommittedWindows != 0 || source.exportCount("7:3") != 2 {
		t.Fatalf("restart ignored persisted coverage: %+v %v", result, err)
	}
	result, err = restarted.RunPass(ctx, testNow.Add(time.Hour))
	if err != nil || result.CommittedWindows != 1 || source.exportCount("7:3") != 3 {
		t.Fatalf("new eligible window was not selected: %+v %v", result, err)
	}
}

func TestSQLiteArchiverFailureDoesNotBlockOtherTenants(t *testing.T) {
	server, source := newSQLiteSource(t, "")
	source.down.Store(true)
	cfg := baseConfig(server.URL)
	cfg.Tenants = []string{"7:3", "8:1"}
	a, _ := newSQLiteArchiver(t, cfg)
	ctx := context.Background()
	result, err := a.RunPass(ctx, testNow)
	if err == nil || result.CommittedWindows != 1 {
		t.Fatalf("healthy tenant did not progress: %+v %v", result, err)
	}
	result, err = a.RunPass(ctx, testNow)
	if err != nil || result.CommittedWindows != 1 || source.exportCount("7:3") != 1 || source.exportCount("8:1") != 2 {
		t.Fatalf("retry cooldown failed: %+v %v", result, err)
	}
	source.down.Store(false)
	for pass := 0; pass < 2; pass++ {
		result, err = a.RunPass(ctx, testNow.Add(time.Minute))
		if err != nil || result.CommittedWindows != 1 {
			t.Fatalf("failed tenant did not catch up: %+v %v", result, err)
		}
	}
	status, err := a.Catalog.Status(ctx)
	if err != nil || status.Generation != 4 {
		t.Fatalf("unexpected publications: %+v %v", status, err)
	}
	for _, stream := range status.Coverage {
		if stream.End != testNow.Add(-cfg.Age).UnixMilli() {
			t.Fatalf("stream failed to catch up: %+v", stream)
		}
	}
}

func TestSQLiteArchiverCompletenessAndEmptyWindows(t *testing.T) {
	for _, mode := range []string{"empty", "record-boundary", "partial"} {
		t.Run(mode, func(t *testing.T) {
			server, _ := newSQLiteSource(t, mode)
			cfg := baseConfig(server.URL)
			a, _ := newSQLiteArchiver(t, cfg)
			result, err := a.RunPass(context.Background(), testNow)
			status, statusErr := a.Catalog.Status(context.Background())
			if statusErr != nil {
				t.Fatal(statusErr)
			}
			if mode != "empty" {
				if err == nil || status.Generation != 0 || result.CommittedWindows != 0 {
					t.Fatalf("incomplete source published: %+v %+v %v", status, result, err)
				}
				return
			}
			if err != nil || result.CommittedWindows != 1 || result.Samples != 0 || result.Bytes != 0 {
				t.Fatalf("empty window failed: %+v %v", result, err)
			}
			plan, err := a.Catalog.Select(context.Background(), catalog.Selection{Tenant: "7:3", Start: cfg.StartMS, End: cfg.EndMS})
			if err != nil || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 0 {
				t.Fatalf("empty publication has objects: %+v %v", plan, err)
			}
			if a.Store.(*objectstore.Local).Stats().Puts != 0 {
				t.Fatal("empty window uploaded unnecessary objects")
			}
		})
	}
}

func TestSQLiteArchiverTimeoutYieldsToHealthyStreams(t *testing.T) {
	server, source := newSQLiteSource(t, "stall")
	source.down.Store(true)
	cfg := baseConfig(server.URL)
	cfg.Tenants = []string{"7:3", "8:1"}
	a, _ := newSQLiteArchiver(t, cfg)
	a.WindowTimeout = 250 * time.Millisecond
	result, err := a.RunPass(context.Background(), testNow)
	if !errors.Is(err, context.DeadlineExceeded) || result.CommittedWindows != 1 {
		t.Fatalf("stalled source prevented healthy publication: %+v %v", result, err)
	}
	status, err := a.Catalog.Status(context.Background())
	if err != nil || status.Generation != 1 {
		t.Fatalf("unexpected state after timeout: %+v %v", status, err)
	}
	for _, stream := range status.Coverage {
		want := cfg.CoverageStart
		if stream.Tenant == "8:1" {
			want += cfg.Window.Milliseconds()
		}
		if stream.End != want {
			t.Fatalf("wrong stream advanced: %+v", stream)
		}
	}
}

func TestSQLiteArchiverRejectsPolicyDriftAndExpiredSource(t *testing.T) {
	server, source := newSQLiteSource(t, "")
	cfg := baseConfig(server.URL)
	a, _ := newSQLiteArchiver(t, cfg)
	a.Config.Age += time.Hour
	if _, err := a.RunPass(context.Background(), testNow); err == nil {
		t.Fatal("changed archive-age contract accepted")
	}
	a.Config = cfg
	if _, err := a.RunPass(context.Background(), testNow.Add(48*time.Hour)); err == nil {
		t.Fatal("expired source treated as an empty successful interval")
	}
	if source.exportCount("7:3") != 0 {
		t.Fatal("invalid policy/retention triggered an export")
	}
}

func TestSQLiteArchiverRechecksRetentionBeforeCommit(t *testing.T) {
	server, _ := newSQLiteSource(t, "")
	cfg := baseConfig(server.URL)
	cfg.Retention = 4*time.Hour + 500*time.Millisecond
	var delayed bool
	cfg.Hook = func(stage string) error {
		if stage == "exported" {
			delayed = true
			time.Sleep(600 * time.Millisecond)
		}
		return nil
	}
	a, _ := newSQLiteArchiver(t, cfg)
	if _, err := a.RunPass(context.Background(), testNow); err == nil {
		t.Fatal("elapsed retention safety margin accepted")
	}
	if !delayed {
		t.Fatal("archive failed before the retention-expiry delay")
	}
	status, err := a.Catalog.Status(context.Background())
	if err != nil || status.Generation != 0 {
		t.Fatalf("late publication advanced coverage: %+v %v", status, err)
	}
}

func TestSQLiteArchiverCancellationStopsExport(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	a, _ := newSQLiteArchiver(t, baseConfig(server.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := a.RunPass(ctx, testNow); done <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected cancellation result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("archive export ignored cancellation")
	}
	status, err := a.Catalog.Status(context.Background())
	if err != nil || status.Generation != 0 {
		t.Fatalf("cancelled export published: %+v %v", status, err)
	}
}
