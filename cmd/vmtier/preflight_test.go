package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	vmencoding "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/config"
)

func TestPreflightCommandReadsOnlySourceAndNeverInitializesCatalog(t *testing.T) {
	now := time.UnixMilli(1000800000)
	start, end := now.Add(-3*time.Hour).UnixMilli(), now.Add(-2*time.Hour).UnixMilli()
	native := vmencoding.MarshalInt64(nil, start)
	native = vmencoding.MarshalInt64(native, end-1)
	requests := 0
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/select/1:2/prometheus/api/v1/") {
			t.Errorf("non-source-read request: %s %s", r.Method, r.URL)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/export/native") {
			w.Header().Set("Content-Type", "VictoriaMetrics/native")
			w.Write(native)
			return
		}
		kind := "vector"
		if strings.HasPrefix(r.URL.Query().Get("query"), "{") {
			kind = "matrix"
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": kind, "result": []any{}}})
	}))
	defer source.Close()
	root := t.TempDir()
	catalogPath := filepath.Join(root, "not-initialized")
	cfg := config.Config{
		Store:   config.Store{Type: "s3", Bucket: "not-contacted", Endpoint: source.URL, Region: "test", Prefix: "preflight"},
		Catalog: config.Catalog{Type: "sqlite", Path: catalogPath, ID: "candidate"},
		Tenants: []string{"1:2"}, Sources: []config.Source{{ID: "hot", URL: source.URL}},
		ArchiveAge: "1h", HotRetention: "24h", Lateness: "30m", SafetyMargin: "30m", Window: "1h", LatenessEnforced: true, CoverageStart: start,
	}
	file := filepath.Join(root, "candidate.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	args := []string{"preflight", "-config", file, "-start", strconv.FormatInt(start, 10), "-end", strconv.FormatInt(end, 10)}
	var output bytes.Buffer
	if err := preflightCommand(context.Background(), args, &output, now); err != nil {
		t.Fatal(err)
	}
	var result archive.PreflightResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || !result.Verified || result.Tenant != "1:2" || result.NativeInputBytes != 16 {
		t.Fatalf("invalid report: %s %v", output.Bytes(), err)
	}
	if _, err := os.Stat(catalogPath); !os.IsNotExist(err) {
		t.Fatalf("catalog path was touched: %v", err)
	}
	// Even an existing directory with unreadable/noncatalog contents remains
	// untouched: this command never opens an authority or takes ownership.
	if err := os.Mkdir(catalogPath, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(catalogPath, "existing-state")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0400); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := preflightCommand(context.Background(), args, &output, now); err != nil {
		t.Fatal(err)
	}
	contents, _ := os.ReadFile(sentinel)
	entries, _ := os.ReadDir(catalogPath)
	if string(contents) != "preserve" || len(entries) != 1 || requests == 0 {
		t.Fatalf("preflight mutated state or never read source: %q %+v %d", contents, entries, requests)
	}

	// PostgreSQL uses the same pack pipeline. An unset DSN must not matter:
	// preflight reads only Hot, without creating a DB client or an S3 client.
	cfg.Catalog = config.Catalog{Type: "postgres", ID: "candidate-pg", DSNEnv: "VMTIER_PREFLIGHT_TEST_DSN"}
	cfg.Service.MaxReplicas = 2
	cfg.Store.RequestsPerSecond, cfg.Store.MaxConcurrentRequests, cfg.Store.MaxQueuedRequests = 80, 8, 64
	t.Setenv(cfg.Catalog.DSNEnv, "")
	writeCLIConfig(t, file, cfg)
	output.Reset()
	before := requests
	if err := preflightCommand(context.Background(), args, &output, now); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || !result.Verified || result.NativeInputBytes != 16 || requests <= before {
		t.Fatalf("PostgreSQL preflight failed to read source: %s %v", output.Bytes(), err)
	}
}
