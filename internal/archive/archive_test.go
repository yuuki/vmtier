package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

var testNow = time.UnixMilli(1000800000)

func baseConfig(endpoint string) Config {
	return Config{Tenants: []string{"7:3"}, Sources: []Source{{ID: "hot-a", URL: endpoint}}, CoverageStart: testNow.Add(-3 * time.Hour).UnixMilli(), StartMS: testNow.Add(-3 * time.Hour).UnixMilli(), EndMS: testNow.Add(-2 * time.Hour).UnixMilli(), Age: time.Hour, Retention: 24 * time.Hour, Lateness: time.Minute, SafetyMargin: time.Hour, Window: time.Hour, LatenessEnforced: true}
}
func nativeFixture(start, end int64, samples bool) []byte {
	b := encoding.MarshalInt64(nil, start)
	b = encoding.MarshalInt64(b, end-1)
	if !samples {
		return b
	}
	mn := pack.MetricName(pack.Tenant{}, map[string]string{"__name__": "x", "job": "test"})
	name := mn.MarshalNoAccountIDProjectID(nil)
	var block storage.Block
	block.Init(&storage.TSID{}, []int64{start, start + 10, start + 10, end - 1, end}, []int64{1, 2, 3, 4, 5}, 0, 64)
	p := block.MarshalPortable(nil)
	b = encoding.MarshalUint32(b, uint32(len(name)))
	b = append(b, name...)
	b = encoding.MarshalUint32(b, uint32(len(p)))
	return append(b, p...)
}
func sourceServer(t *testing.T, cfg Config, mode string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !strings.HasPrefix(r.URL.Path, "/select/7:3/prometheus/api/v1/") {
			t.Errorf("wrong tenant path %q", r.URL.Path)
		}
		if r.URL.Query().Get("deny_partial_response") != "1" {
			t.Error("partial response guard missing")
		}
		if strings.HasSuffix(r.URL.Path, "/export/native") {
			w.Header().Set("Content-Type", "VictoriaMetrics/native")
			b := nativeFixture(cfg.StartMS, cfg.EndMS, mode != "empty")
			if mode == "truncated" {
				b = b[:len(b)-1]
			}
			if mode == "record-boundary" {
				b = b[:16]
			}
			w.Write(b)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/query") {
			if r.URL.Query().Get("time") != seconds(cfg.EndMS-1) {
				t.Errorf("incorrect verification time")
			}
			if r.URL.Query().Get("nocache") != "1" {
				t.Error("verification cache guard missing")
			}
			if strings.HasPrefix(r.URL.Query().Get("query"), "{") {
				values := []any{[]any{float64(cfg.StartMS) / 1000, "1"}, []any{float64(cfg.StartMS+10) / 1000, "3"}, []any{float64(cfg.EndMS-1) / 1000, "4"}}
				result := []any{map[string]any{"metric": map[string]string{"__name__": "x", "job": "test"}, "values": values}}
				if mode == "empty" {
					result = nil
				}
				json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": result}})
				return
			}
			value := "3"
			if strings.HasPrefix(r.URL.Query().Get("query"), "stale_") {
				value = "0"
			}
			result := []any{map[string]any{"metric": map[string]string{"__name__": "x", "job": "test"}, "value": []any{float64(cfg.EndMS-1) / 1000, value}}}
			if mode == "empty" {
				result = nil
			}
			if mode == "bad-count" {
				result[0].(map[string]any)["value"] = []any{1, "2"}
			}
			out := map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}}
			if mode == "partial" {
				out["isPartial"] = true
			}
			json.NewEncoder(w).Encode(out)
			return
		}
		http.Error(w, "unexpected", http.StatusNotFound)
	}))
	return s, calls
}
func localStore(t *testing.T) *objectstore.Local {
	t.Helper()
	s, e := objectstore.NewLocal(t.TempDir(), 64<<20)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestAgeAndSafetyValidation(t *testing.T) {
	cfg := baseConfig("http://localhost:8481")
	if e := cfg.Validate(testNow); e != nil {
		t.Fatal(e)
	}
	tests := map[string]func(*Config){"lateness contract": func(c *Config) { c.LatenessEnforced = false }, "H below L": func(c *Config) { c.Lateness = c.Age }, "retention": func(c *Config) { c.Retention = c.Age + c.SafetyMargin }, "young": func(c *Config) { c.EndMS = testNow.UnixMilli() }, "expired": func(c *Config) { c.StartMS = testNow.Add(-24 * time.Hour).UnixMilli() }, "duplicate tenant": func(c *Config) { c.Tenants = []string{"7", "7:0"} }, "missing source": func(c *Config) { c.Sources = nil }, "source credentials": func(c *Config) { c.Sources[0].URL = "http://user:pass@localhost" }}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			c := cfg
			c.Sources = append([]Source{}, cfg.Sources...)
			mutate(&c)
			if e := c.Validate(testNow); e == nil {
				t.Fatal("accepted invalid contract")
			}
		})
	}
}
func TestRunCommitAndIdempotentRestart(t *testing.T) {
	cfg := baseConfig("")
	server, calls := sourceServer(t, cfg, "")
	defer server.Close()
	cfg.Sources[0].URL = server.URL
	store := localStore(t)
	controls := localCatalog(t)
	ctx := context.Background()
	r, e := Run(ctx, store, controls, cfg, testNow)
	if e != nil {
		t.Fatal(e)
	}
	if r.CommittedWindows != 1 || r.Samples != 3 || r.Entries != 1 {
		t.Fatalf("unexpected result %+v", r)
	}
	m, _, e := catalog.Load(ctx, controls)
	if e != nil {
		t.Fatal(e)
	}
	if m.Generation != 1 || len(m.Entries) != 1 {
		t.Fatal(m)
	}
	e0 := m.Entries[0]
	b, e := store.Get(ctx, e0.IndexKey)
	if e != nil {
		t.Fatal(e)
	}
	if pack.Sum(b) != e0.IndexSHA256 {
		t.Fatal("index checksum not bound")
	}
	if e0.Start != cfg.StartMS || e0.End != cfg.EndMS {
		t.Fatal("coverage differs")
	}
	before := calls.Load()
	r, e = Run(ctx, store, controls, cfg, testNow)
	if e != nil {
		t.Fatal(e)
	}
	if r.SkippedWindows != 1 || calls.Load() != before {
		t.Fatal("restart reexported committed window")
	}
}
func TestFailureNeverAdvancesCoverage(t *testing.T) {
	for _, mode := range []string{"truncated", "record-boundary", "partial", "bad-count"} {
		t.Run(mode, func(t *testing.T) {
			cfg := baseConfig("")
			s, _ := sourceServer(t, cfg, mode)
			defer s.Close()
			cfg.Sources[0].URL = s.URL
			store := localStore(t)
			controls := localCatalog(t)
			if _, e := Run(context.Background(), store, controls, cfg, testNow); e == nil {
				t.Fatal("failure accepted")
			}
			m, _, e := catalog.Load(context.Background(), controls)
			if e != nil || len(m.Entries) != 0 {
				t.Fatalf("coverage advanced: %+v, %v", m, e)
			}
		})
	}
}
func TestEmptyWindowCommitsVerifiedCoverage(t *testing.T) {
	cfg := baseConfig("")
	s, calls := sourceServer(t, cfg, "empty")
	defer s.Close()
	cfg.Sources[0].URL = s.URL
	store := localStore(t)
	controls := localCatalog(t)
	r, e := Run(context.Background(), store, controls, cfg, testNow)
	if e != nil {
		t.Fatal(e)
	}
	if r.Samples != 0 || r.CommittedWindows != 1 || calls.Load() != 4 {
		t.Fatalf("empty coverage not fully verified %+v calls=%d", r, calls.Load())
	}
}
func TestCrashRecovery(t *testing.T) {
	for _, stage := range []string{"exported", "uploaded", "verified", "committed"} {
		t.Run(stage, func(t *testing.T) {
			cfg := baseConfig("")
			s, _ := sourceServer(t, cfg, "")
			defer s.Close()
			cfg.Sources[0].URL = s.URL
			store := localStore(t)
			controls := localCatalog(t)
			cfg.Hook = func(got string) error {
				if got == stage {
					return errors.New("simulated stop")
				}
				return nil
			}
			if _, e := Run(context.Background(), store, controls, cfg, testNow); e == nil {
				t.Fatal("hook failure not returned")
			}
			m, _, e := catalog.Load(context.Background(), controls)
			if e != nil {
				t.Fatal(e)
			}
			expected := 0
			if stage == "committed" {
				expected = 1
			}
			if len(m.Entries) != expected {
				t.Fatal("incorrect publication before recovery")
			}
			cfg.Hook = nil
			r, e := Run(context.Background(), store, controls, cfg, testNow)
			if e != nil {
				t.Fatal(e)
			}
			if stage == "committed" && r.SkippedWindows != 1 {
				t.Fatal("committed recovery did not use checkpoint")
			}
			m, _, e = catalog.Load(context.Background(), controls)
			if e != nil || len(m.Entries) != 1 || m.Generation != 1 {
				t.Fatalf("bad resumed generation %+v %v", m, e)
			}
		})
	}
}

type faultBlobs struct {
	objectstore.BlobStore
	stage string
}

func (s faultBlobs) Put(c context.Context, k string, b []byte) error {
	if s.stage == "put" {
		return errors.New("injected PUT failure")
	}
	e := s.BlobStore.Put(c, k, b)
	if e == nil && s.stage == "ambiguous-put" {
		return errors.New("response lost after successful PUT")
	}
	return e
}
func (s faultBlobs) Get(c context.Context, k string) ([]byte, error) {
	b, e := s.BlobStore.Get(c, k)
	if e == nil {
		switch s.stage {
		case "get":
			return nil, errors.New("injected GET failure")
		case "corrupt":
			return append(b, 0), nil
		}
	}
	return b, e
}

type faultCatalog struct {
	catalog.Store
	stage string
}

func (s faultCatalog) Publish(c context.Context, expected uint64, next *catalog.Manifest) error {
	if s.stage == "conflict" {
		return catalog.ErrConflict
	}
	e := s.Store.Publish(c, expected, next)
	if e == nil && s.stage == "ambiguous" {
		return errors.New("response lost after publication")
	}
	return e
}
func TestObjectFailuresAndAmbiguousCommit(t *testing.T) {
	for _, stage := range []string{"put", "ambiguous-put", "get", "corrupt", "conflict", "ambiguous"} {
		t.Run(stage, func(t *testing.T) {
			cfg := baseConfig("")
			s, _ := sourceServer(t, cfg, "")
			defer s.Close()
			cfg.Sources[0].URL = s.URL
			store := localStore(t)
			controls := localCatalog(t)
			if _, e := Run(context.Background(), faultBlobs{store, stage}, faultCatalog{controls, stage}, cfg, testNow); e == nil {
				t.Fatal("expected failure")
			}
			m, _, e := catalog.Load(context.Background(), controls)
			if e != nil {
				t.Fatal(e)
			}
			expected := 0
			if stage == "ambiguous" {
				expected = 1
			}
			if len(m.Entries) != expected {
				t.Fatal("unexpected publication")
			}
			r, e := Run(context.Background(), store, controls, cfg, testNow)
			if e != nil {
				t.Fatal(e)
			}
			if stage == "ambiguous" && r.SkippedWindows != 1 {
				t.Fatal("ambiguous commit failed recovery")
			}
		})
	}
}

func TestIncompleteSourceSetAndTopologyChange(t *testing.T) {
	cfg := baseConfig("")
	s, _ := sourceServer(t, cfg, "")
	defer s.Close()
	cfg.Sources[0].URL = s.URL
	store := localStore(t)
	controls := localCatalog(t)
	goodSource := cfg.Sources[0]
	cfg.Sources = append(cfg.Sources, Source{"missing", "http://127.0.0.1:1"})
	if _, e := Run(context.Background(), store, controls, cfg, testNow); e == nil {
		t.Fatal("missing source accepted")
	}
	m, _, _ := catalog.Load(context.Background(), controls)
	if len(m.Entries) > 0 {
		t.Fatal("partial source set published")
	}
	cfg.Sources = []Source{goodSource}
	if _, e := Run(context.Background(), store, controls, cfg, testNow); e != nil {
		t.Fatal(e)
	}
	cfg.Sources[0].URL += "/changed"
	if _, e := Run(context.Background(), store, controls, cfg, testNow); e == nil {
		t.Fatal("topology fingerprint change accepted")
	}
}
func TestCountVerifierDedupAndStale(t *testing.T) {
	// The pinned VM prefers the maximum non-stale value for timestamp conflicts
	// (VictoriaMetrics issue #10196), while all-stale conflicts remain stale.
	for _, tc := range []struct {
		name       string
		duplicates []float64
		winner     float64
		counts     count
	}{
		{"staleLast", []float64{1, decimal.StaleNaN}, 1, count{normal: 2}},
		{"staleFirst", []float64{decimal.StaleNaN, 1}, 1, count{normal: 2}},
		{"maximumNonStale", []float64{1, decimal.StaleNaN, 2}, 2, count{normal: 2}},
		{"allStale", []float64{decimal.StaleNaN, decimal.StaleNaN}, decimal.StaleNaN, count{normal: 1, stale: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig("")
			var block storage.Block
			values := append(append([]float64(nil), tc.duplicates...), 3, 4)
			times := make([]int64, len(tc.duplicates))
			for i := range times {
				times[i] = cfg.StartMS
			}
			times = append(times, cfg.StartMS+1, cfg.EndMS)
			vals, scale := decimal.AppendFloatToDecimal(nil, values)
			block.Init(&storage.TSID{}, times, vals, scale, 64)
			payload := block.MarshalPortable(nil)
			name := pack.MetricName(pack.Tenant{}, map[string]string{"__name__": "x"}).MarshalNoAccountIDProjectID(nil)
			b := encoding.MarshalInt64(nil, cfg.StartMS)
			b = encoding.MarshalInt64(b, cfg.EndMS-1)
			b = encoding.MarshalUint32(b, uint32(len(name)))
			b = append(b, name...)
			b = encoding.MarshalUint32(b, uint32(len(payload)))
			b = append(b, payload...)
			idx, data, err := pack.Build(bytes.NewReader(b), pack.Tenant{}, cfg.StartMS, cfg.EndMS, pack.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := extractSamples(idx, data, 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("unexpected series count: %d", len(rows))
			}
			for _, samples := range rows {
				if len(samples) != 2 || samples[0].timestamp != cfg.StartMS || samples[1] != (sample{cfg.StartMS + 1, 3}) {
					t.Fatalf("unexpected deduplicated samples: %+v", samples)
				}
				if math.Float64bits(samples[0].value) != math.Float64bits(tc.winner) {
					t.Fatalf("wrong duplicate winner: got %v, want %v", samples[0].value, tc.winner)
				}
			}
			counts, err := sampleCounts(idx, data, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range counts {
				if got != tc.counts {
					t.Fatalf("dedup stale mismatch: got %+v, want %+v", got, tc.counts)
				}
			}
			if _, err = sampleCounts(idx, data, 1); err == nil {
				t.Fatal("verification expansion bound ignored")
			}
		})
	}
}

func TestFixedTenantSetAndWindowAlignment(t *testing.T) {
	cfg := baseConfig("")
	s, _ := sourceServer(t, cfg, "")
	defer s.Close()
	cfg.Sources[0].URL = s.URL
	store := localStore(t)
	controls := localCatalog(t)
	if _, e := Run(context.Background(), store, controls, cfg, testNow); e != nil {
		t.Fatal(e)
	}
	cfg.Tenants = append(cfg.Tenants, "9:0")
	if _, e := Run(context.Background(), store, controls, cfg, testNow); e == nil {
		t.Fatal("changed tenant set accepted")
	}
	cfg.Tenants = []string{"7:3"}
	cfg.StartMS++
	if _, e := Run(context.Background(), store, controls, cfg, testNow); e == nil {
		t.Fatal("overlapping window alignment accepted")
	}
}

func localCatalog(t *testing.T) *catalog.Local {
	t.Helper()
	s, e := catalog.NewLocal(t.TempDir(), "archive-tests", true)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
