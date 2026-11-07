package archive

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

func TestSQLiteMultiPackCrossPackConflictStaleAndOracleFailure(t *testing.T) {
	for _, badOracle := range []bool{false, true} {
		t.Run(map[bool]string{false: "verified", true: "oracle fails after staging"}[badOracle], func(t *testing.T) {
			cfg := baseConfig("")
			start, end := cfg.StartMS, cfg.EndMS
			metric := map[string]string{"__name__": "cross_pack", "job": "test"}
			mn := pack.MetricName(pack.Tenant{}, metric)
			name := mn.MarshalNoAccountIDProjectID(nil)
			body := encoding.MarshalInt64(nil, start)
			body = encoding.MarshalInt64(body, end-1)
			for _, record := range []struct {
				ts []int64
				vs []float64
			}{
				{[]int64{start, start + 10, start + 20, start + 30, start + 40}, []float64{1, 2, decimal.StaleNaN, 4, decimal.StaleNaN}},
				{[]int64{start + 10, start + 20, start + 30, start + 40, end - 1}, []float64{3, 5, 7, decimal.StaleNaN, decimal.StaleNaN}},
				{[]int64{start + 10, end - 1}, []float64{decimal.StaleNaN, 8}},
			} {
				values, scale := decimal.AppendFloatToDecimal(nil, record.vs)
				var block storage.Block
				block.Init(&storage.TSID{}, record.ts, values, scale, 64)
				payload := block.MarshalPortable(nil)
				body = encoding.MarshalUint32(body, uint32(len(name)))
				body = append(body, name...)
				body = encoding.MarshalUint32(body, uint32(len(payload)))
				body = append(body, payload...)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/export/native") {
					w.Header().Set("Content-Type", "VictoriaMetrics/native")
					w.Write(body)
					return
				}
				expr := r.URL.Query().Get("query")
				if strings.HasPrefix(expr, "{") {
					// VM #10196 prefers maximum non-stale duplicates across packs;
					// the all-stale conflict at start+40 must remain a stale marker.
					values := []any{[]any{float64(start) / 1000, "1"}, []any{float64(start+10) / 1000, "3"}, []any{float64(start+20) / 1000, "5"}, []any{float64(start+30) / 1000, "7"}, []any{float64(start+40) / 1000, "NaN"}, []any{float64(end-1) / 1000, "8"}}
					json.NewEncoder(w).Encode(map[string]any{"status": "success", "isPartial": badOracle, "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": metric, "values": values}}}})
					return
				}
				value := "5"
				if strings.HasPrefix(expr, "stale_") {
					value = "1"
				}
				json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": []any{map[string]any{"metric": metric, "value": []any{float64(end-1) / 1000, value}}}}})
			}))
			defer server.Close()
			cfg.Sources[0].URL = server.URL
			cfg.PackLimits.MaxBlocks = 1
			cfg.Memory = &MemoryStats{}
			a, _ := newSQLiteArchiver(t, cfg)
			result, err := a.RunPass(context.Background(), testNow)
			memory := cfg.Memory.Snapshot()
			if memory.VerifierBytes != 0 || memory.PackBytes != 0 || memory.VerifierPeakBytes < 12*16 || memory.PackPeakBytes == 0 {
				t.Fatalf("archive memory components missing or retained after completion: %+v", memory)
			}
			state, statusErr := a.Catalog.Status(context.Background())
			if statusErr != nil {
				t.Fatal(statusErr)
			}
			if a.Store.(*objectstore.Local).Stats().Puts != 6 {
				t.Fatal("must stage all three packs before independent verification")
			}
			if badOracle {
				if err == nil || !strings.Contains(err.Error(), "raw verification returned partial data") || state.Generation != 0 || result.CommittedWindows != 0 {
					t.Fatalf("failed oracle published staged packs: %+v %+v %v", state, result, err)
				}
			} else if err != nil || result.Samples != 6 || result.CommittedWindows != 1 || state.Generation != 1 {
				t.Fatalf("cross-pack conflicts/stale differ: %+v %v", result, err)
			}
		})
	}
}

func TestSQLiteMultiPackPublicationAndRestart(t *testing.T) {
	server, _ := newSQLiteSource(t, "multipack")
	cfg := baseConfig(server.URL)
	cfg.PackLimits.MaxBlocks = 1
	a, path := newSQLiteArchiver(t, cfg)
	ctx := context.Background()
	result, err := a.RunPass(ctx, testNow)
	if err != nil || result.CommittedWindows != 1 || result.Samples != 3 {
		t.Fatalf("cross-pack dedup: %+v %v", result, err)
	}
	plan, err := a.Catalog.Select(ctx, catalog.Selection{Tenant: "7:3", Start: cfg.StartMS, End: cfg.EndMS})
	if err != nil || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 3 {
		t.Fatalf("wrong logical publication: %+v %v", plan, err)
	}
	var total int64
	for _, ref := range plan.Windows[0].Packs {
		encoded, err := a.Store.Get(ctx, ref.IndexKey)
		if err != nil {
			t.Fatal(err)
		}
		idx, err := pack.ParseIndex(encoded, cfg.PackLimits)
		if err != nil || idx.Start != cfg.StartMS || idx.End != cfg.EndMS || idx.Tenant.String() != "7:3" {
			t.Fatalf("pack coverage/tenant differs: %+v %v", idx, err)
		}
		total += ref.DataBytes + ref.IndexBytes
	}
	if total != result.Bytes {
		t.Fatalf("result bytes=%d actual=%d", result.Bytes, total)
	}
	if err := a.Catalog.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := catalog.OpenSQLite(ctx, path, "archive-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	state, err := db.Status(ctx)
	if err != nil || state.Generation != 1 || state.Coverage[0].End != cfg.EndMS {
		t.Fatalf("restart lost atomic coverage: %+v %v", state, err)
	}
}

func TestSQLiteMultiPackFailedAttemptNeverPublishes(t *testing.T) {
	for _, mode := range []string{"multipack", "multipack-truncated", "physical-limit"} {
		t.Run(mode, func(t *testing.T) {
			sourceMode := "multipack"
			if mode == "multipack-truncated" {
				sourceMode = mode
			}
			server, _ := newSQLiteSource(t, sourceMode)
			cfg := baseConfig(server.URL)
			cfg.PackLimits.MaxBlocks = 1
			uploads := 0
			if mode == "multipack" {
				cfg.Hook = func(stage string) error {
					if stage == "uploaded" {
						uploads++
						if uploads == 2 {
							return errors.New("injected second upload failure")
						}
					}
					return nil
				}
			}
			if mode == "physical-limit" {
				cfg.MaxSamples = 14
			}
			a, _ := newSQLiteArchiver(t, cfg)
			ctx := context.Background()
			result, err := a.RunPass(ctx, testNow)
			if err == nil || result.CommittedWindows != 0 {
				t.Fatalf("failed attempt committed: %+v %v", result, err)
			}
			state, err := a.Catalog.Status(ctx)
			if err != nil || state.Generation != 0 || state.Coverage[0].End != cfg.CoverageStart {
				t.Fatalf("partial pack set published: %+v %v", state, err)
			}
			if a.Store.(*objectstore.Local).Stats().Puts == 0 {
				t.Fatal("failure did not exercise staged unpublished objects")
			}
			if mode != "multipack" {
				return
			}
			// Discard retry deadlines as on service restart. Previously staged keys
			// remain orphans; a successful retry publishes a complete fresh set.
			cfg.Hook = nil
			restarted := &Archiver{Store: a.Store, Catalog: a.Catalog, Config: cfg}
			result, err = restarted.RunPass(ctx, testNow.Add(time.Minute))
			if err != nil || result.CommittedWindows != 1 || result.Samples != 3 {
				t.Fatalf("restart retry failed: %+v %v", result, err)
			}
			plan, err := a.Catalog.Select(ctx, catalog.Selection{Tenant: "7:3", Start: cfg.StartMS, End: cfg.EndMS})
			if err != nil || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 3 {
				t.Fatalf("retry publication incomplete: %+v %v", plan, err)
			}
		})
	}
}
