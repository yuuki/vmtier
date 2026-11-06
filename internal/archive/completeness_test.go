package archive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/pack"
)

// A block-boundary cut can lose conflicting duplicate values without changing
// either count_over_time or stale_samples_over_time. VM's 1ms dedup chooses the
// maximum non-stale value at an identical timestamp, so only an exact sample
// comparison detects this case. The full and truncated streams are both valid
// native framing and the source's complete oracle response is identical.
func TestRecordBoundaryTruncationOfConflictingDuplicates(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		name := "complete"
		if truncate {
			name = "missing_winning_duplicate_block"
		}
		t.Run(name, func(t *testing.T) {
			cfg := baseConfig("")
			times := []int64{cfg.StartMS, cfg.StartMS + 10, cfg.EndMS - 1}
			labels := map[string]string{"__name__": "overlap", "job": "test"}
			nativeName := pack.MetricName(pack.Tenant{}, labels).MarshalNoAccountIDProjectID(nil)
			stream := encoding.MarshalInt64(nil, cfg.StartMS)
			stream = encoding.MarshalInt64(stream, cfg.EndMS-1)
			cut := 0
			for i, values := range [][]int64{{10, 20, 30}, {100, 200, 300}} {
				var b storage.Block
				b.Init(&storage.TSID{}, times, values, 0, 64)
				payload := b.MarshalPortable(nil)
				stream = encoding.MarshalUint32(stream, uint32(len(nativeName)))
				stream = append(stream, nativeName...)
				stream = encoding.MarshalUint32(stream, uint32(len(payload)))
				stream = append(stream, payload...)
				if i == 0 {
					cut = len(stream)
				}
			}
			if truncate {
				stream = stream[:cut]
			}
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/export/native") {
					w.Header().Set("Content-Type", "VictoriaMetrics/native")
					_, _ = w.Write(stream)
					return
				}
				if !strings.HasSuffix(r.URL.Path, "/query") {
					http.Error(w, "unexpected path", 404)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				query := r.URL.Query().Get("query")
				var resultType string
				var result []any
				if strings.HasPrefix(query, "{") {
					resultType = "matrix"
					values := []any{[]any{float64(times[0]) / 1000, "100"}, []any{float64(times[1]) / 1000, "200"}, []any{float64(times[2]) / 1000, "300"}}
					result = []any{map[string]any{"metric": labels, "values": values}}
				} else {
					resultType = "vector"
					value := "3"
					if strings.HasPrefix(query, "stale_") {
						value = "0"
					}
					result = []any{map[string]any{"metric": labels, "value": []any{float64(cfg.EndMS-1) / 1000, value}}}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
			}))
			defer source.Close()
			cfg.Sources[0].URL = source.URL
			store := localStore(t)
			controls := localCatalog(t)
			defer store.Close()
			_, err := Run(context.Background(), store, controls, cfg, testNow)
			if truncate && err == nil {
				t.Fatal("record-boundary cut discarded VM's winning duplicate values but archive committed")
			}
			if !truncate && err != nil {
				t.Fatalf("complete source rejected: %v", err)
			}
			m, _, loadErr := catalog.Load(context.Background(), controls)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if truncate && len(m.Entries) != 0 {
				t.Fatal("truncated source advanced committed coverage")
			}
			if !truncate && len(m.Entries) != 1 {
				t.Fatal("complete source did not commit")
			}
		})
	}
}
