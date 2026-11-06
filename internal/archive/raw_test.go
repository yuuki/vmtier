package archive

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/yuuki/vmtier/internal/pack"
)

func TestRawTimestampExactMilliseconds(t *testing.T) {
	cases := map[string]int64{"0": 0, "0.001": 1, "1.1": 1100, "1793941200.123": 1793941200123, "1793941200.1200": 1793941200120}
	for text, want := range cases {
		got, e := rawTimestamp([]byte(text))
		if e != nil || got != want {
			t.Fatalf("%s -> %d %v, want %d", text, got, e, want)
		}
	}
	for _, text := range []string{"-1", "9223372036854776", "1793941200.1234", "1e9", "NaN"} {
		if _, e := rawTimestamp([]byte(text)); e == nil {
			t.Fatalf("accepted timestamp %q", text)
		}
	}
}

func TestRawOracleExactValuesAndCompletedEnvelope(t *testing.T) {
	const start int64 = 1793941200123
	labels := map[string]string{"__name__": "raw_fixture", "job": "verify"}
	keyBytes, _ := json.Marshal(labels)
	key := string(keyBytes)
	expected := []sample{{start, 1.0000000000000002}, {start + 1, math.Inf(1)}, {start + 2, math.Inf(-1)}, {start + 3, decimal.StaleNaN}, {start + 4, 0}}
	rows := make([]any, len(expected))
	for i, r := range expected {
		rows[i] = []any{json.Number(seconds(r.timestamp)), strconv.FormatFloat(r.value, 'g', -1, 64)}
	}
	base := func() map[string]any {
		return map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": labels, "values": rows}}}}
	}
	for _, mode := range []string{"valid", "no-footer", "value", "timestamp", "extra-series", "partial", "finite-as-nan", "stale-as-finite"} {
		t.Run(mode, func(t *testing.T) {
			payload := base()
			switch mode {
			case "partial":
				payload["isPartial"] = true
			case "extra-series":
				payload["data"].(map[string]any)["result"] = append(payload["data"].(map[string]any)["result"].([]any), map[string]any{"metric": map[string]string{"__name__": "extra"}, "values": rows})
			case "value", "timestamp", "finite-as-nan", "stale-as-finite":
				altered := append([]any{}, rows...)
				switch mode {
				case "value":
					altered[0] = []any{json.Number(seconds(start)), "1"}
				case "timestamp":
					altered[0] = []any{json.Number(seconds(start - 1)), "1.0000000000000002"}
				case "finite-as-nan":
					altered[0] = []any{json.Number(seconds(start)), "NaN"}
				case "stale-as-finite":
					altered[3] = []any{json.Number(seconds(start + 3)), "0"}
				}
				payload["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["values"] = altered
			}
			body, e := json.Marshal(payload)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "no-footer" {
				body = body[:len(body)-1]
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Query().Get("query"), "{") {
					t.Error("not raw selector")
				}
				w.Write(body)
			}))
			defer server.Close()
			cfg := Config{HTTPClient: server.Client(), MaxSamples: 100}
			err := verifyRaw(context.Background(), cfg, Source{URL: server.URL}, pack.Tenant{}, start, start+100, map[string][]sample{key: expected})
			if mode == "valid" && err != nil {
				t.Fatal(err)
			}
			if mode != "valid" && err == nil {
				t.Fatal("invalid raw oracle accepted")
			}
		})
	}
}
