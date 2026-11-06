package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/yuuki/vmtier/internal/pack"
)

func TestIncrementalRawEnvelopeAndLimits(t *testing.T) {
	const series = `{"metric":{"__name__":"x"},"values":[[1.001,"3"],[1.002,"4"]]}`
	const valid = `{"status":"success","data":{"resultType":"matrix","result":[` + series + `]}}`
	want := map[string][]sample{`{"__name__":"x"}`: {{1001, 3}, {1002, 4}}}
	for name, body := range map[string]string{
		"pinned_order": valid,
		"whitespace":   valid + " \n\t",
		"reordered":    `{"data":{"result":[{"values":[[1.001,"3"],[1.002,"4"]],"metric":{"__name__":"x"}}],"resultType":"matrix"},"status":"success"}`,
		"extensions":   `{"stats":{"samples":[1,2,3]},"warnings":[],"isPartial":false,"data":{"resultType":"matrix","result":[` + series + `]},"status":"success"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyRawResponse(strings.NewReader(body), int64(len(body)), 2, 1000, 2000, want); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, body := range map[string]string{
		"trailing_json":       valid + `{}`,
		"trailing_junk":       valid + `x`,
		"missing_status":      `{"data":{"resultType":"matrix","result":[` + series + `]}}`,
		"failure_footer":      `{"data":{"resultType":"matrix","result":[` + series + `]},"status":"error"}`,
		"partial_footer":      valid[:len(valid)-1] + `,"isPartial":true}`,
		"warning_footer":      valid[:len(valid)-1] + `,"warnings":["partial"]}`,
		"duplicate_status":    valid[:len(valid)-1] + `,"status":"success"}`,
		"duplicate_data":      valid[:len(valid)-1] + `,"data":{"resultType":"matrix","result":[]}}`,
		"duplicate_series":    strings.Replace(valid, series, series+`,`+series, 1),
		"missing_values":      strings.Replace(valid, `,"values":[[1.001,"3"],[1.002,"4"]]`, "", 1),
		"missing_metric":      strings.Replace(valid, `"metric":{"__name__":"x"},`, "", 1),
		"short_row":           strings.Replace(valid, `[1.001,"3"]`, `[1.001]`, 1),
		"long_row":            strings.Replace(valid, `[1.001,"3"]`, `[1.001,"3","extra"]`, 1),
		"extra_row":           strings.Replace(valid, `[1.002,"4"]`, `[1.002,"4"],[1.003,"5"]`, 1),
		"missing_row":         strings.Replace(valid, `,[1.002,"4"]`, "", 1),
		"wrong_type":          strings.Replace(valid, `"matrix"`, `"vector"`, 1),
		"reordered_bad_value": `{"data":{"result":[{"values":[[1.001,"3"],[1.002,"5"]],"metric":{"__name__":"x"}}],"resultType":"matrix"},"status":"success"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyRawResponse(strings.NewReader(body), maxOracleBytes, 10, 1000, 2000, want); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
	// Every truncated prefix must fail, including cuts after complete samples.
	for cut := 0; cut < len(valid); cut++ {
		if err := verifyRawResponse(strings.NewReader(valid[:cut]), maxOracleBytes, 10, 1000, 2000, want); err == nil {
			t.Fatalf("accepted prefix ending at %d", cut)
		}
	}
	if err := verifyRawResponse(strings.NewReader(valid), int64(len(valid)-1), 2, 1000, 2000, want); err == nil || !strings.Contains(err.Error(), "response too large") {
		t.Fatalf("byte bound: %v", err)
	}
	if err := verifyRawResponse(strings.NewReader(valid), maxOracleBytes, 1, 1000, 2000, want); err == nil || !strings.Contains(err.Error(), "sample limit") {
		t.Fatalf("sample bound: %v", err)
	}
	// The body read must finish successfully, not just parse a complete object.
	readErr := errors.New("injected body read failure")
	if err := verifyRawResponse(io.MultiReader(strings.NewReader(valid), errorReader{readErr}), maxOracleBytes, 2, 1000, 2000, want); !errors.Is(err, readErr) {
		t.Fatalf("lost read failure: %v", err)
	}
	for _, result := range []string{"[]", "null"} {
		body := `{"status":"success","data":{"resultType":"matrix","result":` + result + `}}`
		if err := verifyRawResponse(strings.NewReader(body), maxOracleBytes, 1, 1000, 2000, nil); err != nil {
			t.Fatal(err)
		}
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestPhysicalSampleCapBeforeOracleDeduplication(t *testing.T) {
	native := nativeFixture(1000, 2000, true)
	// Repeated native blocks consume the physical-sample budget even though the
	// independent raw matrix contains only three deduplicated timestamps.
	native = append(native, native[16:]...)
	idx, data, err := pack.Build(bytes.NewReader(native), pack.Tenant{}, 1000, 2000, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = extractSamples(idx, data, 4); err == nil {
		t.Fatal("replicated physical samples bypassed verifier bound")
	}
	rows, err := extractSamples(idx, data, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, values := range rows {
		if len(values) != 3 {
			t.Fatalf("deduplicated samples: %d", len(values))
		}
	}
}

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
