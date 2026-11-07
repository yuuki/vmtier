package archive

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"testing"
)

// FuzzRawRow checks the oracle's row decoder: an accepted timestamp equals the
// exact decimal value of the JSON number in milliseconds, and formatting it as
// a query parameter parses back to the same millisecond.
func FuzzRawRow(f *testing.F) {
	for _, seed := range []string{`[1.001,"3"]`, `[1793941200.123,"NaN"]`, `[0,"+Inf"]`, `[1.1200,"-0"]`, `[1e3,"1"]`, `[1.0001,"1"]`, `["1","2"]`, `[9223372036854775.807,"1"]`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var pair []json.RawMessage
		if json.Unmarshal(data, &pair) != nil {
			return
		}
		row, err := rawRow(pair)
		if err != nil {
			return
		}
		exact, ok := new(big.Rat).SetString(string(pair[0]))
		if !ok {
			t.Fatalf("accepted timestamp %s is not a decimal number", pair[0])
		}
		if want := exact.Mul(exact, big.NewRat(1000, 1)); !want.IsInt() || !want.Num().IsInt64() || want.Num().Int64() != row.timestamp {
			t.Fatalf("timestamp %s decoded as %d ms, exact value %s ms", pair[0], row.timestamp, want.RatString())
		}
		var text string
		if err := json.Unmarshal(pair[1], &text); err != nil {
			t.Fatal(err)
		}
		want, err := strconv.ParseFloat(text, 64)
		if err != nil || math.Float64bits(want) != math.Float64bits(row.value) && !(math.IsNaN(want) && math.IsNaN(row.value)) {
			t.Fatalf("value %q decoded as %v", text, row.value)
		}
		if again, err := rawTimestamp([]byte(seconds(row.timestamp))); err != nil || again != row.timestamp {
			t.Fatalf("query parameter %q does not round-trip to %d: %d %v", seconds(row.timestamp), row.timestamp, again, err)
		}
	})
}

// FuzzVerifyRawResponse checks the independent raw oracle on untrusted HTTP
// bodies: it never panics, and a body it accepts is no longer accepted once
// truncated, because only a complete envelope proves the source finished.
func FuzzVerifyRawResponse(f *testing.F) {
	const series = `{"metric":{"__name__":"x"},"values":[[1.001,"3"],[1.002,"4"]]}`
	for _, seed := range []string{
		`{"status":"success","data":{"resultType":"matrix","result":[` + series + `]}}`,
		`{"data":{"result":[{"values":[[1.001,"3"],[1.002,"4"]],"metric":{"__name__":"x"}}],"resultType":"matrix"},"status":"success"}` + "\n",
		`{"stats":{"samples":[1,2,3]},"warnings":[],"isPartial":false,"data":{"resultType":"matrix","result":[` + series + `]},"status":"success"}`,
		`{"status":"success","data":{"resultType":"matrix","result":[` + series + `]},"isPartial":true}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<16 {
			return
		}
		want := map[string][]sample{`{"__name__":"x"}`: {{1001, 3}, {1002, 4}}}
		if verifyRawResponse(bytes.NewReader(body), int64(len(body)), 10, 1000, 2000, want) != nil {
			return
		}
		end := len(bytes.TrimRight(body, " \t\r\n"))
		for _, cut := range []int{end - 1, end / 2, 0} {
			if verifyRawResponse(bytes.NewReader(body[:cut]), int64(len(body)), 10, 1000, 2000, want) == nil {
				t.Fatalf("truncated body accepted (cut at %d of %d): %q", cut, len(body), body)
			}
		}
	})
}
