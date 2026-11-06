package archive

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/yuuki/vmtier/internal/pack"
)

type rawTransport func(*http.Request) (*http.Response, error)

func (f rawTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Keep both the body and expected native rows outside the timed region: this
// measures the independent oracle's parsing/comparison allocations, not fixture
// creation, HTTP sockets, or native decoding.
func BenchmarkVerifyRaw(b *testing.B) {
	const series, rows = 100, 2000
	want := make(map[string][]sample, series)
	var body strings.Builder
	body.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for s := 0; s < series; s++ {
		if s > 0 {
			body.WriteByte(',')
		}
		key := fmt.Sprintf(`{"__name__":"fixture","series":"%d"}`, s)
		fmt.Fprintf(&body, `{"metric":%s,"values":[`, key)
		values := make([]sample, rows)
		for i := range values {
			if i > 0 {
				body.WriteByte(',')
			}
			values[i] = sample{int64(1000 + i), float64(i)}
			fmt.Fprintf(&body, `[%s,"%d"]`, seconds(values[i].timestamp), i)
		}
		want[key] = values
		body.WriteString(`]}`)
	}
	body.WriteString(`]}}`)
	payload := body.String()
	c := Config{MaxSamples: series * rows, HTTPClient: &http.Client{Transport: rawTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(payload)), Header: make(http.Header)}, nil
	})}}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := verifyRaw(context.Background(), c, Source{URL: "http://oracle.invalid"}, pack.Tenant{}, 1000, 3000, want); err != nil {
			b.Fatal(err)
		}
	}
}
