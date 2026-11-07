package gateway

import (
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/pack"
)

// The reader evaluates the matcher once per indexed series and the gateway once
// per returned block: an equality, a prefix regexp and a negative regexp.
func BenchmarkMatcher(b *testing.B) {
	m, err := CompileMatcher([][]storage.TagFilter{{
		{Key: nil, Value: []byte("http_requests_total")},
		{Key: []byte("instance"), Value: []byte("host-1.*"), IsRegexp: true},
		{Key: []byte("code"), Value: []byte("5.."), IsRegexp: true, IsNegative: true},
	}})
	if err != nil {
		b.Fatal(err)
	}
	const series = 1000
	labels := make([]map[string]string, series)
	names := make([]storage.MetricName, series)
	for i := range labels {
		labels[i] = map[string]string{"__name__": "http_requests_total", "instance": "host-" + strconv.Itoa(i), "code": strconv.Itoa(200 + i%400)}
		if err := names[i].Unmarshal(pack.MetricName(pack.Tenant{AccountID: 1}, labels[i]).Marshal(nil)); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("labels", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.MatchLabels(labels[i%series])
		}
	})
	b.Run("metric_name", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.Match(&names[i%series])
		}
	})
}
