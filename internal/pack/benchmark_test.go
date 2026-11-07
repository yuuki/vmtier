package pack

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// This codec microbenchmark is intentionally separate from S3/HTTP throughput.
// 1,000 series, 128 samples per series, 1-second scrape, 2 indexed labels.
func benchmarkNative() []byte {
	data := encoding.MarshalInt64(nil, 0)
	data = encoding.MarshalInt64(data, 128000-1)
	ts := make([]int64, 128)
	vs := make([]int64, 128)
	for i := range ts {
		ts[i] = int64(i) * 1000
		vs[i] = int64(i*i*17 + i%7)
	}
	for i := 0; i < 1000; i++ {
		b, _ := nativeFixture(0, 128000, map[string]string{"__name__": "benchmark_counter", "instance": strconv.Itoa(i)}, ts, vs, -3)
		data = append(data, b[16:]...)
	}
	return data
}

func BenchmarkBuildNativePack(b *testing.B) {
	native := benchmarkNative()
	b.SetBytes(int64(len(native)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := Build(bytes.NewReader(native), Tenant{1, 0}, 0, 128000, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPortableToWire(b *testing.B) {
	native := benchmarkNative()
	idx, data, err := Build(bytes.NewReader(native), Tenant{1, 0}, 0, 128000, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	ref := idx.Series[0].Blocks[0]
	payload := data[ref.Offset : ref.Offset+ref.Length]
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := LoadBlock(idx, 0, 0, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// Every Cold query that misses the cache parses and validates the index of each
// selected pack before any Range GET.
func BenchmarkParseIndex(b *testing.B) {
	idx, _, err := Build(bytes.NewReader(benchmarkNative()), Tenant{1, 0}, 0, 128000, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := MarshalIndex(idx, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseIndex(encoded, Limits{}); err != nil {
			b.Fatal(err)
		}
	}
}
