package gateway

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

type backendFunc func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error)

func (f backendFunc) Search(ctx context.Context, sq *storage.SearchQuery) ([]storage.MetricBlock, error) {
	return f(ctx, sq)
}

func block(account, project uint32, name string, timestamps []int64, labels map[string]string) storage.MetricBlock {
	mn := storage.MetricName{AccountID: account, ProjectID: project, MetricGroup: []byte(name)}
	for k, v := range labels {
		mn.AddTag(k, v)
	}
	var b storage.Block
	values := make([]int64, len(timestamps))
	for i := range values {
		values[i] = int64(i + 1)
	}
	b.Init(&storage.TSID{AccountID: account, ProjectID: project}, timestamps, values, 0, 64)
	b.MarshalData(0, 0)
	return storage.MetricBlock{MetricName: mn.Marshal(nil), Block: b}
}

func TestMatcherSemantics(t *testing.T) {
	labels := []map[string]string{
		{"__name__": "metric", "job": "api"},
		{"__name__": "metric"},
		{"__name__": "metric", "job": ""},
		{"__name__": "metric", "job": "api-worker"},
	}
	for _, tc := range []struct {
		name, value     string
		negative, regex bool
		want            string
	}{
		{"equal", "api", false, false, "1000"},
		{"notEqual", "api", true, false, "0111"},
		{"emptyEqual", "", false, false, "0110"},
		{"emptyNotEqual", "", true, false, "1001"},
		{"regexAnchored", "api", false, true, "1000"},
		{"regexWildcard", "api.*", false, true, "1001"},
		{"regexAlternative", "api|", false, true, "1110"},
		{"negativeRegex", "api|", true, true, "0001"},
		{"negativeDotStarVM", ".*", true, true, "0110"},
		{"positiveDotStar", ".*", false, true, "1111"},
		{"negativeAnchoredDotStar", "^.*$", true, true, "0000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := CompileMatcher([][]storage.TagFilter{{{Key: []byte("job"), Value: []byte(tc.value), IsNegative: tc.negative, IsRegexp: tc.regex}}})
			if err != nil {
				t.Fatal(err)
			}
			for i, l := range labels {
				if got := m.MatchLabels(l); got != (tc.want[i] == '1') {
					t.Errorf("labels=%v got=%v", l, got)
				}
			}
		})
	}
	m, err := CompileMatcher([][]storage.TagFilter{
		{{Key: nil, Value: []byte("missing")}},
		{{Key: nil, Value: []byte("metric")}, {Key: []byte("job"), Value: []byte("api")}},
	})
	if err != nil || !m.MatchLabels(labels[0]) || m.MatchLabels(labels[1]) {
		t.Fatalf("OR/AND/name semantics: %v", err)
	}
	if _, err := CompileMatcher([][]storage.TagFilter{{{Key: []byte("job"), Value: []byte("["), IsRegexp: true}}}); err == nil {
		t.Fatal("invalid regex accepted")
	}
	if _, err := CompileMatcher([][]storage.TagFilter{{{Key: []byte("__graphite__"), Value: []byte("*")}}}); err == nil {
		t.Fatal("unsupported Graphite accepted")
	}
}

func TestSearchMetadataAndTimeFiltering(t *testing.T) {
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{
			block(3, 7, "cpu", []int64{1000, 2000}, map[string]string{"job": "api"}),
			block(3, 7, "outside", []int64{10, 20}, map[string]string{"other": "outside"}),
			block(3, 7, "cpu", []int64{2000, 3000}, map[string]string{"job": "api"}),
		}, nil
	}), Limits{})
	sq := storage.NewSearchQuery(3, 7, 1500, 2500, nil, 100)
	labels, err := a.LabelNames(context.Background(), nil, sq, 100)
	if err != nil || strings.Join(labels, ",") != "__name__,job" {
		t.Fatalf("labels=%v err=%v", labels, err)
	}
	values, err := a.LabelValues(context.Background(), nil, sq, "__name__", 100)
	if err != nil || strings.Join(values, ",") != "cpu" {
		t.Fatalf("values=%v err=%v", values, err)
	}
	names, err := a.SearchMetricNames(context.Background(), nil, sq)
	if err != nil || len(names) != 1 {
		t.Fatalf("names=%v err=%v", names, err)
	}
	it, err := a.InitSearch(context.Background(), nil, sq)
	if err != nil {
		t.Fatal(err)
	}
	defer it.MustClose()
	count := 0
	for {
		wire, ok := it.NextBlock(context.Background(), nil)
		if !ok {
			break
		}
		count++
		// vmselect's actual wire decoder must accept the produced block.
		var decoded storage.MetricBlock
		tail, err := decoded.Unmarshal(wire)
		if err != nil || len(tail) != 0 {
			t.Fatalf("wire round trip: %v", err)
		}
		if err := decoded.Block.UnmarshalData(); err != nil {
			t.Fatal(err)
		}
	}
	if count != 2 || it.Error() != nil {
		t.Fatalf("count=%d err=%v", count, it.Error())
	}
}

func TestFailClosed(t *testing.T) {
	sq := storage.NewSearchQuery(3, 7, 0, 5000, nil, 100)
	for _, tc := range []struct {
		name    string
		backend backendFunc
		limits  Limits
		needle  string
	}{
		{"missingObject", func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
			return nil, errors.New("object missing")
		}, Limits{}, "object missing"},
		{"tenantMismatch", func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
			return []storage.MetricBlock{block(4, 7, "cpu", []int64{1000}, nil)}, nil
		}, Limits{}, "tenant mismatch"},
		{"blockLimit", func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
			return []storage.MetricBlock{block(3, 7, "a", []int64{1000}, nil), block(3, 7, "b", []int64{1000}, nil)}, nil
		}, Limits{MaxBlocks: 1}, "block limit"},
		{"seriesLimit", func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
			return []storage.MetricBlock{block(3, 7, "a", []int64{1000}, nil), block(3, 7, "b", []int64{1000}, nil)}, nil
		}, Limits{MaxSeries: 1}, "series limit"},
		{"byteLimit", func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
			return []storage.MetricBlock{block(3, 7, "a", []int64{1000}, nil)}, nil
		}, Limits{MaxResponseBytes: 1}, "byte limit"},
		{"deadline", func(ctx context.Context, _ *storage.SearchQuery) ([]storage.MetricBlock, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, Limits{MaxRequestDuration: time.Millisecond}, "deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := New(tc.backend, tc.limits)
			it, err := a.InitSearch(context.Background(), nil, sq)
			if it != nil || err == nil || !strings.Contains(err.Error(), tc.needle) {
				t.Fatalf("iterator=%v err=%v", it, err)
			}
			if a.Errors.Load() != 1 {
				t.Fatalf("error counter not incremented")
			}
		})
	}
	a := New(nil, Limits{})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if it, err := a.InitSearch(ctx, nil, sq); it != nil || err == nil {
		t.Fatal("expired query accepted")
	}
	if _, err := a.DeleteSeries(context.Background(), nil, sq); err == nil {
		t.Fatal("delete accepted")
	}
	if err := a.RegisterMetricNames(context.Background(), nil, nil); err == nil {
		t.Fatal("write accepted")
	}
	if _, err := a.Tenants(context.Background(), nil, storage.TimeRange{}); err == nil {
		t.Fatal("unsupported tenant discovery accepted")
	}
}

func TestPreservesNativeMetricNameBytes(t *testing.T) {
	mb := block(3, 7, "ordered", []int64{1000}, nil)
	mn := storage.MetricName{AccountID: 3, ProjectID: 7, MetricGroup: []byte("ordered")}
	for _, key := range []string{"job", "instance", "aaa", "z"} {
		mn.AddTag(key, key+"-value")
	}
	mb.MetricName = mn.Marshal(nil)
	want := string(mb.MetricName)
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{mb}, nil
	}), Limits{})
	names, err := a.SearchMetricNames(context.Background(), nil, storage.NewSearchQuery(3, 7, 0, 2000, nil, 10))
	if err != nil || len(names) != 1 || names[0] != want {
		t.Fatalf("native name order changed: %v", err)
	}
}

func TestRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	a := New(backendFunc(func(ctx context.Context, _ *storage.SearchQuery) ([]storage.MetricBlock, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), Limits{MaxRequestDuration: time.Minute})
	done := make(chan error, 1)
	go func() {
		_, err := a.InitSearch(ctx, nil, storage.NewSearchQuery(3, 7, 0, 5000, nil, 100))
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not receive request cancellation")
	}
}

func TestIteratorCancellationAndAppend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{
			block(3, 7, "cpu", []int64{1000}, nil),
			block(3, 7, "cpu", []int64{1001}, nil),
		}, nil
	}), Limits{})
	it, err := a.InitSearch(ctx, nil, storage.NewSearchQuery(3, 7, 0, 2000, nil, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer it.MustClose()
	prefix := []byte("prefix")
	wire, ok := it.NextBlock(ctx, append([]byte(nil), prefix...))
	if !ok || !bytes.HasPrefix(wire, prefix) {
		t.Fatalf("iterator did not append to destination: %x", wire)
	}
	var decoded storage.MetricBlock
	if tail, err := decoded.Unmarshal(wire[len(prefix):]); err != nil || len(tail) != 0 {
		t.Fatalf("invalid wire block: tail=%x err=%v", tail, err)
	}
	cancel()
	if got, ok := it.NextBlock(ctx, prefix); ok || !bytes.Equal(got, prefix) || !errors.Is(it.Error(), context.Canceled) {
		t.Fatalf("canceled iterator returned data: %x %v %v", got, ok, it.Error())
	}
	if _, ok := it.NextBlock(context.Background(), nil); ok {
		t.Fatal("iterator resumed after cancellation")
	}
}

func TestMetricsMetadataUnsupported(t *testing.T) {
	a := New(nil, Limits{})
	if rows, err := a.GetMetadataRecords(context.Background(), nil, nil, 100, ""); rows != nil || err == nil || !strings.Contains(err.Error(), "unsupported RPC searchMetadata") {
		t.Fatalf("metadata RPC must fail explicitly: rows=%v err=%v", rows, err)
	}
}

func TestMetadataLimits(t *testing.T) {
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{
			block(3, 7, "a", []int64{1000}, map[string]string{"job": "one", "instance": "1"}),
			block(3, 7, "b", []int64{1000}, map[string]string{"job": "two", "instance": "2"}),
			block(3, 7, "c", []int64{1000}, map[string]string{"job": "three", "instance": "3"}),
		}, nil
	}), Limits{MaxLabelNames: 2, MaxLabelValues: 2})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	for _, requestLimit := range []int{0, 1, 2, 100} {
		want := 2
		if requestLimit == 1 {
			want = 1
		}
		names, err := a.LabelNames(context.Background(), nil, sq, requestLimit)
		if err != nil || len(names) != want {
			t.Fatalf("label names limit=%d: got %v, %v", requestLimit, names, err)
		}
		values, err := a.LabelValues(context.Background(), nil, sq, "job", requestLimit)
		if err != nil || len(values) != want {
			t.Fatalf("label values limit=%d: got %v, %v", requestLimit, values, err)
		}
	}
}
