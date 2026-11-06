package cold

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

type rangeCall struct{ offset, length int64 }
type recordingRanges struct {
	objectstore.BlobStore
	calls []rangeCall
	alter func([]byte) ([]byte, error)
}

func (s *recordingRanges) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.calls = append(s.calls, rangeCall{offset, length})
	b, err := s.BlobStore.GetRange(ctx, key, offset, length)
	if err == nil && s.alter != nil {
		return s.alter(b)
	}
	return b, err
}

// Interleaved series deliberately make directory order differ from byte order.
func adjacentFixture(t *testing.T, count, rows int) (*Reader, *recordingRanges, *pack.Index, int64) {
	t.Helper()
	tenant := pack.Tenant{AccountID: 1, ProjectID: 2}
	native := encoding.MarshalInt64(nil, 0)
	native = encoding.MarshalInt64(native, 99)
	rng := rand.New(rand.NewSource(10))
	for i := 0; i < count; i++ {
		label := "keep"
		if i%3 == 1 {
			label = "skip"
		}
		name := pack.MetricName(tenant, map[string]string{"__name__": fmt.Sprintf("metric%d", i%3), "job": label}).MarshalNoAccountIDProjectID(nil)
		ts, vs := make([]int64, rows), make([]int64, rows)
		for j := range ts {
			ts[j], vs[j] = 10, int64(i+1)
			if rows > 3 {
				vs[j] = rng.Int63()
			}
		}
		var block storage.Block
		block.Init(&storage.TSID{}, ts, vs, 0, 64)
		payload := block.MarshalPortable(nil)
		native = encoding.MarshalUint32(native, uint32(len(name)))
		native = append(native, name...)
		native = encoding.MarshalUint32(native, uint32(len(payload)))
		native = append(native, payload...)
	}
	idx, data, err := pack.Build(bytes.NewReader(native), tenant, 0, 100, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := pack.MarshalIndex(idx, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := objectstore.NewLocal(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	prefix := "tenants/1:2/attempts/0123456789abcdef0123456789abcdef/"
	entry := catalog.Entry{Tenant: "1:2", Start: 0, End: 100, SourceID: "hot", SourceFingerprint: fp,
		DataKey: prefix + "data.pack", IndexKey: prefix + "index.json", IndexSHA256: pack.Sum(index)}
	if err := blobs.Put(ctx, entry.DataKey, data); err != nil {
		t.Fatal(err)
	}
	if err := blobs.Put(ctx, entry.IndexKey, index); err != nil {
		t.Fatal(err)
	}
	authority := newAuthority(t)
	m, gen, err := catalog.Load(ctx, authority)
	if err != nil {
		t.Fatal(err)
	}
	age := 100 * 365 * 24 * time.Hour
	m.Contract = catalog.Contract{CoverageStart: 0, WindowMS: 100, DedupMS: 1, AgeMS: age.Milliseconds(), Tenants: []string{"1:2"}, Sources: []catalog.Source{{ID: "hot", Fingerprint: fp}}}
	m.Entries = []catalog.Entry{entry}
	if _, err := catalog.Commit(ctx, authority, m, gen); err != nil {
		t.Fatal(err)
	}
	store := &recordingRanges{BlobStore: blobs}
	reader := &Reader{Store: store, CatalogStore: authority, Tenants: map[string]bool{"1:2": true}, Sources: []string{"hot"}, SourceFingerprints: map[string]string{"hot": fp}, DedupMS: 1, Age: age, Window: 100 * time.Millisecond, MaxQueryBytes: 128 << 20}
	return reader, store, idx, int64(len(index))
}

func TestAdjacentRangesSamplesAndMetadata(t *testing.T) {
	r, store, idx, _ := adjacentFixture(t, 4, 3)
	r.CacheBytes = 1 << 20
	got, err := r.Search(ctx, query())
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, got, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4)
	if !reflect.DeepEqual(store.calls, []rangeCall{{0, idx.Size}}) {
		t.Fatalf("four adjacent blocks should use one GET: %v", store.calls)
	}
	api := gateway.New(r, gateway.Limits{})
	names, err := api.LabelNames(nil, query(), 100, 0)
	if err != nil || !reflect.DeepEqual(names, []string{"__name__", "job"}) {
		t.Fatalf("label names: %v, %v", names, err)
	}
	labels, err := api.LabelValues(nil, query(), "job", 100, 0)
	if err != nil || !reflect.DeepEqual(labels, []string{"keep", "skip"}) {
		t.Fatalf("label values: %v, %v", labels, err)
	}
	series, err := api.SearchMetricNames(nil, query(), 0)
	var wantSeries []string
	for _, series := range idx.Series {
		wantSeries = append(wantSeries, string(pack.MetricName(idx.Tenant, series.Labels).Marshal(nil)))
	}
	sort.Strings(wantSeries)
	if err != nil || !reflect.DeepEqual(series, wantSeries) {
		t.Fatalf("series discovery: %v, %v; want %v", series, err, wantSeries)
	}
	got, err = r.Search(ctx, query())
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, got, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4)
	if len(store.calls) != 1 {
		t.Fatalf("warm sample/metadata reads downloaded ranges: %v", store.calls)
	}
	// Metadata still requires an actual sample in the requested time interval.
	q := storage.NewSearchQuery(1, 2, 11, 99, nil, 100)
	names, err = api.LabelNames(nil, q, 100, 0)
	if err != nil || len(names) != 0 {
		t.Fatalf("metadata ignored sample time: %v %v", names, err)
	}
}

func TestAdjacentRangesDoNotOverfetchGaps(t *testing.T) {
	r, store, _, _ := adjacentFixture(t, 4, 3)
	q := query()
	q.TagFilterss = [][]storage.TagFilter{{{Key: []byte("job"), Value: []byte("keep")}}}
	got, err := r.Search(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	assertValues(t, got, 1, 1, 1, 3, 3, 3, 4, 4, 4)
	if len(store.calls) != 2 || store.calls[0].offset+store.calls[0].length >= store.calls[1].offset {
		t.Fatalf("unselected gap was downloaded: %v", store.calls)
	}
}

func TestAdjacentRangesRejectInvalidResponsesAndCancellation(t *testing.T) {
	for _, mode := range []string{"truncated", "oversized", "corrupt-last", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			r, store, _, _ := adjacentFixture(t, 4, 3)
			c, cancel := context.WithCancel(ctx)
			defer cancel()
			store.alter = func(b []byte) ([]byte, error) {
				switch mode {
				case "truncated":
					return b[:len(b)-1], nil
				case "oversized":
					return append(b, 0), nil
				case "corrupt-last":
					b[len(b)-1] ^= 1
				case "error":
					return nil, errors.New("backend unavailable")
				case "cancel":
					cancel()
				}
				return b, nil
			}
			got, err := r.Search(c, query())
			if err == nil || got != nil {
				t.Fatalf("failed combined response returned partial success: %v %v", got, err)
			}
			if len(store.calls) != 1 {
				t.Fatalf("failure did not exercise combined read: %v", store.calls)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
		})
	}
}

func TestAdjacentRangesChargeAllBytesBeforeRead(t *testing.T) {
	r, store, idx, indexBytes := adjacentFixture(t, 4, 3)
	cost := indexBytes + idx.Size + 4*3*16
	r.MaxQueryBytes = cost - 1
	if got, err := r.Search(ctx, query()); err == nil || got != nil || len(store.calls) != 0 {
		t.Fatalf("over-budget combined range was read: %v %v %v", got, err, store.calls)
	}
	r.MaxQueryBytes = cost
	if _, err := r.Search(ctx, query()); err != nil {
		t.Fatalf("exact budget rejected: %v", err)
	}
}

func TestAdjacentRangeReadSizeBound(t *testing.T) {
	r, store, idx, _ := adjacentFixture(t, 80, 8192)
	if idx.Size <= maxCombinedRangeBytes {
		t.Fatalf("fixture must exceed combined range cap: %d", idx.Size)
	}
	got, err := r.Search(ctx, query())
	if err != nil || len(got) != 80 {
		t.Fatalf("bounded read failed: %d %v", len(got), err)
	}
	if len(store.calls) < 2 || len(store.calls) >= 80 {
		t.Fatalf("unexpected GET count: %d", len(store.calls))
	}
	var end int64
	for _, call := range store.calls {
		if call.length > maxCombinedRangeBytes || call.offset != end {
			t.Fatalf("oversized or gapped GET: %v", call)
		}
		end += call.length
	}
	if end != idx.Size {
		t.Fatalf("incomplete reads: %d != %d", end, idx.Size)
	}
}
