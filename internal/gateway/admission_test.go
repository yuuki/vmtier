package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/querylimit"
)

func TestAdmissionRetainsIteratorUntilClose(t *testing.T) {
	var calls atomic.Int64
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		calls.Add(1)
		return []storage.MetricBlock{block(3, 7, "cpu", []int64{1000}, nil)}, nil
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	it, err := a.InitSearch(context.Background(), nil, sq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.LabelNames(context.Background(), nil, sq, 100); querylimit.Reason(err) != querylimit.MemoryAdmission {
		t.Fatalf("metadata should be refused while iterator owns memory: %v", err)
	}
	if calls.Load() != 1 || a.AdmissionStats().ReservedBytes != 100 || a.LimitRefusals()[querylimit.MemoryAdmission] != 1 {
		t.Fatal("refused request reached backend or reservation was released early")
	}
	// Consuming the final block does not drop its backing memory before MustClose.
	for {
		if _, ok := it.NextBlock(context.Background(), nil); !ok {
			break
		}
	}
	if a.AdmissionStats().ReservedBytes != 100 {
		t.Fatal("reservation released before iterator close")
	}
	it.MustClose()
	it.MustClose()
	if a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("iterator close leaked or over-released")
	}
	names, err := a.LabelNames(context.Background(), nil, sq, 100)
	if err != nil || len(names) != 1 || names[0] != "__name__" {
		t.Fatalf("metadata recovery: %v %v", names, err)
	}
	if a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("metadata leaked reservation")
	}
}

func TestAdmissionDoesNotReleaseWhileCanceledBackendUnwinds(t *testing.T) {
	canceled, unwind := make(chan struct{}), make(chan struct{})
	a := New(backendFunc(func(ctx context.Context, _ *storage.SearchQuery) ([]storage.MetricBlock, error) {
		<-ctx.Done()
		close(canceled)
		<-unwind
		return nil, ctx.Err()
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100, MaxRequestDuration: 10 * time.Millisecond})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	done := make(chan error, 1)
	go func() { _, err := a.InitSearch(context.Background(), nil, sq); done <- err }()
	<-canceled
	if _, err := a.InitSearch(context.Background(), nil, sq); querylimit.Reason(err) != querylimit.MemoryAdmission {
		t.Fatalf("memory reused while canceled backend still holds it: %v", err)
	}
	close(unwind)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	if a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("timeout leaked reservation")
	}
}

func TestAdmissionIteratorDeadlineReleasesAbandonedResults(t *testing.T) {
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{block(3, 7, "cpu", []int64{1000}, nil)}, nil
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100, MaxRequestDuration: 20 * time.Millisecond})
	it, err := a.InitSearch(context.Background(), nil, storage.NewSearchQuery(3, 7, 0, 2000, nil, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer it.MustClose()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for a.AdmissionStats().ReservedBytes != 0 {
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("abandoned iterator did not release on deadline")
		}
	}
	if _, ok := it.NextBlock(context.Background(), nil); ok || !errors.Is(it.Error(), context.DeadlineExceeded) {
		t.Fatal("expired iterator yielded successful data")
	}
}

func TestAdmissionErrorAndCancellationRelease(t *testing.T) {
	for _, cause := range []error{errors.New("missing object"), context.Canceled, querylimit.Errorf(querylimit.ReadBytes, "query read byte limit exceeded")} {
		a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) { return nil, cause }), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100})
		for i := 0; i < 2; i++ {
			if _, err := a.InitSearch(context.Background(), nil, storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)); !errors.Is(err, cause) {
				t.Fatalf("backend failure: %v", err)
			}
			if a.AdmissionStats().ReservedBytes != 0 {
				t.Fatal("failed request leaked reservation")
			}
		}
		if reason := querylimit.Reason(cause); reason != "" && a.LimitRefusals()[reason] != 2 {
			t.Fatal("wrapped backend refusal not counted")
		}
	}
}

func TestAdmissionServiceShutdownReleasesHeldIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := NewWithContext(ctx, backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{block(3, 7, "cpu", []int64{1000}, nil)}, nil
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	it, err := a.InitSearch(context.Background(), nil, sq)
	if err != nil {
		t.Fatal(err)
	}
	defer it.MustClose()
	cancel()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for a.AdmissionStats().ReservedBytes != 0 {
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("service shutdown did not release held iterator")
		}
	}
	if !errors.Is(it.Error(), context.Canceled) {
		t.Fatal("service shutdown did not cancel iterator")
	}
	if _, err := a.LabelNames(context.Background(), nil, sq, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("query admitted after shutdown: %v", err)
	}
}

func TestAdmissionConcurrencyIsSharedAcrossAPIs(t *testing.T) {
	entered, release := make(chan struct{}, 2), make(chan struct{})
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 200})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	done := make(chan error, 2)
	go func() { _, err := a.LabelNames(context.Background(), nil, sq, 100); done <- err }()
	go func() { _, err := a.LabelValues(context.Background(), nil, sq, "job", 100); done <- err }()
	<-entered
	<-entered
	if _, err := a.SearchMetricNames(context.Background(), nil, sq); querylimit.Reason(err) != querylimit.MemoryAdmission {
		t.Fatalf("shared budget not enforced: %v", err)
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("parallel metadata leaked memory")
	}
}

func TestAdmissionPrecedesMatcherWorkAndReleasesInvalidMatcher(t *testing.T) {
	a := New(backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return nil, nil
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	it, err := a.InitSearch(context.Background(), nil, sq)
	if err != nil {
		t.Fatal(err)
	}
	invalid := storage.NewSearchQuery(3, 7, 0, 2000, [][]storage.TagFilter{{{Key: []byte("job"), Value: []byte("["), IsRegexp: true}}}, 100)
	if _, err := a.InitSearch(context.Background(), nil, invalid); querylimit.Reason(err) != querylimit.MemoryAdmission {
		t.Fatalf("matcher ran before memory admission: %v", err)
	}
	it.MustClose()
	if _, err := a.InitSearch(context.Background(), nil, invalid); err == nil || querylimit.Reason(err) != "" {
		t.Fatalf("admitted invalid matcher was not rejected: %v", err)
	}
	if a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("invalid matcher leaked reservation")
	}
}

func TestAdmissionRequestCancellationRetainsMemoryUntilBackendReturns(t *testing.T) {
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	started, canceled, unwind := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(unwind) }) }
	defer release()
	a := New(backendFunc(func(ctx context.Context, _ *storage.SearchQuery) ([]storage.MetricBlock, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-unwind
		return nil, ctx.Err()
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100, MaxRequestDuration: time.Minute})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	done := make(chan error, 1)
	go func() { _, err := a.InitSearch(requestCtx, nil, sq); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("backend did not start")
	}
	cancelRequest()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("backend did not receive request cancellation")
	}
	if a.AdmissionStats().ReservedBytes != 100 {
		t.Fatal("request cancellation released memory while backend still owns results")
	}
	if _, err := a.LabelNames(context.Background(), nil, sq, 100); querylimit.Reason(err) != querylimit.MemoryAdmission {
		t.Fatalf("metadata admitted before canceled backend returned: %v", err)
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not finish after release")
	}
	if a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("request cancellation leaked reservation")
	}
}

func TestAdmissionRequestCancellationReleasesHeldIterator(t *testing.T) {
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	a := NewWithContext(serviceCtx, backendFunc(func(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error) {
		return []storage.MetricBlock{block(3, 7, "cpu", []int64{1000}, nil)}, nil
	}), Limits{QueryWorkBytes: 100, QueryMemoryBytes: 100, MaxRequestDuration: time.Minute})
	sq := storage.NewSearchQuery(3, 7, 0, 2000, nil, 100)
	it, err := a.InitSearch(requestCtx, nil, sq)
	if err != nil {
		t.Fatal(err)
	}
	defer it.MustClose()
	cancelRequest()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for a.AdmissionStats().ReservedBytes != 0 {
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("request cancellation did not release held iterator")
		}
	}
	if _, ok := it.NextBlock(context.Background(), nil); ok || !errors.Is(it.Error(), context.Canceled) {
		t.Fatal("canceled request iterator yielded successful data")
	}
	// Canceling one RPC must not cancel the service or poison later admission.
	names, err := a.LabelNames(context.Background(), nil, sq, 100)
	if err != nil || len(names) != 1 || names[0] != "__name__" {
		t.Fatalf("later request did not recover: %v %v", names, err)
	}
	if serviceCtx.Err() != nil || a.AdmissionStats().ReservedBytes != 0 {
		t.Fatal("request cancellation affected service lifetime or leaked reservation")
	}
}
