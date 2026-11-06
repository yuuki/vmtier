package objectstore

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func testBudget(t *testing.T, rate float64, concurrent, queued int) *requestBudget {
	t.Helper()
	b, err := newRequestBudget(rate, concurrent, queued)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func waitBudgetQueue(t *testing.T, b *requestBudget, want int) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for b.snapshot().QueuedRequests != want {
		select {
		case <-deadline.C:
			t.Fatalf("queue did not reach %d: %+v", want, b.snapshot())
		case <-tick.C:
		}
	}
}

func TestS3BudgetConfiguration(t *testing.T) {
	if b, err := newRequestBudget(0, 0, 0); err != nil || b != nil {
		t.Fatalf("zero budget should preserve legacy behavior: %v %v", b, err)
	}
	for _, c := range []S3Config{
		{RequestsPerSecond: 80},
		{MaxConcurrentRequests: 8, MaxQueuedRequests: 64},
		{RequestsPerSecond: math.NaN(), MaxConcurrentRequests: 1, MaxQueuedRequests: 1},
		{RequestsPerSecond: math.Inf(1), MaxConcurrentRequests: 1, MaxQueuedRequests: 1},
		{RequestsPerSecond: -1, MaxConcurrentRequests: 1, MaxQueuedRequests: 1},
		{RequestsPerSecond: 1e-30, MaxConcurrentRequests: 1, MaxQueuedRequests: 1},
		{RequestsPerSecond: 10, MaxConcurrentRequests: -1, MaxQueuedRequests: 1},
		{RequestsPerSecond: 10, MaxConcurrentRequests: 1, MaxQueuedRequests: -1},
	} {
		if _, err := NewS3(c); err == nil || (!strings.Contains(err.Error(), "S3 budget") && !strings.Contains(err.Error(), "S3 request rate")) {
			t.Fatalf("budget was not rejected before credentials: %+v %v", c, err)
		}
	}
	if b, err := newRequestBudget(.5, 1, 1); err != nil || b.interval != 2*time.Second {
		t.Fatalf("fractional pacing: %+v %v", b, err)
	}
}

func TestRequestBudgetFIFOAndPacing(t *testing.T) {
	b := testBudget(t, 20, 1, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := b.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	started := time.Now()
	type admission struct {
		id      int
		release func()
		err     error
	}
	admitted := make(chan admission, 3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			release, err := b.acquire(ctx)
			admitted <- admission{id, release, err}
		}(i)
		waitBudgetQueue(t, b, i+1)
	}
	first()
	for i := 0; i < 3; i++ {
		select {
		case got := <-admitted:
			if got.err != nil || got.id != i {
				if got.release != nil {
					got.release()
				}
				t.Fatalf("FIFO admission %d: %+v", i, got)
			}
			got.release()
		case <-ctx.Done():
			t.Fatal("admission did not finish")
		}
	}
	// Three admissions after the first require three 50ms intervals. The lower
	// bound leaves timing margin without accepting an accumulated burst.
	if elapsed := time.Since(started); elapsed < 130*time.Millisecond {
		t.Fatalf("pacing allowed a burst: %v", elapsed)
	}
	stats := b.snapshot()
	if stats.Attempts != 4 || stats.WaitedRequests != 3 || stats.InFlightRequests != 0 || stats.QueuedRequests != 0 || stats.WaitNanoseconds == 0 {
		t.Fatalf("unexpected budget counters: %+v", stats)
	}
}

func TestRequestBudgetCancellationAndQueueOverflow(t *testing.T) {
	b := testBudget(t, 1000, 1, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := b.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	head, cancelHead := context.WithCancel(ctx)
	defer cancelHead()
	failed := make(chan error, 1)
	go func() {
		release, err := b.acquire(head)
		if release != nil {
			release()
		}
		failed <- err
	}()
	waitBudgetQueue(t, b, 1)
	second := make(chan func(), 1)
	go func() {
		release, _ := b.acquire(ctx)
		second <- release
	}()
	waitBudgetQueue(t, b, 2)
	if release, err := b.acquire(ctx); !errors.Is(err, ErrRequestQueueFull) || release != nil {
		t.Fatalf("full queue accepted work: %v", err)
	}
	cancelHead()
	if err := <-failed; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled head: %v", err)
	}
	waitBudgetQueue(t, b, 1)
	first()
	select {
	case release := <-second:
		if release == nil {
			t.Fatal("next waiter did not recover after cancellation")
		}
		release()
		release() // Body closure/release must be idempotent.
	case <-ctx.Done():
		t.Fatal("queued work did not resume")
	}
	stats := b.snapshot()
	if stats.Attempts != 2 || stats.CanceledRequests != 1 || stats.RejectedRequests != 1 || stats.InFlightRequests != 0 || stats.QueuedRequests != 0 {
		t.Fatalf("leaked permits or queue entries: %+v", stats)
	}
}

func TestBudgetTransportHoldsPermitUntilBodyClose(t *testing.T) {
	b := testBudget(t, 1000, 1, 2)
	var calls atomic.Int32
	transport := budgetTransport{budget: b, base: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("payload")), Header: make(http.Header)}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://unused.invalid", nil)
	first, err := transport.RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	if _, err := io.ReadAll(first.Body); err != nil {
		t.Fatal(err)
	}
	if b.snapshot().InFlightRequests != 1 {
		t.Fatal("permit released at EOF before body Close")
	}
	done := make(chan error, 1)
	go func() {
		resp, err := transport.RoundTrip(r.Clone(ctx))
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	waitBudgetQueue(t, b, 1)
	if calls.Load() != 1 {
		t.Fatal("another request started while body remained open")
	}
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || b.snapshot().InFlightRequests != 0 {
		t.Fatalf("permit did not release after Close: calls=%d stats=%+v", calls.Load(), b.snapshot())
	}
	transport.base = roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection failed") })
	if _, err := transport.RoundTrip(r); err == nil || b.snapshot().InFlightRequests != 0 {
		t.Fatalf("transport failure leaked permit: %v %+v", err, b.snapshot())
	}
}

type trackedRequestBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackedRequestBody) Close() error { b.closed.Store(true); return nil }

func TestBudgetTransportClosesRejectedRequestBody(t *testing.T) {
	b := testBudget(t, 1000, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := b.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	done := make(chan struct{})
	go func() {
		release, _ := b.acquire(ctx)
		if release != nil {
			release()
		}
		close(done)
	}()
	waitBudgetQueue(t, b, 1)
	body := &trackedRequestBody{Reader: strings.NewReader("upload")}
	r, _ := http.NewRequestWithContext(ctx, http.MethodPut, "http://unused.invalid", body)
	transport := budgetTransport{budget: b, base: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("full queue reached the transport")
		return nil, errors.New("unexpected request")
	})}
	if _, err := transport.RoundTrip(r); !errors.Is(err, ErrRequestQueueFull) || !body.closed.Load() {
		t.Fatalf("rejected request body not closed: %v", err)
	}
	cancel()
	<-done
}

func TestS3BudgetCoversSDKRetriesAndMixedOperations(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-only")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-only-secret")
	backend := &basicS3{objects: map[string][]byte{"/test/existing": []byte("payload")}}
	backend.putFailures.Store(2)
	var active, peak atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		backend.ServeHTTP(w, r)
	}))
	defer server.Close()
	s, err := NewS3(S3Config{Endpoint: server.URL, Bucket: "test", MaxAttempts: 3, Timeout: 5 * time.Second,
		RequestsPerSecond: 20, MaxConcurrentRequests: 1, MaxQueuedRequests: 8})
	if err != nil {
		t.Fatal(err)
	}
	// Remove SDK backoff for this fixture: pacing must come from the transport
	// and must still cover retries, including their error response bodies.
	opts := s.client.Options()
	opts.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 3
		o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
	})
	s.client = s3.New(opts)
	started := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, operation := range []func() error{
		func() error { return s.Put(context.Background(), "new", []byte("uploaded")) },
		func() error {
			_, err := s.Get(context.Background(), "existing")
			return err
		},
		func() error {
			_, err := s.GetRange(context.Background(), "existing", 1, 3)
			return err
		},
	} {
		wg.Add(1)
		go func(operation func() error) { defer wg.Done(); errs <- operation() }(operation)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	stats, budget := s.Stats(), s.BudgetStats()
	if stats.Puts != 3 || stats.Gets != 1 || stats.RangeGets != 1 || budget.Attempts != 5 || budget.InFlightRequests != 0 || budget.QueuedRequests != 0 {
		t.Fatalf("SDK attempts were not all budgeted: I/O=%+v budget=%+v", stats, budget)
	}
	if peak.Load() != 1 || time.Since(started) < 180*time.Millisecond || budget.WaitedRequests < 4 || budget.WaitNanoseconds == 0 {
		t.Fatalf("shared pacing/concurrency failed: peak=%d elapsed=%v budget=%+v", peak.Load(), time.Since(started), budget)
	}
}
