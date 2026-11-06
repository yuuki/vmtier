package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"
)

var ErrRequestQueueFull = errors.New("S3 request queue is full")

// BudgetSnapshot counts HTTP attempts, including SDK retries, rather than logical
// BlobStore calls. Waiting and cancellation are accounted for before transmission.
type BudgetSnapshot struct {
	Attempts         uint64 `json:"attempts"`
	WaitedRequests   uint64 `json:"waited_requests"`
	WaitNanoseconds  uint64 `json:"wait_nanoseconds"`
	RejectedRequests uint64 `json:"rejected_requests"`
	CanceledRequests uint64 `json:"canceled_requests"`
	InFlightRequests int    `json:"in_flight_requests"`
	QueuedRequests   int    `json:"queued_requests"`
}

// requestBudget paces one shared S3 client's requests without accumulating burst
// credit. Waiters form a bounded FIFO. Only the queue head needs a timer; releases
// and cancellations wake the queue without a permanent background goroutine.
type requestBudget struct {
	mu          sync.Mutex
	interval    time.Duration
	maxInFlight int
	maxQueued   int
	next        time.Time
	queue       []*budgetWaiter
	changed     chan struct{}
	stats       BudgetSnapshot
}

type budgetWaiter struct{ queuedAt time.Time }

func newRequestBudget(rate float64, concurrent, queued int) (*requestBudget, error) {
	if rate == 0 && concurrent == 0 && queued == 0 {
		return nil, nil
	}
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 || rate > 1000000 || concurrent < 1 || concurrent > 10000 || queued < 1 || queued > 100000 {
		return nil, fmt.Errorf("S3 budget requires a finite positive rate <= 1000000, concurrency in [1,10000], and queue in [1,100000]")
	}
	spacing := math.Ceil(float64(time.Second) / rate)
	if spacing >= float64(math.MaxInt64) {
		return nil, errors.New("S3 request rate is too small")
	}
	return &requestBudget{interval: time.Duration(spacing), maxInFlight: concurrent, maxQueued: queued, changed: make(chan struct{})}, nil
}

func (b *requestBudget) snapshot() BudgetSnapshot {
	if b == nil {
		return BudgetSnapshot{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

func (b *requestBudget) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *requestBudget) admitLocked(now time.Time) func() {
	b.stats.Attempts++
	b.stats.InFlightRequests++
	b.next = now.Add(b.interval)
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.stats.InFlightRequests--
			b.notifyLocked()
			b.mu.Unlock()
		})
	}
}

func (b *requestBudget) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	now := time.Now()
	if len(b.queue) == 0 && b.stats.InFlightRequests < b.maxInFlight && !now.Before(b.next) {
		release := b.admitLocked(now)
		b.mu.Unlock()
		return release, nil
	}
	if len(b.queue) >= b.maxQueued {
		b.stats.RejectedRequests++
		b.mu.Unlock()
		return nil, ErrRequestQueueFull
	}
	waiter := &budgetWaiter{queuedAt: now}
	b.queue = append(b.queue, waiter)
	b.stats.WaitedRequests++
	b.stats.QueuedRequests++
	for {
		if err := ctx.Err(); err != nil {
			for i, item := range b.queue {
				if item == waiter {
					copy(b.queue[i:], b.queue[i+1:])
					b.queue[len(b.queue)-1] = nil
					b.queue = b.queue[:len(b.queue)-1]
					break
				}
			}
			b.stats.QueuedRequests--
			b.stats.CanceledRequests++
			b.stats.WaitNanoseconds += uint64(time.Since(waiter.queuedAt))
			b.notifyLocked()
			b.mu.Unlock()
			return nil, err
		}
		var wait time.Duration
		if b.queue[0] == waiter && b.stats.InFlightRequests < b.maxInFlight {
			now = time.Now()
			if !now.Before(b.next) {
				b.queue[0] = nil
				b.queue = b.queue[1:]
				b.stats.QueuedRequests--
				b.stats.WaitNanoseconds += uint64(now.Sub(waiter.queuedAt))
				release := b.admitLocked(now)
				b.notifyLocked()
				b.mu.Unlock()
				return release, nil
			}
			wait = b.next.Sub(now)
		}
		changed := b.changed
		b.mu.Unlock()
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
			case <-changed:
			case <-timer.C:
			}
			timer.Stop()
		} else {
			select {
			case <-ctx.Done():
			case <-changed:
			}
		}
		b.mu.Lock()
	}
}

type budgetTransport struct {
	base   http.RoundTripper
	budget *requestBudget
}

func (t budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	release, err := t.budget.acquire(r.Context())
	if err != nil {
		// RoundTripper owns request-body closure even when admission fails.
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, err
	}
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		release()
		return resp, err
	}
	if resp == nil || resp.Body == nil {
		release()
		return resp, nil
	}
	resp.Body = &budgetBody{ReadCloser: resp.Body, release: release}
	return resp, nil
}

// A permit covers the complete response-body lifetime, including slow readers
// and error responses. EOF alone does not release it: callers must close bodies.
type budgetBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
	err     error
}

func (b *budgetBody) Close() error {
	b.once.Do(func() {
		b.err = b.ReadCloser.Close()
		b.release()
	})
	return b.err
}
