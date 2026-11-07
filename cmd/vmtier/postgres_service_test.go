package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
)

type drainingArchiveWorker struct {
	started chan struct{}
	drained atomic.Bool
}

func (w *drainingArchiveWorker) RunPass(ctx context.Context, _ time.Time) (*archive.Result, error) {
	close(w.started)
	<-ctx.Done()
	w.drained.Store(true)
	return nil, ctx.Err()
}

type testLeaseCoordinator struct {
	worker             *drainingArchiveWorker
	renewals, releases atomic.Int64
	releasedEarly      atomic.Bool
	renewError         error
}

func (l *testLeaseCoordinator) AcquireLease(context.Context, string, time.Duration) (catalog.Lease, error) {
	return catalog.Lease{}, catalog.ErrLeaseHeld
}

func (l *testLeaseCoordinator) RenewLease(_ context.Context, lease catalog.Lease, _ time.Duration) (catalog.Lease, error) {
	l.renewals.Add(1)
	return lease, l.renewError
}

func (l *testLeaseCoordinator) ReleaseLease(context.Context, catalog.Lease) error {
	if !l.worker.drained.Load() {
		l.releasedEarly.Store(true)
	}
	l.releases.Add(1)
	return nil
}

func (l *testLeaseCoordinator) Fenced(catalog.Lease) catalog.Repository { return nil }

func TestArchiveLeaseFailureAndShutdownDrainBeforeRelease(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "ambiguous renewal"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			worker := &drainingArchiveWorker{started: make(chan struct{})}
			leases := &testLeaseCoordinator{worker: worker, renewError: errors.New("renewal response lost")}
			var stats serviceStats
			done := make(chan struct{})
			ttl := 30 * time.Millisecond
			if shutdown {
				ttl = time.Hour
			}
			go func() {
				defer close(done)
				runArchiveLeadership(ctx, leases, func(catalog.Repository) archivePassRunner { return worker }, catalog.Lease{Owner: "test", Token: 1}, time.Hour, ttl, &stats)
			}()
			select {
			case <-worker.started:
			case <-time.After(time.Second):
				t.Fatal("archive work did not start")
			}
			if shutdown {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("lease loss did not cancel archive work")
			}
			if !worker.drained.Load() || leases.releasedEarly.Load() || leases.releases.Load() != 1 || stats.leader.Load() {
				t.Fatal("ownership released before drain or leadership retained")
			}
			if !shutdown && leases.renewals.Load() != 1 {
				t.Fatal("ambiguous renewal did not end this leadership session")
			}
		})
	}
}
