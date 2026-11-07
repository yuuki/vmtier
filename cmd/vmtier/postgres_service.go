package main

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
)

type archiveLeaseCoordinator interface {
	AcquireLease(context.Context, string, time.Duration) (catalog.Lease, error)
	RenewLease(context.Context, catalog.Lease, time.Duration) (catalog.Lease, error)
	ReleaseLease(context.Context, catalog.Lease) error
	Fenced(catalog.Lease) catalog.Repository
}

// Readers remain independent of this loop. Every acquisition starts a new
// archiver, whose publication repository validates the token in the database.
func runPostgresArchiveLoop(ctx context.Context, leases archiveLeaseCoordinator, worker func(catalog.Repository) archivePassRunner, interval, ttl time.Duration, stats *serviceStats) {
	owner := "vmtier-" + rand.Text()
	renewInterval := ttl / 3
	for ctx.Err() == nil {
		acquireCtx, cancel := context.WithTimeout(ctx, renewInterval)
		lease, err := leases.AcquireLease(acquireCtx, owner, ttl)
		cancel()
		if err == nil {
			runArchiveLeadership(ctx, leases, worker, lease, interval, ttl, stats)
		} else if ctx.Err() == nil && !errors.Is(err, catalog.ErrLeaseHeld) {
			slog.Error("acquire archive lease failed", "error", err)
		}
		if !waitArchiveRetry(ctx, renewInterval) {
			return
		}
	}
}

func runArchiveLeadership(ctx context.Context, leases archiveLeaseCoordinator, worker func(catalog.Repository) archivePassRunner, lease catalog.Lease, interval, ttl time.Duration, stats *serviceStats) {
	archiveCtx, cancelArchive := context.WithCancel(ctx)
	done := make(chan struct{})
	archiveWorker := worker(leases.Fenced(lease))
	stats.leader.Store(true)
	slog.Info("archive lease acquired", "owner", lease.Owner, "token", lease.Token)
	go func() {
		defer close(done)
		runArchiveLoop(archiveCtx, archiveWorker, interval, stats)
	}()
	renewInterval := ttl / 3
	ticker := time.NewTicker(renewInterval)
leadership:
	for {
		select {
		case <-ctx.Done():
			break leadership
		case <-done:
			break leadership
		case <-ticker.C:
			renewCtx, stop := context.WithTimeout(ctx, renewInterval)
			renewed, err := leases.RenewLease(renewCtx, lease, ttl)
			stop()
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("archive lease renewal failed; draining archive work", "error", err)
				}
				break leadership
			}
			lease = renewed
		}
	}
	ticker.Stop()
	stats.leader.Store(false)
	cancelArchive()
	// Cancellation alone does not establish that no work remains. Keep ownership
	// until the worker drains, even after an ambiguous renewal response.
	<-done
	releaseCtx, stop := context.WithTimeout(context.Background(), min(renewInterval, 5*time.Second))
	defer stop()
	if err := leases.ReleaseLease(releaseCtx, lease); err != nil && !errors.Is(err, catalog.ErrLeaseLost) {
		slog.Error("release archive lease failed; ownership will expire", "error", err)
	}
}

func waitArchiveRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
