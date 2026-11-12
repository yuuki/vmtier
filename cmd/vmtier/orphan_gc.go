package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
)

// orphanGCStats counts garbage collection passes of this process.
type orphanGCStats struct {
	enabled                                    atomic.Bool
	passes, failures, claimed, delete, retired atomic.Uint64
	lastSuccess                                atomic.Int64
}

func (s *orphanGCStats) write(w io.Writer) {
	enabled := 0
	if s.enabled.Load() {
		enabled = 1
	}
	fmt.Fprintf(w, "vmtier_orphan_gc_enabled %d\nvmtier_orphan_gc_passes_total %d\nvmtier_orphan_gc_failed_passes_total %d\nvmtier_orphan_gc_claimed_objects_total %d\nvmtier_orphan_gc_deleted_objects_total %d\nvmtier_orphan_gc_last_success_timestamp_seconds %d\n",
		enabled, s.passes.Load(), s.failures.Load(), s.claimed.Load(), s.delete.Load(), s.lastSuccess.Load())
	fmt.Fprintf(w, "vmtier_cold_retired_windows_total %d\n", s.retired.Load())
}

// orphanPassRunner runs one garbage collection pass.
type orphanPassRunner interface {
	RunPass(context.Context) (archive.CollectResult, error)
}

// collectingWorker runs garbage collection between archive passes, at most
// once per interval. It runs in the archive worker's goroutine, so with
// PostgreSQL only the archiver lease holder collects, and a lost lease stops
// it. Safety does not depend on that: publication and claims are serialized
// by the catalog.
type collectingWorker struct {
	archiver  archivePassRunner
	collector orphanPassRunner
	interval  time.Duration
	next      time.Time
	stats     *orphanGCStats
}

func (w *collectingWorker) RunPass(ctx context.Context, now time.Time) (*archive.Result, error) {
	result, err := w.archiver.RunPass(ctx, now)
	if ctx.Err() != nil || errors.Is(err, catalog.ErrLeaseLost) || time.Now().Before(w.next) {
		return result, err
	}
	collected, gcErr := w.collector.RunPass(ctx)
	w.next = time.Now().Add(w.interval)
	w.stats.passes.Add(1)
	w.stats.retired.Add(uint64(collected.Retired))
	w.stats.claimed.Add(uint64(collected.Claimed))
	w.stats.delete.Add(uint64(collected.Deleted))
	if gcErr != nil {
		if ctx.Err() == nil {
			w.stats.failures.Add(1)
			slog.Error("orphan garbage collection failed", "retired", collected.Retired, "claimed", collected.Claimed, "deleted", collected.Deleted, "error", gcErr)
		}
	} else {
		w.stats.lastSuccess.Store(time.Now().Unix())
		if collected.Deleted > 0 || collected.Retired > 0 {
			slog.Info("orphan garbage collection deleted objects", "retired", collected.Retired, "claimed", collected.Claimed, "deleted", collected.Deleted)
		}
	}
	return result, err
}
