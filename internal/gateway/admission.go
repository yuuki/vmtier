package gateway

import (
	"context"
	"github.com/yuuki/vmtier/internal/querylimit"
	"sync"
	"sync/atomic"
)

// queryMemory limits admitted work, not Go heap or resident process memory.
// Reservations remain held while a backend is unwinding a canceled operation.
// There is no admission queue: excess work fails before any backend allocation.
type queryMemory struct {
	limit    int64
	used     atomic.Int64
	rejected atomic.Uint64
}

type queryLease struct {
	ctx    context.Context
	cancel context.CancelFunc
	memory *queryMemory
	bytes  int64
	once   sync.Once
}

func (m *queryMemory) reserve(ctx context.Context, cancel context.CancelFunc, bytes int64) (*queryLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for {
		used := m.used.Load()
		if bytes <= 0 || bytes > m.limit-used {
			m.rejected.Add(1)
			return nil, querylimit.Errorf(querylimit.MemoryAdmission, "Cold query memory admission limit exceeded: %d bytes reserved, %d requested, %d limit", used, bytes, m.limit)
		}
		if m.used.CompareAndSwap(used, used+bytes) {
			return &queryLease{ctx: ctx, cancel: cancel, memory: m, bytes: bytes}, nil
		}
	}
}

func (q *queryLease) close() {
	q.once.Do(func() {
		q.cancel()
		q.memory.used.Add(-q.bytes)
	})
}

type AdmissionSnapshot struct {
	LimitBytes    int64
	WorkBytes     int64
	ReservedBytes int64
	Refusals      uint64
}

func (a *API) AdmissionStats() AdmissionSnapshot {
	return AdmissionSnapshot{a.memory.limit, a.limits.QueryWorkBytes, a.memory.used.Load(), a.memory.rejected.Load()}
}

// LimitRefusals returns counters with a finite reason vocabulary, never query text.
func (a *API) LimitRefusals() map[string]uint64 {
	result := make(map[string]uint64)
	for _, reason := range querylimit.Reasons() {
		if counter, ok := a.refusals.Load(reason); ok {
			result[reason] = counter.(*atomic.Uint64).Load()
		} else {
			result[reason] = 0
		}
	}
	return result
}
