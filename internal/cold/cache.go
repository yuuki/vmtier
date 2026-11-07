package cold

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/yuuki/vmtier/internal/querylimit"
)

// Shared reads are independently bounded even when the Reader is used without
// the RPC admission layer. No queue or unbounded pending-key map is created.
const maxSharedReads = 16

// Bookkeeping is an explicit conservative allowance, not measured heap/RSS.
// Parsed indexes are deliberately not cached; their allocations remain bounded
// per-query work, separate from this raw immutable-byte cache.
const cacheEntryOverhead int64 = 256

type cacheEntry struct {
	key  string
	data []byte
	cost int64
}

type sharedRead struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	data    []byte
	err     error
}

type immutableCache struct {
	mu        sync.Mutex
	entries   map[string]*list.Element
	lru       list.List
	used      int64
	evictions uint64
	flights   map[string]*sharedRead
}

// CacheSnapshot accounts for retained raw data, keys and a per-entry bookkeeping
// allowance. It excludes transient parsed indexes, shared-read buffers and Go
// allocator/GC overhead; it is not a process-memory or object-bucket measurement.
type CacheSnapshot struct {
	UsedBytes  int64
	LimitBytes int64
	Entries    int
	Inflight   int
	Evictions  uint64
}

func (r *Reader) CacheStats() CacheSnapshot {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	return CacheSnapshot{r.cache.used, r.CacheBytes, len(r.cache.entries), len(r.cache.flights), r.cache.evictions}
}

func cacheKey(key string, offset, length int64, sum string) string {
	return fmt.Sprintf("%s:%d:%d:%s", key, offset, length, sum)
}

func (r *Reader) cacheGet(key string) ([]byte, bool) {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	entry := r.cache.entries[key]
	if entry == nil {
		return nil, false
	}
	r.cache.lru.MoveToFront(entry)
	return entry.Value.(*cacheEntry).data, true
}

func (r *Reader) cachePut(key string, b []byte) {
	cost := int64(len(b)+len(key)) + cacheEntryOverhead
	if r.CacheBytes <= 0 || cost > r.CacheBytes {
		return
	}
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	if entry := r.cache.entries[key]; entry != nil {
		r.cache.lru.MoveToFront(entry)
		return
	}
	if r.cache.entries == nil {
		r.cache.entries = make(map[string]*list.Element)
	}
	for r.cache.used+cost > r.CacheBytes {
		oldest := r.cache.lru.Back()
		entry := oldest.Value.(*cacheEntry)
		delete(r.cache.entries, entry.key)
		r.cache.used -= entry.cost
		r.cache.lru.Remove(oldest)
		r.cache.evictions++
	}
	// Own precisely this block's bytes: a slice of a combined range must not
	// retain the entire larger response while accounting only for its length.
	owned := make([]byte, len(b))
	copy(owned, b)
	entry := &cacheEntry{key: key, data: owned, cost: cost}
	r.cache.entries[key] = r.cache.lru.PushFront(entry)
	r.cache.used += cost
}

func (r *Reader) sharedRead(ctx context.Context, key string, read func(context.Context) ([]byte, error)) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var flight *sharedRead
	for {
		r.cache.mu.Lock()
		flight = r.cache.flights[key]
		if flight != nil && flight.waiters == 0 {
			// The previous callers abandoned this read. Keep it counted while
			// cancellation unwinds, but never attach a live caller to its canceled
			// context. Retry only after it drains, still honoring our own deadline.
			r.cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-flight.done:
				continue
			}
		}
		if flight == nil {
			if len(r.cache.flights) >= maxSharedReads {
				r.cache.mu.Unlock()
				return nil, querylimit.Errorf(querylimit.SharedReads, "query shared-read limit exceeded")
			}
			if r.cache.flights == nil {
				r.cache.flights = make(map[string]*sharedRead)
			}
			// A caller timeout must not cancel another live caller's read. The
			// final departing waiter requests backend cancellation; that flight
			// stays admitted until the store cooperatively unwinds. Its temporary
			// buffers are separate from the caller's query-work reservation.
			readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			flight = &sharedRead{done: make(chan struct{}), cancel: cancel}
			r.cache.flights[key] = flight
			go func(flight *sharedRead) {
				data, err := read(readCtx)
				cancel()
				r.cache.mu.Lock()
				flight.data, flight.err = data, err
				delete(r.cache.flights, key)
				close(flight.done)
				r.cache.mu.Unlock()
			}(flight)
		}
		flight.waiters++
		r.cache.mu.Unlock()
		break
	}
	defer func() {
		r.cache.mu.Lock()
		flight.waiters--
		if flight.waiters == 0 {
			flight.cancel()
		}
		r.cache.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return flight.data, flight.err
	}
}
