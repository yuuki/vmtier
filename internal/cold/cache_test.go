package cold

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

func TestLRUPreservesRecentlyUsedEntriesUnderPressure(t *testing.T) {
	store, err := objectstore.NewLocal(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	payload := []byte("immutable data")
	sum := pack.Sum(payload)
	cost := int64(len(payload)+len(cacheKey("a", 0, -1, sum))) + cacheEntryOverhead
	r := &Reader{Store: store, CacheBytes: cost * 2}
	for _, key := range []string{"a", "b", "c"} {
		if err := store.Put(ctx, key, payload); err != nil {
			t.Fatal(err)
		}
	}
	read := func(key string) {
		t.Helper()
		got, err := r.cached(ctx, key, 0, -1, sum)
		if err != nil || !reflect.DeepEqual(got, payload) {
			t.Fatalf("read %s: %q %v", key, got, err)
		}
	}
	read("a")
	read("b")
	read("a") // b becomes the least recently used entry.
	read("c") // Evict b alone; a must survive.
	read("a")
	if got := store.Stats().Gets; got != 3 {
		t.Fatalf("recent entry evicted under pressure: %d GETs", got)
	}
	stats := r.CacheStats()
	if stats.UsedBytes != cost*2 || stats.Entries != 2 || stats.UsedBytes > stats.LimitBytes || stats.Evictions != 1 {
		t.Fatalf("cache accounting: %+v", stats)
	}
	read("b")
	if got := store.Stats().Gets; got != 4 {
		t.Fatalf("least recently used entry was not evicted: %d GETs", got)
	}
	t.Logf("LRU fixture: 6 logical reads, 4 GETs; retained accounting=%d bytes (raw payload + keys + bookkeeping)", stats.UsedBytes)
}

type blockingReads struct {
	objectstore.BlobStore
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
	cancel  atomic.Int64
	fail    atomic.Bool
}

func (s *blockingReads) wait(ctx context.Context) error {
	s.calls.Add(1)
	s.entered <- struct{}{}
	select {
	case <-ctx.Done():
		s.cancel.Add(1)
		return ctx.Err()
	case <-s.release:
		if s.fail.Load() {
			return errors.New("injected backend failure")
		}
		return nil
	}
}
func (s *blockingReads) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.BlobStore.Get(ctx, key)
}
func (s *blockingReads) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.BlobStore.GetRange(ctx, key, offset, length)
}

func awaitCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for test synchronization")
		case <-tick.C:
		}
	}
}
func awaitWaiters(t *testing.T, r *Reader, want int) {
	t.Helper()
	awaitCondition(t, func() bool {
		r.cache.mu.Lock()
		defer r.cache.mu.Unlock()
		for _, f := range r.cache.flights {
			if f.waiters == want {
				return true
			}
		}
		return false
	})
}

func TestConcurrentMissesShareVerifiedIndexAndAdjacentRange(t *testing.T) {
	r, original, _, _ := adjacentFixture(t, 4, 3)
	r.CacheBytes = 1 << 20
	blocked := &blockingReads{BlobStore: original, entered: make(chan struct{}, 4), release: make(chan struct{})}
	r.Store = blocked
	const queries = 8
	results := make(chan error, queries)
	for i := 0; i < queries; i++ {
		go func() {
			got, err := r.Search(ctx, query())
			if err == nil && len(got) != 4 {
				err = fmt.Errorf("expected 4 blocks, got %d", len(got))
			}
			results <- err
		}()
	}
	<-blocked.entered
	awaitWaiters(t, r, queries)
	// Release the index read only; block followers together on the range read.
	blocked.release <- struct{}{}
	<-blocked.entered
	awaitWaiters(t, r, queries)
	close(blocked.release)
	for i := 0; i < queries; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if blocked.calls.Load() != 2 || len(original.calls) != 1 {
		t.Fatalf("concurrent miss duplication: calls=%d ranges=%v", blocked.calls.Load(), original.calls)
	}
	coldBytes := original.BlobStore.(*objectstore.Local).Stats().DownloadedBytes
	if _, err := r.Search(ctx, query()); err != nil || blocked.calls.Load() != 2 {
		t.Fatalf("warm query changed reads/results: %v, %d", err, blocked.calls.Load())
	}
	if got := original.BlobStore.(*objectstore.Local).Stats().DownloadedBytes; got != coldBytes {
		t.Fatalf("warm query downloaded bytes: %d -> %d", coldBytes, got)
	}
	stats := r.CacheStats()
	if stats.Inflight != 0 || stats.UsedBytes > stats.LimitBytes {
		t.Fatalf("shared reads or cache leaked: %+v", stats)
	}
	t.Logf("8 concurrent cold queries: 1 index GET + 1 Range GET, %d downloaded bytes; warm query: 0 GETs/bytes; %+v", coldBytes, stats)
}

func TestSharedReadCancellationAndFailureDoNotPoisonRetry(t *testing.T) {
	for _, mode := range []string{"leader-cancel", "all-cancel", "error"} {
		t.Run(mode, func(t *testing.T) {
			f := fixture(t)
			r := f.reader
			blocked := &blockingReads{BlobStore: f.blobs, entered: make(chan struct{}, 4), release: make(chan struct{})}
			r.Store = blocked
			e := f.entries[0]
			c1, cancel1 := context.WithCancel(ctx)
			defer cancel1()
			c2, cancel2 := context.WithCancel(ctx)
			defer cancel2()
			first, second := make(chan error, 1), make(chan error, 1)
			read := func(c context.Context, out chan error) {
				_, err := r.cached(c, e.IndexKey, 0, -1, e.IndexSHA256)
				out <- err
			}
			go read(c1, first)
			<-blocked.entered
			go read(c2, second)
			awaitWaiters(t, r, 2)
			if mode == "error" {
				blocked.fail.Store(true)
				blocked.release <- struct{}{}
				if <-first == nil || <-second == nil {
					t.Fatal("backend failure became successful shared read")
				}
			} else {
				cancel1()
				if err := <-first; !errors.Is(err, context.Canceled) {
					t.Fatalf("leader cancellation lost: %v", err)
				}
				if mode == "all-cancel" {
					cancel2()
					if err := <-second; !errors.Is(err, context.Canceled) {
						t.Fatalf("follower cancellation lost: %v", err)
					}
					awaitCondition(t, func() bool { return blocked.cancel.Load() == 1 })
				} else {
					close(blocked.release)
					if err := <-second; err != nil {
						t.Fatalf("leader canceled a live follower: %v", err)
					}
				}
			}
			awaitCondition(t, func() bool { return r.CacheStats().Inflight == 0 })
			if mode != "leader-cancel" {
				if r.CacheStats().Entries != 0 {
					t.Fatal("failed read entered cache")
				}
				blocked.fail.Store(false)
				close(blocked.release)
			}
			if _, err := r.cached(ctx, e.IndexKey, 0, -1, e.IndexSHA256); err != nil {
				t.Fatalf("fresh retry poisoned: %v", err)
			}
		})
	}
}

func TestSharedReadMapHasFiniteAdmission(t *testing.T) {
	r := &Reader{}
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	entered := make(chan struct{}, maxSharedReads)
	var wg sync.WaitGroup
	for i := 0; i < maxSharedReads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = r.sharedRead(c, fmt.Sprint(i), func(readCtx context.Context) ([]byte, error) {
				entered <- struct{}{}
				<-readCtx.Done()
				return nil, readCtx.Err()
			})
		}(i)
	}
	for i := 0; i < maxSharedReads; i++ {
		<-entered
	}
	if _, err := r.sharedRead(ctx, "excess", func(context.Context) ([]byte, error) {
		t.Error("over-budget backend work started")
		return nil, nil
	}); err == nil {
		t.Fatal("unbounded pending map accepted excess work")
	}
	if got := r.CacheStats().Inflight; got != maxSharedReads {
		t.Fatalf("unexpected pending work: %d", got)
	}
	cancel()
	wg.Wait()
	awaitCondition(t, func() bool { return r.CacheStats().Inflight == 0 })
}

func TestCacheChecksumIdentityAndCorruption(t *testing.T) {
	f := fixture(t)
	r := f.reader
	e := f.entries[0]
	badSum := pack.Sum([]byte("different bytes"))
	if _, err := r.cached(ctx, e.IndexKey, 0, -1, badSum); err == nil {
		t.Fatal("checksum mismatch entered cache")
	}
	if r.CacheStats().Entries != 0 {
		t.Fatal("corrupt response retained")
	}
	if _, err := r.cached(ctx, e.IndexKey, 0, -1, e.IndexSHA256); err != nil {
		t.Fatalf("correct checksum retry failed: %v", err)
	}
	before := f.blobs.Stats().Gets
	if _, err := r.cached(ctx, e.IndexKey, 0, -1, badSum); err == nil {
		t.Fatal("different checksum reused cached successful bytes")
	}
	if got := f.blobs.Stats().Gets; got != before+1 {
		t.Fatalf("checksum identity did not isolate entries: %d -> %d", before, got)
	}
}

func TestCanceledSharedReadDrainsBeforeNewCallerRetries(t *testing.T) {
	r := &Reader{}
	c, cancel := context.WithCancel(ctx)
	started, canceled, drain := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := r.sharedRead(c, "same", func(readCtx context.Context) ([]byte, error) {
			close(started)
			<-readCtx.Done()
			close(canceled)
			<-drain // Model a backend still cleaning up after cancellation.
			return nil, readCtx.Err()
		})
		first <- err
	}()
	<-started
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation was not prompt: %v", err)
	}
	<-canceled
	if got := r.CacheStats().Inflight; got != 1 {
		t.Fatalf("canceled backend released admission before unwind: %d", got)
	}
	// A live caller must await the canceled operation, not inherit its error.
	waiting, stopWaiting := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopWaiting()
	if _, err := r.sharedRead(waiting, "same", func(context.Context) ([]byte, error) {
		t.Error("replacement read started before abandoned I/O drained")
		return nil, nil
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting caller inherited abandoned cancellation: %v", err)
	}
	second := make(chan error, 1)
	go func() {
		_, err := r.sharedRead(ctx, "same", func(readCtx context.Context) ([]byte, error) {
			if err := readCtx.Err(); err != nil {
				return nil, err
			}
			return []byte("retry succeeded"), nil
		})
		second <- err
	}()
	close(drain)
	if err := <-second; err != nil {
		t.Fatalf("healthy caller inherited abandoned cancellation: %v", err)
	}
	if got := r.CacheStats().Inflight; got != 0 {
		t.Fatalf("drained shared read leaked: %d", got)
	}
}
