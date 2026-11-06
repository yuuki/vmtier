package archive

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
)

type observedBlobs struct {
	objectstore.BlobStore
	mu   sync.Mutex
	puts map[string]int
}

func (s *observedBlobs) Put(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	s.puts[key]++
	s.mu.Unlock()
	return s.BlobStore.Put(ctx, key, data)
}
func (s *observedBlobs) snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]int{}
	for k, n := range s.puts {
		m[k] = n
	}
	return m
}
func observer(t *testing.T) *observedBlobs {
	return &observedBlobs{BlobStore: localStore(t), puts: map[string]int{}}
}

// Two writers can upload concurrently to plain PUT object stores. The authority
// serializes publication; losing uploads must never touch the winner's keys.
func TestCompetingWritersUseDisjointAttemptKeys(t *testing.T) {
	cfg := baseConfig("")
	server, _ := sourceServer(t, cfg, "")
	defer server.Close()
	cfg.Sources[0].URL = server.URL
	blobs := observer(t)
	authority := localCatalog(t)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	cfg.Hook = func(stage string) error {
		if stage == "verified" {
			ready <- struct{}{}
			<-release
		}
		return nil
	}
	errorsOut := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, e := Run(context.Background(), blobs, authority, cfg, testNow); errorsOut <- e }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case e := <-errorsOut:
			t.Fatalf("writer failed before barrier: %v", e)
		case <-time.After(10 * time.Second):
			t.Fatal("writers did not reach publication barrier")
		}
	}
	close(release)
	wins, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		e := <-errorsOut
		if e == nil {
			wins++
		} else if errors.Is(e, catalog.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected publish error %v", e)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	m, _, e := catalog.Load(context.Background(), authority)
	if e != nil {
		t.Fatal(e)
	}
	if m.Generation != 1 || len(m.Entries) != 1 {
		t.Fatal("losing writer changed winner")
	}
	puts := blobs.snapshot()
	if len(puts) != 4 {
		t.Fatalf("expected four disjoint uploads, got %v", puts)
	}
	for key, n := range puts {
		if n != 1 || !strings.Contains(key, "/attempts/") {
			t.Fatalf("key reused or missing attempt identity %s: %d", key, n)
		}
	}
	winner := m.Entries[0]
	if puts[winner.DataKey] != 1 || puts[winner.IndexKey] != 1 {
		t.Fatal("winner keys were overwritten or missing")
	}
	// Retry after Hot retention has elapsed remains a checkpoint no-op.
	cfg.Hook = nil
	r, e := Run(context.Background(), blobs, authority, cfg, testNow.Add(48*time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if r.SkippedWindows != 1 || len(blobs.snapshot()) != 4 {
		t.Fatal("committed retry rewrote blobs")
	}
}

func TestAmbiguousPutLeavesOnlyUnpublishedUniqueOrphan(t *testing.T) {
	cfg := baseConfig("")
	server, _ := sourceServer(t, cfg, "")
	defer server.Close()
	cfg.Sources[0].URL = server.URL
	blobs := observer(t)
	authority := localCatalog(t)
	if _, e := Run(context.Background(), faultBlobs{blobs, "ambiguous-put"}, authority, cfg, testNow); e == nil {
		t.Fatal("ambiguous upload accepted")
	}
	before := blobs.snapshot()
	if len(before) != 1 {
		t.Fatal("expected one uncertain upload")
	}
	m, _, e := catalog.Load(context.Background(), authority)
	if e != nil || m.Generation != 0 {
		t.Fatalf("uncertain PUT published %+v %v", m, e)
	}
	if _, e = Run(context.Background(), blobs, authority, cfg, testNow); e != nil {
		t.Fatal(e)
	}
	after := blobs.snapshot()
	if len(after) != 3 {
		t.Fatalf("retry must allocate new keys, got %v", after)
	}
	m, _, e = catalog.Load(context.Background(), authority)
	if e != nil {
		t.Fatal(e)
	}
	for key := range before {
		if key == m.Entries[0].DataKey || key == m.Entries[0].IndexKey {
			t.Fatal("uncertain previous attempt reused")
		}
		if after[key] != 1 {
			t.Fatal("orphan was overwritten")
		}
	}
}

type unreadableCatalog struct{}

func (unreadableCatalog) Read(context.Context) (*catalog.Manifest, error) {
	return nil, errors.New("authority unavailable")
}
func (unreadableCatalog) Publish(context.Context, uint64, *catalog.Manifest) error {
	return errors.New("must not publish")
}
func TestUnavailableAuthorityCannotFallBackToObjects(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1")
	blobs := observer(t)
	if _, e := Run(context.Background(), blobs, unreadableCatalog{}, cfg, testNow); e == nil {
		t.Fatal("unavailable authority accepted")
	}
	if len(blobs.snapshot()) != 0 {
		t.Fatal("uploaded despite unavailable authority")
	}
}

// Changing the lateness policy after publication can reopen an immutable Cold
// window. Reject policy drift before export, upload or even a checkpoint no-op.
func TestPolicyChangesRejectedBeforeAnyUpload(t *testing.T) {
	cfg := baseConfig("")
	server, _ := sourceServer(t, cfg, "")
	cfg.Sources[0].URL = server.URL
	blobs := observer(t)
	authority := localCatalog(t)
	if _, err := Run(context.Background(), blobs, authority, cfg, testNow); err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Close() // Any attempt to re-export must also fail this test's error check.
	before := blobs.snapshot()
	for name, mutate := range map[string]func(*Config){
		"age":      func(c *Config) { c.Age += time.Hour },
		"lateness": func(c *Config) { c.Lateness += time.Minute },
		"reopen published windows": func(c *Config) {
			c.Age = 6 * time.Hour
			c.Lateness = 5 * time.Hour
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			mutate(&changed)
			if _, err := Run(context.Background(), blobs, authority, changed, testNow); err == nil || !strings.Contains(err.Error(), "catalog contract differs") {
				t.Fatalf("expected early policy rejection, got %v", err)
			}
			after := blobs.snapshot()
			if len(after) != len(before) {
				t.Fatal("policy mismatch uploaded new objects")
			}
			for key, n := range before {
				if after[key] != n {
					t.Fatal("policy mismatch overwrote an object")
				}
			}
			m, _, err := catalog.Load(context.Background(), authority)
			if err != nil || m.Generation != 1 || m.Contract.AgeMS != cfg.Age.Milliseconds() || m.Contract.LatenessMS != cfg.Lateness.Milliseconds() {
				t.Fatalf("policy rejection changed authority: %+v %v", m, err)
			}
		})
	}
}
