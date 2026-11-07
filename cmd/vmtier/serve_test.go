package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
)

func TestServeRequiresInitializedPinnedSQLite(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	root := t.TempDir()
	c, _ := sqliteCLIConfig(t, root, true)
	ctx := context.Background()
	if err := runServe(ctx, &c, "invalid", "invalid"); err == nil || !strings.Contains(err.Error(), "catalog.type=sqlite") {
		t.Fatalf("legacy catalog accepted: %v", err)
	}
	c.Catalog.Type = "sqlite"
	c.Catalog.Path = filepath.Join(root, "sqlite")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := runServe(ctx, &c, "invalid", "invalid"); !errors.Is(err, catalog.ErrNotInitialized) {
		t.Fatalf("missing authority not rejected before external work: %v", err)
	}
	blob, err := c.BlobLocator()
	if err != nil {
		t.Fatal(err)
	}
	db, err := catalog.CreateSQLite(ctx, c.Catalog.Path, c.Catalog.ID, c.ArchiveContract(), blob)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	changed := c
	changed.ArchiveAge = "4h"
	if err := changed.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := runServe(ctx, &changed, "invalid", "invalid"); err == nil || !strings.Contains(err.Error(), "contract") {
		t.Fatalf("contract drift accepted: %v", err)
	}
	changed = c
	changed.Store.Bucket = "different-bucket"
	if err := runServe(ctx, &changed, "invalid", "invalid"); err == nil || !strings.Contains(err.Error(), "blob locator") {
		t.Fatalf("locator drift accepted: %v", err)
	}
	for _, op := range []string{"archive", "gateway"} {
		if err := runSQLiteOffline(ctx, op, &c); err == nil || !strings.Contains(err.Error(), "use vmtier serve") {
			t.Fatalf("independent SQLite worker accepted: %v", err)
		}
	}
}

type testPassRunner struct{ calls atomic.Int64 }

func (r *testPassRunner) RunPass(ctx context.Context, now time.Time) (*archive.Result, error) {
	if r.calls.Add(1) == 1 {
		return &archive.Result{CommittedWindows: 1, Samples: 2, Bytes: 10}, nil
	}
	return &archive.Result{}, errors.New("source unavailable")
}

func TestArchiveLoopDrainsProgressAndCancelsIdleWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &testPassRunner{}
	var stats serviceStats
	done := make(chan struct{})
	go func() { defer close(done); runArchiveLoop(ctx, runner, time.Hour, &stats) }()
	deadline := time.Now().Add(2 * time.Second)
	for stats.failures.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for scheduler interval")
	}
	if runner.calls.Load() != 2 || stats.windows.Load() != 1 || stats.samples.Load() != 2 || stats.failures.Load() != 1 {
		t.Fatal("progress did not drain immediately before bounded failure wait")
	}
}

func TestServeShutdownReleasesCatalogOwnership(t *testing.T) {
	root := t.TempDir()
	c, _ := sqliteCLIConfig(t, root, false)
	c.Catalog.Type, c.Catalog.Path = "sqlite", filepath.Join(root, "sqlite")
	// No windows are eligible yet, so this lifecycle test needs no VM process.
	c.CoverageStart = time.Now().Add(time.Hour).Truncate(time.Hour).UnixMilli()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	blob, err := c.BlobLocator()
	if err != nil {
		t.Fatal(err)
	}
	db, err := catalog.CreateSQLite(context.Background(), c.Catalog.Path, c.Catalog.ID, c.ArchiveContract(), blob)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runServe(ctx, &c, "127.0.0.1:0", address) }()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	started := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + address + "/health")
		if err == nil {
			response.Body.Close()
			started = response.StatusCode == http.StatusOK
			if started {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !started {
		cancel()
		t.Fatalf("service never became healthy: %v", <-done)
	}
	if second, err := catalog.OpenSQLite(context.Background(), c.Catalog.Path, c.Catalog.ID); err == nil {
		second.Close()
		t.Fatal("service did not own catalog")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("service shutdown did not finish")
	}
	reopened, err := catalog.OpenSQLite(context.Background(), c.Catalog.Path, c.Catalog.ID)
	if err != nil {
		t.Fatalf("shutdown retained catalog lock: %v", err)
	}
	reopened.Close()
}
