package archive

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteWindowFailureObserverCountsAttemptsNotSkippedRetries(t *testing.T) {
	server, source := newSQLiteSource(t, "")
	source.down.Store(true)
	cfg := baseConfig(server.URL)
	cfg.Tenants = []string{"7:3", "8:1"}
	a, _ := newSQLiteArchiver(t, cfg)
	failures := 0
	a.OnWindowFailure = func(tenant, source string, err error) {
		if tenant != "7:3" || source != cfg.Sources[0].ID || err == nil {
			t.Errorf("unexpected failure observation: %s %s %v", tenant, source, err)
		}
		failures++
	}
	if result, err := a.RunPass(context.Background(), testNow); err == nil || result.CommittedWindows != 1 || failures != 1 {
		t.Fatalf("first pass=%+v err=%v failures=%d", result, err, failures)
	}
	if _, err := a.RunPass(context.Background(), testNow); err != nil || failures != 1 {
		t.Fatalf("backoff created another failure: err=%v failures=%d", err, failures)
	}
	source.down.Store(false)
	if _, err := a.RunPass(context.Background(), testNow.Add(time.Minute)); err != nil || failures != 1 {
		t.Fatalf("successful retry created failure: err=%v failures=%d", err, failures)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.RunPass(ctx, testNow); !errors.Is(err, context.Canceled) || failures != 1 {
		t.Fatalf("canceled pass without attempted window changed count: err=%v failures=%d", err, failures)
	}
}

func TestSQLiteWindowFailureObserverRetainsTimeoutReason(t *testing.T) {
	server, source := newSQLiteSource(t, "stall")
	source.down.Store(true)
	a, _ := newSQLiteArchiver(t, baseConfig(server.URL))
	a.WindowTimeout = 20 * time.Millisecond
	var failure error
	a.OnWindowFailure = func(_, _ string, err error) { failure = err }
	if _, err := a.RunPass(context.Background(), testNow); err == nil || !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("timeout observation=%v pass error=%v", failure, err)
	}
}

func TestSQLiteWindowFailureObserverCountsCanceledAttempt(t *testing.T) {
	server, _ := newSQLiteSource(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := baseConfig(server.URL)
	cfg.Hook = func(stage string) error {
		if stage == "verified" {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	a, _ := newSQLiteArchiver(t, cfg)
	var failure error
	count := 0
	a.OnWindowFailure = func(_, _ string, err error) { failure = err; count++ }
	result, err := a.RunPass(ctx, testNow)
	if !errors.Is(err, context.Canceled) || !errors.Is(failure, context.Canceled) || count != 1 || result.CommittedWindows != 0 {
		t.Fatalf("canceled attempt: result=%+v failure=%v count=%d pass error=%v", result, failure, count, err)
	}
}
