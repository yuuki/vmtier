package catalog

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPostgresLeaseOwnershipAndFencing(t *testing.T) {
	ctx, dsn, db, first := postgresTestStore(t, contract())
	second, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	publication := sqlitePublication("1:2", "a", 100, 0)
	if _, err := first.PublishWindow(ctx, 0, publication); !errors.Is(err, ErrLeaseRequired) {
		t.Fatalf("unfenced publication accepted: %v", err)
	}
	lease, err := first.AcquireLease(ctx, "first", time.Minute)
	if err != nil || lease.Token == 0 || lease.Owner != "first" || lease.CatalogID != "test" {
		t.Fatalf("acquire: %+v err=%v", lease, err)
	}
	if _, err := second.AcquireLease(ctx, "second", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second owner stole live lease: %v", err)
	}
	// A retry after a lost acquisition response resolves to one acquisition.
	replayed, err := second.AcquireLease(ctx, "first", time.Minute)
	if err != nil || !reflect.DeepEqual(lease, replayed) {
		t.Fatalf("acquisition replay changed token/deadline: %+v err=%v", replayed, err)
	}
	oldView := first.Fenced(lease)
	if _, err := oldView.PublishWindow(ctx, 0, publication); err != nil {
		t.Fatal(err)
	}
	// Expire via database time, independent of the test host's wall clock.
	if _, err := db.ExecContext(ctx, `UPDATE archive_lease SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := first.RenewLease(ctx, lease, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired lease renewed: %v", err)
	}
	// Even an identical committed retry must not bypass current ownership.
	if _, err := oldView.PublishWindow(ctx, 0, publication); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired owner replay accepted: %v", err)
	}
	next, err := second.AcquireLease(ctx, "second", time.Minute)
	if err != nil || next.Token <= lease.Token {
		t.Fatalf("takeover did not advance token: %+v err=%v", next, err)
	}
	if _, err := oldView.PublishWindow(ctx, 1, sqlitePublication("1:2", "a", 200, 0)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("resumed old owner published: %v", err)
	}
	if err := first.ReleaseLease(ctx, lease); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old owner released successor: %v", err)
	}
	if _, err := second.Fenced(next).PublishWindow(ctx, 1, sqlitePublication("1:2", "a", 200, 0)); err != nil {
		t.Fatal("successor could not publish:", err)
	}
	if state := postgresStatus(t, ctx, second); state.Generation != 2 || state.Coverage[0].End != 300 {
		t.Fatalf("unexpected fenced coverage: %+v", state)
	}
}

func TestPostgresLeaseRenewalReleaseAndConcurrentAcquisition(t *testing.T) {
	ctx, dsn, _, first := postgresTestStore(t, contract())
	second, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	type outcome struct {
		lease Lease
		err   error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for i, client := range []*Postgres{first, second} {
		wg.Add(1)
		go func(owner string, client *Postgres) {
			defer wg.Done()
			<-start
			lease, err := client.AcquireLease(ctx, owner, time.Minute)
			results <- outcome{lease, err}
		}([]string{"first", "second"}[i], client)
	}
	close(start)
	wg.Wait()
	close(results)
	var lease Lease
	held := 0
	for result := range results {
		if result.err == nil {
			if lease.Token != 0 {
				t.Fatal("multiple owners acquired the lease")
			}
			lease = result.lease
		} else if errors.Is(result.err, ErrLeaseHeld) {
			held++
		} else {
			t.Fatal(result.err)
		}
	}
	if lease.Token != 1 || held != 1 {
		t.Fatalf("unexpected first acquisition: %+v held=%d", lease, held)
	}
	view := first.Fenced(lease)
	renewed, err := second.RenewLease(ctx, lease, 2*time.Minute)
	if err != nil || renewed.Token != lease.Token || !renewed.ExpiresAt.After(lease.ExpiresAt) {
		t.Fatalf("renewal: %+v err=%v", renewed, err)
	}
	// A pre-renewal view remains valid because DB expiry is authoritative.
	if _, err := view.PublishWindow(ctx, 0, sqlitePublication("1:2", "a", 100, 0)); err != nil {
		t.Fatal("renewed view could not publish:", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Status(ctx); err != nil {
		t.Fatal("closing fenced view closed runtime handle:", err)
	}
	if err := first.ReleaseLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := first.ReleaseLease(ctx, lease); err != nil {
		t.Fatal("release replay:", err)
	}
	next, err := second.AcquireLease(ctx, "successor", time.Minute)
	if err != nil || next.Token != lease.Token+1 {
		t.Fatalf("release reset fencing counter: %+v err=%v", next, err)
	}
	wrongCatalog := next
	wrongCatalog.CatalogID = "other"
	if _, err := second.Fenced(wrongCatalog).PublishWindow(ctx, 1, sqlitePublication("1:2", "a", 200, 0)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign catalog lease accepted: %v", err)
	}
}

func TestPostgresLeaseExpiresDuringPublication(t *testing.T) {
	ctx, _, db, p := postgresTestStore(t, contract())
	// A database stall after initial ownership validation must not allow an
	// expired writer to commit. This exercises the final check, not a mock.
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION delay_window() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.5); RETURN NEW; END $$;
		CREATE TRIGGER delay_window BEFORE INSERT ON windows FOR EACH ROW EXECUTE FUNCTION delay_window()`); err != nil {
		t.Fatal(err)
	}
	lease, err := p.AcquireLease(ctx, "delayed", 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	delayed := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, p, delayed)
	if _, err := p.Fenced(lease).PublishWindow(ctx, 0, delayed); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("publication committed after lease expiry: %v", err)
	}
	if state := postgresStatus(t, ctx, p); state.Generation != 0 || state.Coverage[0].End != 100 {
		t.Fatalf("expired transaction changed coverage: %+v", state)
	}
	for _, table := range []string{"windows", "packs", "object_keys"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("expired transaction left %s: count=%d err=%v", table, count, err)
		}
	}
}

func TestPostgresUpgradeAddsFencingExplicitly(t *testing.T) {
	ctx, dsn, db, p := postgresTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, p, first)
	if _, err := publishPostgresTest(ctx, p, 0, first); err != nil {
		t.Fatal(err)
	}
	before := postgresStatus(t, ctx, p)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Reconstruct the stage-1 schema shape while preserving a committed window.
	if _, err := db.ExecContext(ctx, `DROP TABLE archive_lease, pending_objects; DROP INDEX windows_start; ALTER TABLE catalog_state DROP COLUMN retired_windows; ALTER TABLE coverage DROP COLUMN start_ms; UPDATE catalog_state SET schema_version=1`); err != nil {
		t.Fatal(err)
	}
	if old, err := OpenPostgres(ctx, dsn, "test"); err == nil {
		old.Close()
		t.Fatal("normal open silently upgraded schema 1")
	}
	if err := UpgradePostgres(ctx, dsn, "wrong-id"); err == nil {
		t.Fatal("upgrade accepted wrong identity")
	}
	if err := UpgradePostgres(ctx, dsn, "test"); err != nil {
		t.Fatal(err)
	}
	if err := UpgradePostgres(ctx, dsn, "test"); err != nil {
		t.Fatal("repeat upgrade:", err)
	}
	upgraded, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if after := postgresStatus(t, ctx, upgraded); !reflect.DeepEqual(before, after) {
		t.Fatalf("upgrade changed publication state: before=%+v after=%+v", before, after)
	}
	if _, err := upgraded.PublishWindow(ctx, 1, sqlitePublication("1:2", "a", 200, 0)); !errors.Is(err, ErrLeaseRequired) {
		t.Fatalf("upgraded catalog permits unfenced writes: %v", err)
	}
}

func TestPostgresConnectionErrorsDoNotExposeDSN(t *testing.T) {
	secret := "sensitive-test-password"
	_, err := OpenPostgres(context.Background(), "postgres://user:"+secret+"@host/%zz", "test")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("connection error exposed DSN: %v", err)
	}
}

func TestPostgresPublicationChecksExpiryAfterWaitingForStateLock(t *testing.T) {
	ctx, _, db, p := postgresTestStore(t, contract())
	lease, err := p.AcquireLease(ctx, "waiting", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(ctx, `SELECT singleton FROM catalog_state WHERE singleton=1 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Fenced(lease).PublishWindow(ctx, 0, sqlitePublication("1:2", "a", 100, 0))
		done <- err
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := db.QueryRowContext(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND strpos(query,$1)>0)`, "FROM "+p.table("catalog_state")).Scan(&blocked)
		if err != nil {
			_ = blocker.Rollback()
			<-done
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("publication did not wait for state lock: %v", err)
		case <-waitCtx.Done():
			_ = blocker.Rollback()
			<-done
			t.Fatal("publication did not reach state lock")
		case <-ticker.C:
		}
	}
	// This change becomes visible only after the publisher has begun waiting.
	if _, err := blocker.ExecContext(ctx, `UPDATE archive_lease SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("publisher used pre-lock ownership: %v", err)
	}
	if state := postgresStatus(t, ctx, p); state.Generation != 0 {
		t.Fatalf("expired waiting publisher advanced generation: %+v", state)
	}
}
