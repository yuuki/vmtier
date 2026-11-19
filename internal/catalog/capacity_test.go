package catalog

import (
	"context"
	"testing"
)

func TestCommittedCapacity(t *testing.T) {
	for name, fixture := range map[string]func(*testing.T, Contract) ledgerFixture{"sqlite": sqliteLedgerFixtureFor, "postgres": postgresLedgerFixtureFor} {
		t.Run(name, func(t *testing.T) { testCommittedCapacity(t, fixture(t, hourContract())) })
	}
}

func testCommittedCapacity(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	reader, ok := f.repo.(CapacityReader)
	if !ok {
		t.Fatal("service backend does not report capacity")
	}
	_, local := f.repo.(*SQLite)
	observe := func() Capacity {
		t.Helper()
		capacity, err := reader.Capacity(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Only SQLite has local catalog files.
		if (capacity.Files != nil) != local || (local && capacity.Files.DatabaseBytes <= 0) {
			t.Fatalf("catalog files: %+v", capacity.Files)
		}
		capacity.Files = nil
		return capacity
	}
	if got := observe(); got != (Capacity{}) {
		t.Fatalf("initial capacity: %+v", got)
	}
	first := hourPublication("1:2", "a", 1, 2)
	recordPublication(t, f.repo, first)
	if _, err := f.publish(0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.publish(1, hourPublication("1:2", "a", 2, 0)); err != nil {
		t.Fatal(err)
	}
	// Recorded keys and failed publications are not committed objects.
	stale := hourPublication("1:2", "a", 3, 1)
	recordPublication(t, f.repo, stale)
	if _, err := f.publish(0, stale); err == nil {
		t.Fatal("stale generation unexpectedly published")
	}
	if got := observe(); got != (Capacity{Windows: 2, Packs: 2, CommittedDataBytes: 20, CommittedIndexBytes: 40}) {
		t.Fatalf("published capacity: %+v", got)
	}
	// Cold retention removes the window and its packs from the totals.
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 2), 10); err != nil || n != 1 {
		t.Fatalf("retire first window: %d %v", n, err)
	}
	if got := observe(); got != (Capacity{Windows: 1}) {
		t.Fatalf("capacity after retention: %+v", got)
	}
}

func TestPostgresCapacitySurvivesReconnectAndRejectsUnavailableAuthority(t *testing.T) {
	ctx, dsn, db, s := postgresTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 2)
	recordPublication(t, s, first)
	if _, err := publishPostgresTest(ctx, s, 0, first); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	want := Capacity{Windows: 1, Packs: 2, CommittedDataBytes: 20, CommittedIndexBytes: 40}
	if got, err := reopened.Capacity(ctx); err != nil || got != want {
		t.Fatalf("capacity after reconnect: %+v %v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reopened.Capacity(canceled); err == nil {
		t.Fatal("canceled observation succeeded")
	}
	// A catalog of another schema is not observed, as by every other read.
	if _, err := db.ExecContext(ctx, `UPDATE catalog_state SET schema_version=schema_version+1`); err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Capacity(ctx); err == nil {
		t.Fatalf("unsupported schema observed: %+v", got)
	}
}
