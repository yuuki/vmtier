package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// ledgerFixture runs the object ledger contract against one backend.
type ledgerFixture struct {
	repo    Repository
	publish func(uint64, Publication) (uint64, error)
	// age makes every recorded key at least d older on the catalog's clock.
	age func(time.Duration)
	// exec runs raw SQL against the catalog's tables.
	exec func(string, ...any) error
}

func sqliteLedgerFixture(t *testing.T) ledgerFixture {
	s := sqliteTestStore(t, contract())
	var mu sync.Mutex
	offset := time.Duration(0)
	s.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return time.Now().Add(offset)
	}
	return ledgerFixture{
		repo:    s,
		publish: func(gen uint64, p Publication) (uint64, error) { return s.PublishWindow(context.Background(), gen, p) },
		age: func(d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			offset += d
		},
		exec: func(query string, args ...any) error {
			_, err := s.db.Exec(query, args...)
			return err
		},
	}
}

func postgresLedgerFixture(t *testing.T) ledgerFixture {
	ctx, _, db, s := postgresTestStore(t, contract())
	return ledgerFixture{
		repo:    s,
		publish: func(gen uint64, p Publication) (uint64, error) { return publishPostgresTest(ctx, s, gen, p) },
		// The database clock cannot be moved: age the records instead.
		age: func(d time.Duration) {
			if _, err := db.ExecContext(ctx, `UPDATE pending_objects SET created_ms=created_ms-$1`, d.Milliseconds()); err != nil {
				t.Fatal(err)
			}
		},
		exec: func(query string, args ...any) error {
			_, err := db.ExecContext(ctx, query, args...)
			return err
		},
	}
}

func publicationKeys(p Publication) []string {
	var keys []string
	for _, ref := range p.Packs {
		keys = append(keys, ref.DataKey, ref.IndexKey)
	}
	sort.Strings(keys)
	return keys
}

func TestObjectLedger(t *testing.T) {
	for name, fixture := range map[string]func(*testing.T) ledgerFixture{"sqlite": sqliteLedgerFixture, "postgres": postgresLedgerFixture} {
		t.Run(name, func(t *testing.T) {
			t.Run("claim only old unpublished keys", func(t *testing.T) { testLedgerClaim(t, fixture(t)) })
			t.Run("record validation", func(t *testing.T) { testLedgerRecordValidation(t, fixture(t)) })
			t.Run("claim refuses damaged ledger", func(t *testing.T) { testLedgerDamaged(t, fixture(t)) })
		})
	}
}

func testLedgerClaim(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	published, abandoned := sqlitePublication("1:2", "a", 100, 1), sqlitePublication("1:2", "a", 200, 2)
	recordPublication(t, f.repo, published)
	recordPublication(t, f.repo, abandoned)
	if counts, err := f.repo.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{Pending: 6}) {
		t.Fatalf("recorded counts: %+v %v", counts, err)
	}
	if _, err := f.publish(0, published); err != nil {
		t.Fatal(err)
	}
	if keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Hour, 10); err != nil || len(keys) != 0 {
		t.Fatalf("claimed young keys: %v %v", keys, err)
	}
	f.age(2 * time.Hour)
	first, err := f.repo.ClaimUnpublishedObjects(ctx, time.Hour, 3)
	if err != nil || len(first) != 3 {
		t.Fatalf("bounded claim: %v %v", first, err)
	}
	second, err := f.repo.ClaimUnpublishedObjects(ctx, time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Claimed keys that were not forgotten come back first, so an interrupted
	// collection resumes; the remaining pending key is claimed after them.
	all := append(append([]string(nil), first...), second...)
	sort.Strings(all)
	if want := publicationKeys(abandoned); len(second) != 4 || !reflect.DeepEqual(all[0:1], want[0:1]) || !containsAll(second, want) {
		t.Fatalf("resumed claim: first=%v second=%v want=%v", first, second, want)
	}
	if counts, err := f.repo.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{Deleting: 4}) {
		t.Fatalf("claimed counts: %+v %v", counts, err)
	}
	// A claimed key can never be published, even by an attempt that verified it.
	if _, err := f.publish(1, abandoned); !errors.Is(err, ErrUnrecordedObject) {
		t.Fatalf("claimed key published: %v", err)
	}
	if err := f.repo.RecordPendingObjects(ctx, "1:2", []string{abandoned.Packs[0].DataKey}); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("claimed key recorded again: %v", err)
	}
	if err := f.repo.ForgetDeletedObjects(ctx, publicationKeys(abandoned)[:2]); err != nil {
		t.Fatal(err)
	}
	if counts, err := f.repo.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{Deleting: 2}) {
		t.Fatalf("forgotten counts: %+v %v", counts, err)
	}
	if err := f.repo.ForgetDeletedObjects(ctx, publicationKeys(abandoned)); err != nil {
		t.Fatal(err)
	}
	if counts, err := f.repo.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{}) {
		t.Fatalf("final counts: %+v %v", counts, err)
	}
	// Forgotten keys stay unpublishable: publication requires a pending record.
	if _, err := f.publish(1, abandoned); !errors.Is(err, ErrUnrecordedObject) {
		t.Fatalf("forgotten key published: %v", err)
	}
	// Forgetting a pending key does nothing: only claimed keys are forgotten.
	retry := sqlitePublication("1:2", "a", 200, 1)
	retry.Packs[0].DataKey, retry.Packs[0].IndexKey = "tenants/1:2/attempts/retry/x.pack", "tenants/1:2/attempts/retry/x.json"
	recordPublication(t, f.repo, retry)
	if err := f.repo.ForgetDeletedObjects(ctx, publicationKeys(retry)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.publish(1, retry); err != nil {
		t.Fatalf("retry with fresh keys: %v", err)
	}
	for _, bad := range []struct {
		age   time.Duration
		limit int
	}{{0, 1}, {time.Microsecond, 1}, {time.Hour, 0}, {time.Hour, MaxClaimedObjects + 1}} {
		if _, err := f.repo.ClaimUnpublishedObjects(ctx, bad.age, bad.limit); err == nil {
			t.Fatalf("invalid claim accepted: %+v", bad)
		}
	}
	if err := f.repo.ForgetDeletedObjects(ctx, []string{"../escape"}); err == nil {
		t.Fatal("invalid forget accepted")
	}
}

func containsAll(have, want []string) bool {
	set := map[string]bool{}
	for _, key := range have {
		set[key] = true
	}
	for _, key := range want {
		if !set[key] {
			return false
		}
	}
	return true
}

func testLedgerRecordValidation(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		tenant string
		keys   []string
	}{
		{"no keys", "1:2", nil},
		{"unknown tenant", "3:4", []string{"tenants/3:4/attempts/x"}},
		{"tenant escape", "1:2", []string{"tenants/3:4/attempts/x"}},
		{"hidden component", "1:2", []string{"tenants/1:2/.attempts/x"}},
		{"repeated", "1:2", []string{"tenants/1:2/attempts/x", "tenants/1:2/attempts/x"}},
	} {
		if err := f.repo.RecordPendingObjects(ctx, tc.tenant, tc.keys); !errors.Is(err, ErrInvalidPublication) {
			t.Fatalf("%s accepted: %v", tc.name, err)
		}
	}
	if err := f.repo.RecordPendingObjects(ctx, "1:2", []string{"tenants/1:2/attempts/x"}); err != nil {
		t.Fatal(err)
	}
	if err := f.repo.RecordPendingObjects(ctx, "1:2", []string{"tenants/1:2/attempts/y", "tenants/1:2/attempts/x"}); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("duplicate recording accepted: %v", err)
	}
	if counts, err := f.repo.UnpublishedObjects(ctx); err != nil || counts.Pending != 1 {
		t.Fatalf("failed recording was not atomic: %+v %v", counts, err)
	}
}

func testLedgerDamaged(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	p := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, f.repo, p)
	if _, err := f.publish(0, p); err != nil {
		t.Fatal(err)
	}
	// A damaged ledger that lists a published key must not direct a deletion.
	if err := f.exec(`INSERT INTO pending_objects(object_key,created_ms,state) VALUES('` + p.Packs[0].DataKey + `',0,'pending')`); err != nil {
		t.Fatal(err)
	}
	if keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Millisecond, 10); err == nil {
		t.Fatalf("damaged ledger claimed a published key: %v", keys)
	}
	if err := f.exec(`DELETE FROM pending_objects`); err != nil {
		t.Fatal(err)
	}
	if err := f.exec(`INSERT INTO pending_objects(object_key,created_ms,state) VALUES('tenants/3:4/attempts/x',0,'deleting')`); err != nil {
		t.Fatal(err)
	}
	if keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Millisecond, 10); err == nil {
		t.Fatalf("damaged ledger claimed another tenant's key: %v", keys)
	}
}

// A publication and a claim of the same keys race on independent connections:
// exactly one of them wins, and a claimed key is never published.
func TestPostgresLedgerClaimRacesPublication(t *testing.T) {
	ctx, dsn, db, s := postgresTestStore(t, contract())
	collector, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	lease, err := s.AcquireLease(ctx, "racer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	published, claimedWins := 0, 0
	for i := 0; i < 40; i++ {
		start := int64(100 + 100*published)
		p := sqlitePublication("1:2", "a", start, 1)
		p.Packs[0].DataKey = fmt.Sprintf("tenants/1:2/attempts/race-%d.pack", i)
		p.Packs[0].IndexKey = fmt.Sprintf("tenants/1:2/attempts/race-%d.json", i)
		recordPublication(t, s, p)
		if _, err := db.ExecContext(ctx, `UPDATE pending_objects SET created_ms=0`); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var publishErr, claimErr error
		var claimed []string
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, publishErr = s.Fenced(lease).PublishWindow(ctx, uint64(published), p)
		}()
		go func() {
			defer wg.Done()
			claimed, claimErr = collector.ClaimUnpublishedObjects(ctx, time.Millisecond, 10)
		}()
		wg.Wait()
		if claimErr != nil {
			t.Fatal(claimErr)
		}
		// Row locks order each key's claim and consumption. A claim may skip a
		// key the publication holds and take the other one, failing it; the
		// skipped key stays pending for a later claim. A published key is never
		// claimed.
		switch {
		case publishErr == nil && len(claimed) == 0:
			published++
		case errors.Is(publishErr, ErrUnrecordedObject) && len(claimed) > 0 && containsAll(publicationKeys(p), claimed):
			claimedWins++
			// The next claim returns the unforgotten claims again, plus the rest.
			all, err := collector.ClaimUnpublishedObjects(ctx, time.Millisecond, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(all) != 2 || !containsAll(all, publicationKeys(p)) {
				t.Fatalf("race %d: claims did not cover the failed attempt: %v", i, all)
			}
			if err := collector.ForgetDeletedObjects(ctx, publicationKeys(p)); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("race %d: publish=%v claimed=%v", i, publishErr, claimed)
		}
	}
	state := postgresStatus(t, ctx, s)
	if state.Generation != uint64(published) {
		t.Fatalf("generation=%d published=%d", state.Generation, published)
	}
	if counts, err := s.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{}) {
		t.Fatalf("race left records: %+v %v", counts, err)
	}
	t.Logf("published=%d claimed=%d", published, claimedWins)
}

func TestPostgresUpgradeAddsObjectLedger(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, `DROP TABLE pending_objects; UPDATE catalog_state SET schema_version=2`); err != nil {
		t.Fatal(err)
	}
	if old, err := OpenPostgres(ctx, dsn, "test"); err == nil {
		old.Close()
		t.Fatal("normal open silently upgraded schema 2")
	}
	if err := UpgradePostgres(ctx, dsn, "test"); err != nil {
		t.Fatal(err)
	}
	upgraded, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if after := postgresStatus(t, ctx, upgraded); !reflect.DeepEqual(before, after) {
		t.Fatalf("upgrade changed publication state: before=%+v after=%+v", before, after)
	}
	if counts, err := upgraded.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{}) {
		t.Fatalf("upgraded ledger: %+v %v", counts, err)
	}
	if err := upgraded.RecordPendingObjects(ctx, "1:2", []string{"tenants/1:2/attempts/after-upgrade"}); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresImportCopiesObjectLedger(t *testing.T) {
	ctx, dsn, _ := postgresTestDatabase(t)
	source := sqliteTestStore(t, contract())
	published := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, source, published)
	if _, err := source.PublishWindow(ctx, 0, published); err != nil {
		t.Fatal(err)
	}
	abandoned := sqlitePublication("1:2", "a", 200, 1)
	recordPublication(t, source, abandoned)
	source.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	claimed, err := source.ClaimUnpublishedObjects(ctx, time.Hour, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	destination, err := ImportSQLiteToPostgres(ctx, source, dsn, "ledger-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if counts, err := destination.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{Pending: 1, Deleting: 1}) {
		t.Fatalf("imported ledger: %+v %v", counts, err)
	}
	keys, err := destination.ClaimUnpublishedObjects(ctx, time.Hour, 10)
	if err != nil || !reflect.DeepEqual(keys, claimed) {
		t.Fatalf("imported claim resumes: %v want %v (%v)", keys, claimed, err)
	}
}

func TestSQLiteUpgradeAddsObjectLedger(t *testing.T) {
	ctx := context.Background()
	s := sqliteTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, s, first)
	if _, err := s.PublishWindow(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	before := sqliteStatus(t, s)
	downgradeSQLite(t, s)
	if old, err := OpenSQLite(ctx, s.dir, s.id); err == nil {
		old.Close()
		t.Fatal("normal open silently upgraded schema 1")
	}
	if err := UpgradeSQLite(ctx, s.dir, "wrong-id"); err == nil {
		t.Fatal("upgrade accepted wrong identity")
	}
	for i := 0; i < 2; i++ {
		if err := UpgradeSQLite(ctx, s.dir, s.id); err != nil {
			t.Fatalf("upgrade %d: %v", i, err)
		}
	}
	upgraded, err := OpenSQLite(ctx, s.dir, s.id)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if after := sqliteStatus(t, upgraded); !reflect.DeepEqual(before, after) {
		t.Fatalf("upgrade changed state: before=%+v after=%+v", before, after)
	}
	held, err := OpenSQLite(ctx, s.dir, s.id)
	if err == nil {
		held.Close()
		t.Fatal("second owner opened the catalog")
	}
	if err := UpgradeSQLite(ctx, s.dir, s.id); err == nil {
		t.Fatal("upgrade ran while the catalog was owned")
	}
}

// downgradeSQLite closes s and turns its file into the schema 1 shape.
func downgradeSQLite(t *testing.T, s *SQLite) {
	t.Helper()
	if _, err := s.db.Exec(`DROP TABLE pending_objects; UPDATE catalog_state SET schema_version=1`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteRestoreKeepsLedgerAndUpgradesSchema1Backups(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := CreateSQLite(ctx, filepath.Join(root, "state"), "original", contract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	first := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, s, first)
	if _, err := s.PublishWindow(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	abandoned := sqlitePublication("1:2", "a", 200, 1)
	recordPublication(t, s, abandoned)
	if err := s.Backup(ctx, filepath.Join(root, "backup")); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSQLite(ctx, filepath.Join(root, "backup"), filepath.Join(root, "restored"), "restored")
	if err != nil {
		t.Fatal(err)
	}
	if counts, err := restored.UnpublishedObjects(ctx); err != nil || counts.Pending != 2 {
		t.Fatalf("restored ledger: %+v %v", counts, err)
	}
	restored.Close()

	// A backup whose ledger names a published key is refused.
	if _, err := s.db.Exec(`INSERT INTO pending_objects VALUES(?,0,'deleting')`, first.Packs[0].IndexKey); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(ctx, filepath.Join(root, "damaged")); err != nil {
		t.Fatal(err)
	}
	if db, err := RestoreSQLite(ctx, filepath.Join(root, "damaged"), filepath.Join(root, "restored-damaged"), "restored-damaged"); err == nil {
		db.Close()
		t.Fatal("restore accepted a ledger record of a published key")
	}
	if _, err := s.db.Exec(`DELETE FROM pending_objects WHERE object_key=?`, first.Packs[0].IndexKey); err != nil {
		t.Fatal(err)
	}

	// A backup made by a release without the ledger still restores.
	before := sqliteStatus(t, s)
	downgradeSQLite(t, s)
	writeSchema1Backup(t, filepath.Join(root, "state"), filepath.Join(root, "schema1"), "original")
	upgraded, err := RestoreSQLite(ctx, filepath.Join(root, "schema1"), filepath.Join(root, "restored-schema1"), "restored-schema1")
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	after := sqliteStatus(t, upgraded)
	before.ID = "restored-schema1"
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("schema 1 restore: before=%+v after=%+v", before, after)
	}
	if counts, err := upgraded.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{}) {
		t.Fatalf("schema 1 restore ledger: %+v %v", counts, err)
	}
}

// writeSchema1Backup writes a backup of a schema 1 catalog as an earlier
// release did: a database image and metadata that records schema 1.
func writeSchema1Backup(t *testing.T, stateDir, backupDir, id string) {
	t.Helper()
	ctx := context.Background()
	s, err := openSQLiteFile(ctx, stateDir, id)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := os.Mkdir(backupDir, 0700); err != nil {
		t.Fatal(err)
	}
	image := filepath.Join(backupDir, SQLiteFile)
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", image); err != nil {
		t.Fatal(err)
	}
	status, err := readSQLiteStatusVersion(ctx, sqlDB{s.db}, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	checksum, size, err := hashDatabase(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(sqliteBackupMetadata{Version: 1, DatabaseBytes: size, DatabaseSHA256: checksum, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, backupMetadataFile), data, 0600); err != nil {
		t.Fatal(err)
	}
}

type sqlDB struct{ db *sql.DB }

func (q sqlDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return q.db.QueryRowContext(ctx, query, args...)
}

func (q sqlDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.db.QueryContext(ctx, query, args...)
}
