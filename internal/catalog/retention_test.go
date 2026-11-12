package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRetireWindows(t *testing.T) {
	for name, fixture := range map[string]func(*testing.T) ledgerFixture{"sqlite": sqliteLedgerFixture, "postgres": postgresLedgerFixture} {
		t.Run(name, func(t *testing.T) { testRetireWindows(t, fixture(t)) })
	}
}

func testRetireWindows(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	var publications []Publication
	for i := 0; i < 3; i++ {
		p := sqlitePublication("1:2", "a", 100+int64(i)*100, 1+i%2)
		if i == 1 {
			p.Packs = []PackRef{}
		}
		recordPublication(t, f.repo, p)
		if _, err := f.publish(uint64(i), p); err != nil {
			t.Fatal(err)
		}
		publications = append(publications, p)
	}
	before, err := f.repo.Status(ctx)
	if err != nil || before.Coverage[0].Start != 100 || before.Coverage[0].End != 400 || before.RetiredWindows != 0 {
		t.Fatalf("initial status: %+v %v", before, err)
	}
	for _, bad := range []struct {
		before int64
		limit  int
	}{{-1, 1}, {100, 0}, {100, MaxRetiredWindows + 1}} {
		if _, err := f.repo.RetireWindows(ctx, bad.before, bad.limit); err == nil {
			t.Fatalf("invalid retirement accepted: %+v", bad)
		}
	}
	// Only whole windows that end at or before the boundary are retired.
	if n, err := f.repo.RetireWindows(ctx, 199, 10); err != nil || n != 0 {
		t.Fatalf("partial window retired: %d %v", n, err)
	}
	if n, err := f.repo.RetireWindows(ctx, 250, 10); err != nil || n != 1 {
		t.Fatalf("retire first window: %d %v", n, err)
	}
	after, err := f.repo.Status(ctx)
	if err != nil || after.Coverage[0].Start != 200 || after.Coverage[0].End != 400 || after.RetiredWindows != 1 || after.Generation != before.Generation {
		t.Fatalf("status after retirement: %+v %v", after, err)
	}
	plan, err := f.repo.Select(ctx, Selection{Tenant: "1:2", Start: 0, End: 400})
	if err != nil || len(plan.Windows) != 2 || plan.Windows[0].Start != 200 {
		t.Fatalf("retired window still selected: %+v %v", plan, err)
	}
	if counts, err := f.repo.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{Retired: 2}) {
		t.Fatalf("retired keys: %+v %v", counts, err)
	}
	retiredKey := publications[0].Packs[0].DataKey
	if err := f.repo.RecordPendingObjects(ctx, "1:2", []string{retiredKey}); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("retired key recorded again: %v", err)
	}
	// Retired windows cannot be published again, even by a replay.
	if _, err := f.publish(after.Generation, publications[0]); err == nil {
		t.Fatal("retired window republished")
	}
	// Publication continues at the coverage end without a generation conflict.
	next := sqlitePublication("1:2", "a", 400, 1)
	recordPublication(t, f.repo, next)
	if _, err := f.publish(after.Generation, next); err != nil {
		t.Fatal(err)
	}
	// Retired keys wait for the minimum age like pending ones, then are claimed.
	if keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Hour, 10); err != nil || len(keys) != 0 {
		t.Fatalf("young retired keys claimed: %v %v", keys, err)
	}
	f.age(2 * time.Hour)
	keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Hour, 10)
	if err != nil || !reflect.DeepEqual(keys, publicationKeys(publications[0])) {
		t.Fatalf("retired keys not claimed: %v %v", keys, err)
	}
	if err := f.repo.ForgetDeletedObjects(ctx, keys); err != nil {
		t.Fatal(err)
	}
	// A boundary beyond the coverage end retires everything published, in
	// bounded batches, and the start never passes the end.
	if n, err := f.repo.RetireWindows(ctx, 10_000, 2); err != nil || n != 2 {
		t.Fatalf("bounded retirement: %d %v", n, err)
	}
	if n, err := f.repo.RetireWindows(ctx, 10_000, 2); err != nil || n != 1 {
		t.Fatalf("remaining retirement: %d %v", n, err)
	}
	final, err := f.repo.Status(ctx)
	if err != nil || final.Coverage[0].Start != 500 || final.Coverage[0].End != 500 || final.RetiredWindows != 4 || final.Generation != 4 {
		t.Fatalf("final status: %+v %v", final, err)
	}
	if n, err := f.repo.RetireWindows(ctx, 10_000, 2); err != nil || n != 0 {
		t.Fatalf("repeated retirement: %d %v", n, err)
	}
	// The stream continues from its end after everything was retired.
	last := sqlitePublication("1:2", "a", 500, 0)
	if _, err := f.publish(final.Generation, last); err != nil {
		t.Fatal(err)
	}
	if plan, err := f.repo.Select(ctx, Selection{Tenant: "1:2", Start: 0, End: 600}); err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != 500 {
		t.Fatalf("selection after full retirement: %+v %v", plan, err)
	}
}

func TestSQLiteRetirementSurvivesBackupAndImport(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	c := contract()
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	s, err := CreateSQLite(ctx, filepath.Join(root, "state"), "original", c, BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	generation := uint64(0)
	for _, source := range []string{"a", "b"} {
		for i := 0; i < 3; i++ {
			p := sqlitePublication("1:2", source, 100+int64(i)*100, 1)
			recordPublication(t, s, p)
			if generation, err = s.PublishWindow(ctx, generation, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Source a retires two windows, source b one, within one limit.
	if n, err := s.RetireWindows(ctx, 300, 3); err != nil || n != 3 {
		t.Fatalf("retire: %d %v", n, err)
	}
	if n, err := s.RetireWindows(ctx, 300, 3); err != nil || n != 1 {
		t.Fatalf("retire rest: %d %v", n, err)
	}
	before := sqliteStatus(t, s)
	if before.Coverage[0].Start != 300 || before.Coverage[1].Start != 300 || before.RetiredWindows != 4 {
		t.Fatalf("retired status: %+v", before)
	}
	if err := s.Backup(ctx, filepath.Join(root, "backup")); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSQLite(ctx, filepath.Join(root, "backup"), filepath.Join(root, "restored"), "restored")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	after := sqliteStatus(t, restored)
	before.ID = "restored"
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("restore changed retention: before=%+v after=%+v", before, after)
	}
	if counts, err := restored.UnpublishedObjects(ctx); err != nil || counts.Retired != 8 {
		t.Fatalf("restored retired keys: %+v %v", counts, err)
	}
	// A backup whose windows do not start at the recorded start is refused.
	if _, err := s.db.Exec(`UPDATE coverage SET start_ms=200 WHERE source_id='a'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(ctx, filepath.Join(root, "damaged")); err != nil {
		t.Fatal(err)
	}
	if db, err := RestoreSQLite(ctx, filepath.Join(root, "damaged"), filepath.Join(root, "restored-damaged"), "restored-damaged"); err == nil {
		db.Close()
		t.Fatal("restore accepted windows that disagree with the stream start")
	}
}

func TestPostgresImportCopiesRetention(t *testing.T) {
	ctx, dsn, _ := postgresTestDatabase(t)
	source := sqliteTestStore(t, contract())
	generation := uint64(0)
	var err error
	for i := 0; i < 3; i++ {
		p := sqlitePublication("1:2", "a", 100+int64(i)*100, 1)
		recordPublication(t, source, p)
		if generation, err = source.PublishWindow(ctx, generation, p); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := source.RetireWindows(ctx, 300, 10); err != nil || n != 2 {
		t.Fatalf("retire: %d %v", n, err)
	}
	before := sqliteStatus(t, source)
	destination, err := ImportSQLiteToPostgres(ctx, source, dsn, "retained-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	after := postgresStatus(t, ctx, destination)
	before.ID, before.SchemaVersion = "retained-cutover", PostgreSQLSchemaVersion
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("import changed retention: before=%+v after=%+v", before, after)
	}
	if counts, err := destination.UnpublishedObjects(ctx); err != nil || counts.Retired != 4 {
		t.Fatalf("imported retired keys: %+v %v", counts, err)
	}
	if n, err := destination.RetireWindows(ctx, 400, 10); err != nil || n != 1 {
		t.Fatalf("retire after import: %d %v", n, err)
	}
}
