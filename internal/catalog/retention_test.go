package catalog

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

const hourMS = int64(time.Hour / time.Millisecond)

// hourContract has one-hour windows from 1970-01-01T01:00Z, so that a test can
// choose a retention boundary from the catalog's clock with half a window of
// margin for the time the test itself takes.
func hourContract() Contract {
	return Contract{CoverageStart: hourMS, WindowMS: hourMS, DedupMS: 1, AgeMS: 2 * hourMS, LatenessMS: 60000,
		Tenants: []string{"1:2"}, Sources: []Source{{ID: "a", Fingerprint: strings.Repeat("a", 64)}}}
}

// hourPublication is the window of a stream that starts at the given hour.
func hourPublication(tenant, source string, hour int64, packs int) Publication {
	p := Publication{Tenant: tenant, SourceID: source, Start: hour * hourMS, End: (hour + 1) * hourMS, Packs: []PackRef{}}
	for i := 0; i < packs; i++ {
		key := fmt.Sprintf("tenants/%s/attempts/%s-h%d-%d", tenant, source, hour, i)
		p.Packs = append(p.Packs, PackRef{DataKey: key + ".pack", IndexKey: key + ".json", DataSHA256: strings.Repeat("a", 64), IndexSHA256: strings.Repeat("b", 64), DataBytes: 10, IndexBytes: 20})
	}
	return p
}

// retentionEnding is the retention whose boundary at the catalog's clock now
// is the end of hour, with half a window of margin either way.
func retentionEnding(now int64, hour int64) time.Duration {
	return time.Duration(now-hour*hourMS-hourMS-hourMS/2) * time.Millisecond
}

func TestRetentionBoundary(t *testing.T) {
	for _, tc := range []struct{ now, retention, window, want int64 }{
		{now: 1000, retention: 100, window: 100, want: 800},
		{now: 1099, retention: 100, window: 100, want: 800},
		{now: 1100, retention: 100, window: 100, want: 900},
		{now: 50, retention: 100, window: 100, want: -100},
	} {
		if got := RetentionBoundary(tc.now, tc.retention, tc.window); got != tc.want {
			t.Fatalf("RetentionBoundary(%d,%d,%d)=%d want %d", tc.now, tc.retention, tc.window, got, tc.want)
		}
	}
}

func TestRetireWindows(t *testing.T) {
	for name, fixture := range map[string]func(*testing.T, Contract) ledgerFixture{"sqlite": sqliteLedgerFixtureFor, "postgres": postgresLedgerFixtureFor} {
		t.Run(name, func(t *testing.T) { testRetireWindows(t, fixture(t, hourContract())) })
	}
}

func testRetireWindows(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	var publications []Publication
	for i := int64(0); i < 3; i++ {
		p := hourPublication("1:2", "a", 1+i, 1)
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
	if err != nil || before.Coverage[0].Start != hourMS || before.Coverage[0].End != 4*hourMS || before.RetiredWindows != 0 {
		t.Fatalf("initial status: %+v %v", before, err)
	}
	for _, bad := range []struct {
		retention time.Duration
		limit     int
	}{{0, 1}, {time.Microsecond, 1}, {time.Hour, 0}, {time.Hour, MaxRetiredWindows + 1}} {
		if _, err := f.repo.RetireWindows(ctx, bad.retention, bad.limit); err == nil {
			t.Fatalf("invalid retirement accepted: %+v", bad)
		}
	}
	// Nothing is retired until the boundary passes the coverage origin.
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 1), 10); err != nil || n != 0 {
		t.Fatalf("retired at the origin: %d %v", n, err)
	}
	// Only whole windows that end at or before the boundary are retired.
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 2), 10); err != nil || n != 1 {
		t.Fatalf("retire first window: %d %v", n, err)
	}
	after, err := f.repo.Status(ctx)
	if err != nil || after.Coverage[0].Start != 2*hourMS || after.Coverage[0].End != 4*hourMS || after.RetiredWindows != 1 || after.Generation != before.Generation {
		t.Fatalf("status after retirement: %+v %v", after, err)
	}
	plan, err := f.repo.Select(ctx, Selection{Tenant: "1:2", Start: 0, End: 4 * hourMS})
	if err != nil || len(plan.Windows) != 2 || plan.Windows[0].Start != 2*hourMS {
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
	next := hourPublication("1:2", "a", 4, 1)
	recordPublication(t, f.repo, next)
	if _, err := f.publish(after.Generation, next); err != nil {
		t.Fatal(err)
	}
	// Retired keys wait for MinRetiredObjectAge whatever age a caller asks for:
	// queries holding older plans may still read them.
	f.age(MinRetiredObjectAge / 2)
	if keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Millisecond, 10); err != nil || len(keys) != 0 {
		t.Fatalf("young retired keys claimed: %v %v", keys, err)
	}
	f.age(MinRetiredObjectAge)
	keys, err := f.repo.ClaimUnpublishedObjects(ctx, time.Millisecond, 10)
	sort.Strings(keys)
	if err != nil || !reflect.DeepEqual(keys, publicationKeys(publications[0])) {
		t.Fatalf("retired keys not claimed: %v %v", keys, err)
	}
	if err := f.repo.ForgetDeletedObjects(ctx, keys); err != nil {
		t.Fatal(err)
	}
	// A boundary beyond the coverage end retires everything published, in
	// bounded batches oldest first, and the start never passes the end.
	everything := retentionEnding(f.now(), 100)
	if everything <= 0 {
		t.Fatal("clock too early for the fixture")
	}
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 1000), 2); err != nil || n != 2 {
		t.Fatalf("bounded retirement: %d %v", n, err)
	}
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 1000), 2); err != nil || n != 1 {
		t.Fatalf("remaining retirement: %d %v", n, err)
	}
	final, err := f.repo.Status(ctx)
	if err != nil || final.Coverage[0].Start != 5*hourMS || final.Coverage[0].End != 5*hourMS || final.RetiredWindows != 4 || final.Generation != 4 {
		t.Fatalf("final status: %+v %v", final, err)
	}
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 1000), 2); err != nil || n != 0 {
		t.Fatalf("repeated retirement: %d %v", n, err)
	}
	// The stream continues from its end after everything was retired.
	if _, err := f.publish(final.Generation, hourPublication("1:2", "a", 5, 0)); err != nil {
		t.Fatal(err)
	}
	if plan, err := f.repo.Select(ctx, Selection{Tenant: "1:2", Start: 0, End: 6 * hourMS}); err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != 5*hourMS {
		t.Fatalf("selection after full retirement: %+v %v", plan, err)
	}
}

// Retirement is oldest first across streams, and each stream's retired windows
// continue from its start.
func TestRetireWindowsAcrossStreams(t *testing.T) {
	ctx := context.Background()
	c := hourContract()
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	f := sqliteLedgerFixtureFor(t, c)
	generation := uint64(0)
	var err error
	for _, source := range []string{"a", "b"} {
		for hour := int64(1); hour <= 3; hour++ {
			p := hourPublication("1:2", source, hour, 1)
			recordPublication(t, f.repo, p)
			if generation, err = f.publish(generation, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 3), 3); err != nil || n != 3 {
		t.Fatalf("retire: %d %v", n, err)
	}
	state, err := f.repo.Status(ctx)
	if err != nil || state.Coverage[0].Start != 3*hourMS || state.Coverage[1].Start != 2*hourMS {
		t.Fatalf("oldest first across streams: %+v %v", state.Coverage, err)
	}
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 3), 3); err != nil || n != 1 {
		t.Fatalf("retire rest: %d %v", n, err)
	}
}

func TestSQLiteRetirementSurvivesBackupAndImport(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	c := hourContract()
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	s, err := CreateSQLite(ctx, filepath.Join(root, "state"), "original", c, BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	generation := uint64(0)
	for _, source := range []string{"a", "b"} {
		for hour := int64(1); hour <= 3; hour++ {
			p := hourPublication("1:2", source, hour, 1)
			recordPublication(t, s, p)
			if generation, err = s.PublishWindow(ctx, generation, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n, err := s.RetireWindows(ctx, retentionEnding(time.Now().UnixMilli(), 3), 10); err != nil || n != 4 {
		t.Fatalf("retire: %d %v", n, err)
	}
	before := sqliteStatus(t, s)
	if before.Coverage[0].Start != 3*hourMS || before.Coverage[1].Start != 3*hourMS || before.RetiredWindows != 4 {
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
	if _, err := s.db.Exec(`UPDATE coverage SET start_ms=? WHERE source_id='a'`, 2*hourMS); err != nil {
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
	source := sqliteTestStore(t, hourContract())
	generation := uint64(0)
	var err error
	for hour := int64(1); hour <= 3; hour++ {
		p := hourPublication("1:2", "a", hour, 1)
		recordPublication(t, source, p)
		if generation, err = source.PublishWindow(ctx, generation, p); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := source.RetireWindows(ctx, retentionEnding(time.Now().UnixMilli(), 3), 10); err != nil || n != 2 {
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
	if n, err := destination.RetireWindows(ctx, retentionEnding(time.Now().UnixMilli(), 4), 10); err != nil || n != 1 {
		t.Fatalf("retire after import: %d %v", n, err)
	}
}
