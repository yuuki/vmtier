package catalog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yuuki/vmtier/internal/pack"
)

func sqliteTestStore(t *testing.T, c Contract) *SQLite {
	t.Helper()
	s, err := CreateSQLite(context.Background(), filepath.Join(t.TempDir(), "state"), "test", c, BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sqlitePublication(tenant, source string, start int64, packs int) Publication {
	p := Publication{Tenant: tenant, SourceID: source, Start: start, End: start + 100, Packs: []PackRef{}}
	for i := 0; i < packs; i++ {
		key := fmt.Sprintf("tenants/%s/attempts/%s-%d-%d", tenant, source, start, i)
		p.Packs = append(p.Packs, PackRef{DataKey: key + ".pack", IndexKey: key + ".json", DataSHA256: strings.Repeat("a", 64), IndexSHA256: strings.Repeat("b", 64), DataBytes: 10, IndexBytes: 20})
	}
	return p
}

// recordPublication records the keys of a publication as an archive attempt
// does before its uploads, so that the publication may reference them.
func recordPublication(t testing.TB, ledger ObjectLedger, p Publication) {
	t.Helper()
	var keys []string
	for _, ref := range p.Packs {
		keys = append(keys, ref.DataKey, ref.IndexKey)
	}
	if len(keys) == 0 {
		return
	}
	if err := ledger.RecordPendingObjects(context.Background(), p.Tenant, keys); err != nil {
		t.Fatal("record pending objects:", err)
	}
}

func sqliteStatus(t *testing.T, s *SQLite) SQLiteStatus {
	t.Helper()
	out, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSQLitePublicationAndRestart(t *testing.T) {
	ctx := context.Background()
	c := contract()
	c.Tenants = append(c.Tenants, "3:4")
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	s := sqliteTestStore(t, c)
	initial := sqliteStatus(t, s)
	if initial.Generation != 0 || initial.SchemaVersion != SQLiteSchemaVersion || initial.Blob.Region != "us-east-1" || len(initial.Coverage) != 4 {
		t.Fatalf("unexpected initial state: %+v", initial)
	}
	first := sqlitePublication("1:2", "a", 100, 2)
	recordPublication(t, s, first)
	if g, err := s.PublishWindow(ctx, 0, first); err != nil || g != 1 {
		t.Fatalf("publish: generation=%d err=%v", g, err)
	}
	if g, err := s.PublishWindow(ctx, 1, sqlitePublication("3:4", "a", 100, 0)); err != nil || g != 2 {
		t.Fatalf("independent tenant: generation=%d err=%v", g, err)
	}
	plan, err := s.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 200})
	if err != nil || plan.CoverageEnd != 100 || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 2 {
		t.Fatalf("incomplete source coverage: %+v, %v", plan, err)
	}
	if g, err := s.PublishWindow(ctx, 2, sqlitePublication("1:2", "b", 100, 0)); err != nil || g != 3 {
		t.Fatalf("empty window: generation=%d err=%v", g, err)
	}
	plan, err = s.Select(ctx, Selection{Tenant: "1:2", Start: 199, End: 200})
	if err != nil || plan.Generation != 3 || plan.CoverageEnd != 200 || len(plan.Windows) != 2 || len(plan.Windows[1].Packs) != 0 {
		t.Fatalf("complete snapshot: %+v, %v", plan, err)
	}
	// A response-loss replay stays idempotent even after another stream committed.
	if g, err := s.PublishWindow(ctx, 0, first); err != nil || g != 3 {
		t.Fatalf("replay: generation=%d err=%v", g, err)
	}
	changed := first
	changed.Packs = append([]PackRef(nil), first.Packs...)
	changed.Packs[0].DataSHA256 = strings.Repeat("c", 64)
	if _, err := s.PublishWindow(ctx, 0, changed); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("changed replay accepted: %v", err)
	}
	before := sqliteStatus(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(ctx, s.dir, s.id)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if after := sqliteStatus(t, reopened); !reflect.DeepEqual(before, after) {
		t.Fatalf("restart changed catalog: before=%+v after=%+v", before, after)
	}
	afterPlan, err := reopened.Select(ctx, Selection{Tenant: "1:2", Start: 199, End: 200})
	if err != nil || !reflect.DeepEqual(plan, afterPlan) {
		t.Fatalf("restart changed query plan: %+v, %v", afterPlan, err)
	}
	boundary, err := reopened.Select(ctx, Selection{Tenant: "1:2", Start: 200, End: 300})
	if err != nil || len(boundary.Windows) != 0 {
		t.Fatalf("half-open boundary selected previous window: %+v, %v", boundary, err)
	}
}

func TestSQLitePublicationFailureIsAtomic(t *testing.T) {
	s := sqliteTestStore(t, contract())
	ctx := context.Background()
	if _, err := s.db.Exec(`CREATE TRIGGER fail_second_pack BEFORE INSERT ON packs WHEN NEW.ordinal=1 BEGIN SELECT RAISE(ABORT,'injected storage error'); END`); err != nil {
		t.Fatal(err)
	}
	p := sqlitePublication("1:2", "a", 100, 2)
	recordPublication(t, s, p)
	if _, err := s.PublishWindow(ctx, 0, p); err == nil {
		t.Fatal("injected transaction failure succeeded")
	}
	state := sqliteStatus(t, s)
	if state.Generation != 0 || state.Coverage[0].End != 100 {
		t.Fatalf("failed transaction advanced coverage: %+v", state)
	}
	for _, table := range []string{"windows", "packs"} {
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("partial rows persisted in %s: n=%d err=%v", table, n, err)
		}
	}
	if counts, err := s.UnpublishedObjects(ctx); err != nil || counts.Pending != 4 {
		t.Fatalf("failed publication consumed pending records: %+v %v", counts, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_second_pack`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishWindow(ctx, 0, p); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteRejectsInvalidTransitions(t *testing.T) {
	s := sqliteTestStore(t, contract())
	ctx := context.Background()
	p := sqlitePublication("1:2", "a", 100, 1)
	for _, tc := range []struct {
		name string
		edit func(*Publication)
	}{
		{"unknown tenant", func(p *Publication) { p.Tenant = "3:4" }},
		{"unknown source", func(p *Publication) { p.SourceID = "b" }},
		{"unaligned", func(p *Publication) { p.Start++; p.End++ }},
		{"gap", func(p *Publication) { p.Start += 100; p.End += 100 }},
		{"partial window", func(p *Publication) { p.End-- }},
		{"tenant escape", func(p *Publication) { p.Packs[0].DataKey = "tenants/3:4/data" }},
		{"bad checksum", func(p *Publication) { p.Packs[0].DataSHA256 = "bad" }},
		{"negative bytes", func(p *Publication) { p.Packs[0].DataBytes = -1 }},
		{"oversized index", func(p *Publication) { p.Packs[0].IndexBytes = 16<<20 + 1 }},
		{"repeated key", func(p *Publication) { p.Packs = append(p.Packs, p.Packs[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := p
			next.Packs = append([]PackRef(nil), p.Packs...)
			tc.edit(&next)
			if _, err := s.PublishWindow(ctx, 0, next); !errors.Is(err, ErrInvalidPublication) {
				t.Fatalf("invalid publication did not fail: %v", err)
			}
			if state := sqliteStatus(t, s); state.Generation != 0 || state.Coverage[0].End != 100 {
				t.Fatalf("rejection changed state: %+v", state)
			}
		})
	}
	if _, err := s.PublishWindow(ctx, 1, p); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong generation accepted: %v", err)
	}
	if _, err := s.PublishWindow(ctx, 0, p); !errors.Is(err, ErrInvalidPublication) || !errors.Is(err, ErrUnrecordedObject) {
		t.Fatalf("unrecorded object key accepted: %v", err)
	}
	recordPublication(t, s, p)
	if _, err := s.PublishWindow(ctx, 0, p); err != nil {
		t.Fatal(err)
	}
	if counts, err := s.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{}) {
		t.Fatalf("publication left pending records: %+v %v", counts, err)
	}
	if err := s.RecordPendingObjects(ctx, "1:2", []string{p.Packs[0].DataKey}); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("published key recorded as pending: %v", err)
	}
	next := sqlitePublication("1:2", "a", 200, 1)
	next.Packs[0].IndexKey = p.Packs[0].DataKey
	if _, err := s.PublishWindow(ctx, 1, next); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("cross-column key reuse accepted: %v", err)
	}
}

func TestSQLiteConcurrentExpectedGeneration(t *testing.T) {
	c := contract()
	c.Tenants = append(c.Tenants, "3:4")
	s := sqliteTestStore(t, c)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, tenant := range c.Tenants {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			_, err := s.PublishWindow(ctx, 0, sqlitePublication(tenant, "a", 100, 0))
			errs <- err
		}(tenant)
	}
	wg.Wait()
	close(errs)
	success, conflict := 0, 0
	for err := range errs {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 || sqliteStatus(t, s).Generation != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
}

func TestSQLiteSelectionLimitsAndSnapshots(t *testing.T) {
	s := sqliteTestStore(t, contract())
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		p := sqlitePublication("1:2", "a", 100+int64(i)*100, 2)
		recordPublication(t, s, p)
		if _, err := s.PublishWindow(ctx, uint64(i), p); err != nil {
			t.Fatal(err)
		}
	}
	for _, sel := range []Selection{
		{Tenant: "1:2", Start: 100, End: 300, MaxWindows: 1},
		{Tenant: "1:2", Start: 100, End: 200, MaxPacks: 1},
		{Tenant: "1:2", Start: 100, End: 200, MaxPlanBytes: 300},
		{Tenant: "1:2", Start: 100, End: 100},
		{Tenant: "9:9", Start: 100, End: 200},
	} {
		if _, err := s.Select(ctx, sel); err == nil {
			t.Fatalf("invalid or oversized selection succeeded: %+v", sel)
		}
	}
	plan, err := s.Select(ctx, Selection{Tenant: "1:2", Start: 200, End: 300, MaxWindows: 1, MaxPacks: 2})
	if err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != 200 {
		t.Fatalf("bounded narrow query failed: %+v %v", plan, err)
	}
	if _, err = s.PublishWindow(ctx, 2, sqlitePublication("1:2", "a", 300, 0)); err != nil {
		t.Fatal(err)
	}
	if plan.Generation != 2 || plan.CoverageEnd != 300 || len(plan.Windows) != 1 {
		t.Fatalf("completed plan changed after publication: %+v", plan)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.Select(canceled, Selection{Tenant: "1:2", Start: 100, End: 200}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled selection: %v", err)
	}
}

func TestSQLiteExplicitInitializationAndOwnership(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "missing")
	if _, err := OpenSQLite(ctx, dir, "test"); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("missing state: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("opening missing state created a directory")
	}
	s, err := CreateSQLite(ctx, dir, "test", contract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = OpenSQLite(ctx, dir, "test"); err == nil {
		t.Fatal("second catalog owner accepted")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = CreateSQLite(ctx, dir, "other", contract(), BlobLocator{Type: "s3", Bucket: "test"}); err == nil {
		t.Fatal("initialization overwrote existing state")
	}
	if _, err = OpenSQLite(ctx, dir, "other"); err == nil {
		t.Fatal("wrong catalog ID accepted")
	}
	reopened, err := OpenSQLite(ctx, dir, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.db.Exec(`UPDATE catalog_state SET schema_version=schema_version+1`); err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSQLite(ctx, dir, "test"); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("newer schema accepted: %v", err)
	}
	corruptDir := t.TempDir()
	corrupt := []byte("not a SQLite database")
	if err = os.WriteFile(filepath.Join(corruptDir, SQLiteFile), corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenSQLite(ctx, corruptDir, "test"); err == nil {
		t.Fatal("corrupt state accepted")
	}
	got, err := os.ReadFile(filepath.Join(corruptDir, SQLiteFile))
	if err != nil || string(got) != string(corrupt) {
		t.Fatal("opening corrupt state overwrote it")
	}
	local, err := CreateSQLite(ctx, filepath.Join(t.TempDir(), "local"), "test", contract(), BlobLocator{Type: "local", Path: dir})
	if err != nil {
		// A different local object directory is valid, regardless of its contents.
		t.Fatal(err)
	}
	if err = local.Close(); err != nil {
		t.Fatal(err)
	}
	same := filepath.Join(t.TempDir(), "same")
	if _, err = CreateSQLite(ctx, same, "test", contract(), BlobLocator{Type: "local", Path: same}); err == nil {
		t.Fatal("same catalog/blob directory accepted")
	}
}

func TestSQLiteBeyondMVPEntryLimit(t *testing.T) {
	s := sqliteTestStore(t, contract())
	ctx := context.Background()
	// Populate a large historical fixture in one transaction. The public API
	// must still publish and select beyond the old whole-manifest entry ceiling.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO windows(tenant,source_id,start_ms,end_ms,generation,pack_count,digest) VALUES(?,?,?,?,?,0,?)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxEntries; i++ {
		p := sqlitePublication("1:2", "a", 100+int64(i)*100, 0)
		digest, err := validatePublication(contract(), p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = stmt.ExecContext(ctx, p.Tenant, p.SourceID, p.Start, p.End, i+1, digest); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	end := int64(100 + MaxEntries*100)
	if _, err = tx.ExecContext(ctx, `UPDATE coverage SET end_ms=?`, end); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE catalog_state SET generation=?`, MaxEntries); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	p := sqlitePublication("1:2", "a", end, 1)
	recordPublication(t, s, p)
	if g, err := s.PublishWindow(ctx, MaxEntries, p); err != nil || g != MaxEntries+1 {
		t.Fatalf("publication above old ceiling: generation=%d err=%v", g, err)
	}
	plan, err := s.Select(ctx, Selection{Tenant: "1:2", Start: end, End: end + 100, MaxWindows: 1, MaxPacks: 1})
	if err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != end {
		t.Fatalf("bounded selection above old ceiling: %+v %v", plan, err)
	}
}

func TestSQLiteBlobLocatorAndLegacyEmptyPack(t *testing.T) {
	got, err := NormalizeBlobLocator(BlobLocator{Type: "s3", Endpoint: "https://example.invalid/", Bucket: "test", Prefix: "metrics/"})
	if err != nil || got.Endpoint != "https://example.invalid" || got.Prefix != "metrics" || got.Region != "us-east-1" {
		t.Fatalf("normalization: %+v %v", got, err)
	}
	for _, b := range []BlobLocator{
		{Type: "s3", Bucket: "test", Endpoint: "https://user:secret@example.invalid"},
		{Type: "s3", Bucket: "test", Prefix: "../escape"},
		{Type: "s3", Bucket: "test", Prefix: "/absolute"},
		{Type: "local", Path: "/tmp/objects", Bucket: "test"},
	} {
		if _, err := NormalizeBlobLocator(b); err == nil {
			t.Fatalf("invalid locator accepted: %+v", b)
		}
	}
	s := sqliteTestStore(t, contract())
	p := sqlitePublication("1:2", "a", 100, 1)
	p.Packs[0].DataBytes = 0
	p.Packs[0].DataSHA256 = pack.Sum(nil)
	recordPublication(t, s, p)
	if _, err := s.PublishWindow(context.Background(), 0, p); err != nil {
		t.Fatalf("legacy empty data pack rejected: %v", err)
	}
}
