package catalog

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestContractCovers(t *testing.T) {
	base := contract()
	if !ContractCovers(base, base) {
		t.Fatal("contract does not cover itself")
	}
	grown := base
	grown.Tenants = []string{"3:4", "1:2"}
	grown.Sources = append([]Source{{ID: "b", Fingerprint: strings.Repeat("b", 64)}}, base.Sources...)
	if !ContractCovers(grown, base) || ContractCovers(base, grown) {
		t.Fatal("added streams are not one-way coverage")
	}
	for name, change := range map[string]func(*Contract){
		"origin":      func(c *Contract) { c.CoverageStart += c.WindowMS },
		"window":      func(c *Contract) { c.WindowMS *= 2 },
		"age":         func(c *Contract) { c.AgeMS++ },
		"lateness":    func(c *Contract) { c.LatenessMS++ },
		"fingerprint": func(c *Contract) { c.Sources = []Source{{ID: "a", Fingerprint: strings.Repeat("c", 64)}} },
		"tenant":      func(c *Contract) { c.Tenants = []string{"3:4"} },
	} {
		changed := base
		change(&changed)
		if ContractCovers(changed, base) {
			t.Fatalf("%s change accepted", name)
		}
	}
}

func TestAddStreams(t *testing.T) {
	for name, fixture := range map[string]func(*testing.T, Contract) ledgerFixture{"sqlite": sqliteLedgerFixtureFor, "postgres": postgresLedgerFixtureFor} {
		t.Run(name, func(t *testing.T) { testAddStreams(t, fixture(t, hourContract())) })
	}
}

func testAddStreams(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	adder := f.repo.(StreamAdder)
	first := hourPublication("1:2", "a", 1, 1)
	recordPublication(t, f.repo, first)
	if _, err := f.publish(0, first); err != nil {
		t.Fatal(err)
	}
	before, err := f.repo.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := hourContract()
	next.Tenants = append(next.Tenants, "3:4")
	next.Sources = append(next.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	dropped := next
	dropped.Tenants = []string{"3:4"}
	changed := next
	changed.WindowMS, changed.CoverageStart = 2*hourMS, 2*hourMS
	for name, bad := range map[string]Contract{"nothing new": hourContract(), "dropped tenant": dropped, "changed window": changed} {
		if _, err := adder.AddStreams(ctx, bad); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if after, err := f.repo.Status(ctx); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused additions changed the catalog: %+v %v", after, err)
	}
	// New streams start at the archive-age boundary of the catalog's clock,
	// which the gateway already requires: they are complete from the start.
	low := (f.now() - 2*hourMS) / hourMS * hourMS
	added, err := adder.AddStreams(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	high := (f.now() - 2*hourMS) / hourMS * hourMS
	if len(added) != 3 {
		t.Fatalf("added %+v", added)
	}
	origin := added[0].Start
	for i, want := range [][2]string{{"1:2", "b"}, {"3:4", "a"}, {"3:4", "b"}} {
		if added[i].Tenant != want[0] || added[i].SourceID != want[1] || added[i].Start != origin || added[i].End != origin {
			t.Fatalf("added %+v", added)
		}
	}
	if origin < low || origin > high {
		t.Fatalf("origin %d outside the archive-age boundary [%d,%d]", origin, low, high)
	}
	after, err := f.repo.Status(ctx)
	if err != nil || !EqualContract(after.Contract, next) || len(after.Coverage) != 4 || after.Generation != before.Generation {
		t.Fatalf("status after addition: %+v %v", after, err)
	}
	if _, err := adder.AddStreams(ctx, next); err == nil {
		t.Fatal("repeated addition accepted")
	}
	// The existing stream keeps its windows; a new stream starts at its origin.
	hour := origin / hourMS
	if _, err := f.publish(after.Generation, hourPublication("3:4", "a", hour-1, 0)); err == nil {
		t.Fatal("new stream accepted a window before its origin")
	}
	p := hourPublication("3:4", "a", hour, 1)
	recordPublication(t, f.repo, p)
	if _, err := f.publish(after.Generation, p); err != nil {
		t.Fatal(err)
	}
	plan, err := f.repo.Select(ctx, Selection{Tenant: "3:4", Start: hourMS, End: origin + hourMS})
	if err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != origin || plan.CoverageEnd != origin {
		t.Fatalf("new tenant plan: %+v %v", plan, err)
	}
	plan, err = f.repo.Select(ctx, Selection{Tenant: "1:2", Start: hourMS, End: 3 * hourMS})
	if err != nil || len(plan.Windows) != 1 || plan.CoverageEnd != 2*hourMS {
		t.Fatalf("existing tenant plan: %+v %v", plan, err)
	}
	// Retirement leaves the new streams, younger than the boundary, alone.
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 2), 10); err != nil || n != 1 {
		t.Fatalf("retire: %d %v", n, err)
	}
	if state, err := f.repo.Status(ctx); err != nil || state.Coverage[1].Start != origin || state.Coverage[1].End != origin || state.Coverage[0].Start != 2*hourMS {
		t.Fatalf("status after retirement: %+v %v", state, err)
	}
}

func TestAddedStreamsSurviveBackupAndImport(t *testing.T) {
	ctx, dsn, _ := postgresTestDatabase(t)
	root := t.TempDir()
	s, err := CreateSQLite(ctx, filepath.Join(root, "state"), "original", hourContract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	next := hourContract()
	next.Tenants = append(next.Tenants, "3:4")
	added, err := s.AddStreams(ctx, next)
	if err != nil || len(added) != 1 {
		t.Fatalf("add: %+v %v", added, err)
	}
	p := hourPublication("3:4", "a", added[0].Start/hourMS, 1)
	recordPublication(t, s, p)
	if _, err := s.PublishWindow(ctx, 0, p); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(ctx, filepath.Join(root, "backup")); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSQLite(ctx, filepath.Join(root, "backup"), filepath.Join(root, "restored"), "restored")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	before := sqliteStatus(t, s)
	after := sqliteStatus(t, restored)
	before.ID = "restored"
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("restore: before=%+v after=%+v", before, after)
	}
	destination, err := ImportSQLiteToPostgres(ctx, s, dsn, "added-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	imported := postgresStatus(t, ctx, destination)
	before.ID, before.SchemaVersion = "added-cutover", PostgreSQLSchemaVersion
	if !reflect.DeepEqual(before, imported) {
		t.Fatalf("import: before=%+v after=%+v", before, imported)
	}
}
