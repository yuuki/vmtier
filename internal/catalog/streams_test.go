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
	for name, fixture := range map[string]func(*testing.T) ledgerFixture{"sqlite": sqliteLedgerFixture, "postgres": postgresLedgerFixture} {
		t.Run(name, func(t *testing.T) { testAddStreams(t, fixture(t)) })
	}
}

func testAddStreams(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	adder := f.repo.(StreamAdder)
	first := sqlitePublication("1:2", "a", 100, 1)
	recordPublication(t, f.repo, first)
	if _, err := f.publish(0, first); err != nil {
		t.Fatal(err)
	}
	before, err := f.repo.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := contract()
	next.Tenants = append(next.Tenants, "3:4")
	next.Sources = append(next.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	for name, bad := range map[string]struct {
		contract Contract
		origin   int64
	}{
		"nothing new":       {contract(), 300},
		"unaligned origin":  {next, 350},
		"before the origin": {next, 0},
		"dropped tenant":    {Contract{CoverageStart: 100, WindowMS: 100, DedupMS: 1, AgeMS: 1000, LatenessMS: 100, Tenants: []string{"3:4"}, Sources: next.Sources}, 300},
		"changed window":    {Contract{CoverageStart: 200, WindowMS: 200, DedupMS: 1, AgeMS: 1000, LatenessMS: 100, Tenants: next.Tenants, Sources: next.Sources}, 400},
	} {
		if _, err := adder.AddStreams(ctx, bad.contract, bad.origin); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if after, err := f.repo.Status(ctx); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused additions changed the catalog: %+v %v", after, err)
	}
	added, err := adder.AddStreams(ctx, next, 300)
	if err != nil {
		t.Fatal(err)
	}
	want := []StreamCoverage{
		{Tenant: "1:2", SourceID: "b", Start: 300, End: 300},
		{Tenant: "3:4", SourceID: "a", Start: 300, End: 300},
		{Tenant: "3:4", SourceID: "b", Start: 300, End: 300},
	}
	if !reflect.DeepEqual(added, want) {
		t.Fatalf("added %+v, want %+v", added, want)
	}
	after, err := f.repo.Status(ctx)
	if err != nil || !EqualContract(after.Contract, next) || len(after.Coverage) != 4 || after.Generation != before.Generation {
		t.Fatalf("status after addition: %+v %v", after, err)
	}
	if _, err := adder.AddStreams(ctx, next, 300); err == nil {
		t.Fatal("repeated addition accepted")
	}
	// The existing stream keeps its windows; a new stream starts at its origin.
	if _, err := f.publish(after.Generation, sqlitePublication("3:4", "a", 200, 0)); err == nil {
		t.Fatal("new stream accepted a window before its origin")
	}
	p := sqlitePublication("3:4", "a", 300, 1)
	recordPublication(t, f.repo, p)
	if _, err := f.publish(after.Generation, p); err != nil {
		t.Fatal(err)
	}
	plan, err := f.repo.Select(ctx, Selection{Tenant: "3:4", Start: 0, End: 400})
	if err != nil || len(plan.Windows) != 1 || plan.Windows[0].Start != 300 || plan.CoverageEnd != 300 {
		t.Fatalf("new tenant plan: %+v %v", plan, err)
	}
	plan, err = f.repo.Select(ctx, Selection{Tenant: "1:2", Start: 0, End: 400})
	if err != nil || len(plan.Windows) != 1 || plan.CoverageEnd != 200 {
		t.Fatalf("existing tenant plan: %+v %v", plan, err)
	}
	// Cold retention never retires past a new stream's start.
	if n, err := f.repo.RetireWindows(ctx, 1000, 10); err != nil || n != 2 {
		t.Fatalf("retire: %d %v", n, err)
	}
	if state, err := f.repo.Status(ctx); err != nil || state.Coverage[1].Start != 300 || state.Coverage[1].End != 300 {
		t.Fatalf("status after retirement: %+v %v", state, err)
	}
}

func TestAddedStreamsSurviveBackupAndImport(t *testing.T) {
	ctx, dsn, _ := postgresTestDatabase(t)
	root := t.TempDir()
	s, err := CreateSQLite(ctx, filepath.Join(root, "state"), "original", contract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	next := contract()
	next.Tenants = append(next.Tenants, "3:4")
	if _, err := s.AddStreams(ctx, next, 500); err != nil {
		t.Fatal(err)
	}
	p := sqlitePublication("3:4", "a", 500, 1)
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
