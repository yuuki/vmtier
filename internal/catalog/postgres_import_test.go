package catalog

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func TestPostgresImportSQLitePreservesMetadata(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	c := contract()
	c.Tenants = append(c.Tenants, "3:4")
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	source := sqliteTestStore(t, c)
	publications := []Publication{
		sqlitePublication("1:2", "a", 100, 2),
		sqlitePublication("3:4", "a", 100, 0),
		sqlitePublication("1:2", "b", 100, 0),
		sqlitePublication("1:2", "a", 200, 1),
		sqlitePublication("3:4", "b", 100, 1),
	}
	// A legacy empty pack remains a pack; it must not become a verified empty
	// interval with no references during migration.
	publications[4].Packs[0].DataBytes = 0
	for i, publication := range publications {
		recordPublication(t, source, publication)
		if _, err := source.PublishWindow(ctx, uint64(i), publication); err != nil {
			t.Fatal(err)
		}
	}
	before := sqliteStatus(t, source)
	destination, err := ImportSQLiteToPostgres(ctx, source, dsn, "postgres-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	want := before
	want.ID, want.SchemaVersion = "postgres-cutover", PostgreSQLSchemaVersion
	if got := postgresStatus(t, ctx, destination); !reflect.DeepEqual(got, want) {
		t.Fatalf("import changed catalog metadata: got=%+v want=%+v", got, want)
	}
	for _, tenant := range c.Tenants {
		selection := Selection{Tenant: tenant, Start: 100, End: 300}
		original, err := source.Select(ctx, selection)
		if err != nil {
			t.Fatal(err)
		}
		imported, err := destination.Select(ctx, selection)
		if err != nil {
			t.Fatal(err)
		}
		original.ID = "postgres-cutover"
		if !reflect.DeepEqual(imported, original) {
			t.Fatalf("import changed references for tenant %s: got=%+v want=%+v", tenant, imported, original)
		}
	}
	if got := sqliteStatus(t, source); !reflect.DeepEqual(got, before) {
		t.Fatalf("import modified source catalog: %+v", got)
	}
	if duplicate, err := OpenSQLite(ctx, source.dir, before.ID); err == nil {
		_ = duplicate.Close()
		t.Fatal("import released the source ownership lock")
	}
	var owner string
	var token int64
	if err := db.QueryRowContext(ctx, `SELECT owner,token FROM archive_lease WHERE singleton=1`).Scan(&owner, &token); err != nil || owner != "" || token != 0 {
		t.Fatalf("imported catalog has an active archiver lease: owner=%q token=%d err=%v", owner, token, err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgres(ctx, dsn, "postgres-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := postgresStatus(t, ctx, reopened); !reflect.DeepEqual(got, want) {
		t.Fatalf("imported catalog changed after reopen: %+v", got)
	}
}

func TestPostgresImportSQLiteEmptyCatalog(t *testing.T) {
	ctx, dsn, _ := postgresTestDatabase(t)
	source := sqliteTestStore(t, contract())
	destination, err := ImportSQLiteToPostgres(ctx, source, dsn, "empty-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	state := postgresStatus(t, ctx, destination)
	if state.Generation != 0 || len(state.Coverage) != 1 || state.Coverage[0].End != contract().CoverageStart {
		t.Fatalf("empty import invented coverage: %+v", state)
	}
	plan, err := destination.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 200})
	if err != nil || len(plan.Windows) != 0 {
		t.Fatalf("empty import invented windows: %+v err=%v", plan, err)
	}
}

func TestPostgresImportSQLiteRejectsIdentityAndExistingDestination(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	source := sqliteTestStore(t, contract())
	if imported, err := ImportSQLiteToPostgres(ctx, source, dsn, source.id); err == nil {
		_ = imported.Close()
		t.Fatal("import reused the old catalog identity")
	}
	assertNoPostgresImportTables(t, ctx, db)
	existing, err := CreatePostgres(ctx, dsn, "existing", contract(), BlobLocator{Type: "s3", Bucket: "other"})
	if err != nil {
		t.Fatal(err)
	}
	defer existing.Close()
	before := postgresStatus(t, ctx, existing)
	if imported, err := ImportSQLiteToPostgres(ctx, source, dsn, "new-import"); err == nil {
		_ = imported.Close()
		t.Fatal("import overwrote an initialized PostgreSQL catalog")
	}
	if after := postgresStatus(t, ctx, existing); !reflect.DeepEqual(before, after) {
		t.Fatalf("failed import changed destination: %+v", after)
	}
}

func TestPostgresImportSQLiteRejectsCorruptSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"coverage gap", `UPDATE coverage SET end_ms=300`},
		{"publication digest", `UPDATE windows SET digest='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'`},
		{"pack count", `UPDATE windows SET pack_count=2`},
		{"missing generation", `UPDATE windows SET generation=2; UPDATE catalog_state SET generation=2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, dsn, db := postgresTestDatabase(t)
			source := sqliteTestStore(t, contract())
			first := sqlitePublication("1:2", "a", 100, 1)
			recordPublication(t, source, first)
			if _, err := source.PublishWindow(ctx, 0, first); err != nil {
				t.Fatal(err)
			}
			if _, err := source.db.ExecContext(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			if imported, err := ImportSQLiteToPostgres(ctx, source, dsn, "corrupt-cutover"); err == nil {
				_ = imported.Close()
				t.Fatal("import accepted corrupt source metadata")
			}
			assertNoPostgresImportTables(t, ctx, db)
		})
	}
}

func TestPostgresImportSQLiteFailureRollsBackAllMetadata(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	source := sqliteTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 1)
	second := sqlitePublication("1:2", "a", 200, 1)
	for i, publication := range []Publication{first, second} {
		recordPublication(t, source, publication)
		if _, err := source.PublishWindow(ctx, uint64(i), publication); err != nil {
			t.Fatal(err)
		}
	}
	// SQLite's separate column constraints do not express cross-role key
	// uniqueness. Simulate damaged local state that passes per-window checks
	// and fails the destination reservation after the first window was copied.
	second.Packs[0].DataKey = first.Packs[0].IndexKey
	digest, err := validatePublication(contract(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.ExecContext(ctx, `UPDATE packs SET data_key=? WHERE window_id=(SELECT id FROM windows WHERE generation=2)`, second.Packs[0].DataKey); err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.ExecContext(ctx, `UPDATE windows SET digest=? WHERE generation=2`, digest); err != nil {
		t.Fatal(err)
	}
	if imported, err := ImportSQLiteToPostgres(ctx, source, dsn, "failed-cutover"); err == nil {
		_ = imported.Close()
		t.Fatal("import accepted a cross-role object key collision")
	}
	assertNoPostgresImportTables(t, ctx, db)
	// Correcting the source permits a fresh attempt against the same schema;
	// the failed transaction must not leave tables or key reservations behind.
	second = sqlitePublication("1:2", "a", 200, 1)
	digest, err = validatePublication(contract(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.ExecContext(ctx, `UPDATE packs SET data_key=? WHERE window_id=(SELECT id FROM windows WHERE generation=2)`, second.Packs[0].DataKey); err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.ExecContext(ctx, `UPDATE windows SET digest=? WHERE generation=2`, digest); err != nil {
		t.Fatal(err)
	}
	imported, err := ImportSQLiteToPostgres(ctx, source, dsn, "successful-cutover")
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	if state := postgresStatus(t, ctx, imported); state.Generation != 2 || state.Coverage[0].End != 300 {
		t.Fatalf("retry imported incomplete state: %+v", state)
	}
}

func TestPostgresImportSQLiteRequiresLiveOwnedSource(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	if imported, err := ImportSQLiteToPostgres(ctx, nil, dsn, "new"); err == nil {
		_ = imported.Close()
		t.Fatal("import accepted nil source")
	}
	source := sqliteTestStore(t, contract())
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if imported, err := ImportSQLiteToPostgres(ctx, source, dsn, "new"); err == nil {
		_ = imported.Close()
		t.Fatal("import accepted a closed source")
	}
	assertNoPostgresImportTables(t, ctx, db)
}

func assertNoPostgresImportTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=current_schema()`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed import exposed partial destination tables: count=%d err=%v", count, err)
	}
}
