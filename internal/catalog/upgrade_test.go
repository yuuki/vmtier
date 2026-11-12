package catalog

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/yuuki/vmtier/internal/pack"
)

// The DDL of the last release before the object ledger, verbatim from
// caf57c3, so that the upgrades are tested on what that release wrote rather
// than on a downgraded current catalog.
var (
	//go:embed testdata/sqlite-schema1.sql
	sqliteSchema1 string
	//go:embed testdata/postgres-schema2.sql
	postgresSchema2 string
)

// createReleasedSQLite writes a schema 1 catalog as the earlier release did,
// with one published window of one pack.
func createReleasedSQLite(t *testing.T, dir, id string, c Contract) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(dir, SQLiteFile)+"?_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c = canonicalContract(c)
	cb, _ := json.Marshal(c)
	bb, _ := json.Marshal(BlobLocator{Type: "s3", Bucket: "test", Region: "us-east-1"})
	p := hourPublication(c.Tenants[0], c.Sources[0].ID, c.CoverageStart/hourMS, 1)
	digest, err := validatePublication(c, p)
	if err != nil {
		t.Fatal(err)
	}
	statements := []struct {
		query string
		args  []any
	}{
		{sqliteSchema1, nil},
		{`INSERT INTO catalog_state VALUES(1,?,?,?,?,?,?)`, []any{1, id, pack.VMVersion, 1, string(cb), string(bb)}},
		{`INSERT INTO coverage VALUES(?,?,?)`, []any{c.Tenants[0], c.Sources[0].ID, p.End}},
		{`INSERT INTO windows(tenant,source_id,start_ms,end_ms,generation,pack_count,digest) VALUES(?,?,?,?,?,?,?)`, []any{p.Tenant, p.SourceID, p.Start, p.End, 1, 1, digest}},
		{`INSERT INTO packs VALUES(1,0,?,?,?,?,?,?)`, []any{p.Packs[0].DataKey, p.Packs[0].IndexKey, p.Packs[0].DataSHA256, p.Packs[0].IndexSHA256, p.Packs[0].DataBytes, p.Packs[0].IndexBytes}},
	}
	for _, s := range statements {
		if _, err := db.Exec(s.query, s.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// sqliteShape describes every table's columns and indexes.
func sqliteShape(t *testing.T, path string) map[string]any {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	out := map[string]any{}
	for _, table := range []string{"catalog_state", "coverage", "windows", "packs", "pending_objects"} {
		for _, pragma := range []string{"table_xinfo", "index_list"} {
			rows, err := db.Query(fmt.Sprintf("PRAGMA %s(%s)", pragma, table))
			if err != nil {
				t.Fatal(err)
			}
			columns, _ := rows.Columns()
			var all [][]any
			for rows.Next() {
				values := make([]any, len(columns))
				pointers := make([]any, len(columns))
				for i := range values {
					pointers[i] = &values[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					t.Fatal(err)
				}
				all = append(all, values)
			}
			rows.Close()
			out[table+"/"+pragma] = all
		}
	}
	return out
}

func TestSQLiteUpgradeOfReleasedSchema1(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	old := filepath.Join(root, "old")
	createReleasedSQLite(t, old, "released", hourContract())
	if s, err := OpenSQLite(ctx, old, "released"); err == nil || !strings.Contains(err.Error(), "catalog-upgrade") {
		if s != nil {
			s.Close()
		}
		t.Fatalf("schema 1 opened without an upgrade: %v", err)
	}
	status, err := UpgradeSQLite(ctx, old, "released")
	if err != nil {
		t.Fatal(err)
	}
	if status.SchemaVersion != SQLiteSchemaVersion || status.Generation != 1 || status.Coverage[0].Start != hourMS || status.Coverage[0].End != 2*hourMS {
		t.Fatalf("upgraded status: %+v", status)
	}
	fresh, err := CreateSQLite(ctx, filepath.Join(root, "fresh"), "fresh", hourContract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	fresh.Close()
	if got, want := sqliteShape(t, filepath.Join(old, SQLiteFile)), sqliteShape(t, filepath.Join(root, "fresh", SQLiteFile)); !reflect.DeepEqual(got, want) {
		t.Fatalf("upgraded schema differs from a fresh one:\nupgraded %v\nfresh    %v", got, want)
	}
	s, err := OpenSQLite(ctx, old, "released")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	next := hourPublication("1:2", "a", 2, 1)
	recordPublication(t, s, next)
	if _, err := s.PublishWindow(ctx, 1, next); err != nil {
		t.Fatal(err)
	}
}

// An upgrade refuses a damaged file before writing anything to it.
func TestSQLiteUpgradeChecksIntegrityFirst(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "old")
	createReleasedSQLite(t, dir, "released", hourContract())
	path := filepath.Join(dir, SQLiteFile)
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	var root, pageSize int64
	if err := db.QueryRow(`SELECT rootpage FROM sqlite_master WHERE name='packs'`).Scan(&root); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Claim more cells on the packs root page than it holds.
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], 500)
	if _, err := f.WriteAt(count[:], (root-1)*pageSize+3); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := UpgradeSQLite(ctx, dir, "released"); err == nil {
		t.Fatal("damaged catalog upgraded")
	}
	db, err = sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, ledgers int
	if err := db.QueryRow(`SELECT schema_version FROM catalog_state`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='pending_objects'`).Scan(&ledgers); err != nil {
		t.Fatal(err)
	}
	if version != 1 || ledgers != 0 {
		t.Fatalf("refused upgrade wrote to the damaged catalog: version=%d ledger tables=%d", version, ledgers)
	}
}

func TestNewerSchemaIsExplained(t *testing.T) {
	ctx := context.Background()
	s := sqliteTestStore(t, contract())
	if _, err := s.db.Exec(`UPDATE catalog_state SET schema_version=schema_version+1`); err != nil {
		t.Fatal(err)
	}
	dir, id := s.dir, s.id
	s.Close()
	if db, err := OpenSQLite(ctx, dir, id); err == nil || !strings.Contains(err.Error(), "newer") {
		if db != nil {
			db.Close()
		}
		t.Fatalf("newer schema: %v", err)
	}
	if _, err := UpgradeSQLite(ctx, dir, id); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("upgrade of a newer schema: %v", err)
	}
}

// createReleasedPostgres writes a schema 2 catalog as the earlier release did.
func createReleasedPostgres(t *testing.T, ctx context.Context, db *sql.DB, id string, c Contract) {
	t.Helper()
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	quoted := pq.QuoteIdentifier(schema)
	c = canonicalContract(c)
	cb, _ := json.Marshal(c)
	bb, _ := json.Marshal(BlobLocator{Type: "s3", Bucket: "test", Region: "us-east-1"})
	if _, err := db.ExecContext(ctx, strings.ReplaceAll(postgresSchema2, "%[1]s", quoted)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO `+quoted+`.catalog_state VALUES(1,2,$1,$2,0,$3,$4)`, id, pack.VMVersion, string(cb), string(bb)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO `+quoted+`.coverage VALUES($1,$2,$3)`, c.Tenants[0], c.Sources[0].ID, c.CoverageStart); err != nil {
		t.Fatal(err)
	}
}

// postgresShape describes the columns, constraints and indexes of a schema.
func postgresShape(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	var out []string
	for _, query := range []string{
		`SELECT table_name||'.'||column_name||' '||ordinal_position||' '||data_type||' '||is_nullable||' '||coalesce(column_default,'') FROM information_schema.columns WHERE table_schema=current_schema() ORDER BY 1`,
		`SELECT conrelid::regclass::text||' '||conname||' '||pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace ORDER BY 1`,
		`SELECT replace(indexdef, current_schema()||'.', '') FROM pg_indexes WHERE schemaname=current_schema() ORDER BY 1`,
	} {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			if schema := strings.Split(line, ".")[0]; strings.HasPrefix(schema, "vmtier_test_") {
				line = line[len(schema)+1:]
			}
			out = append(out, line)
		}
		rows.Close()
	}
	return out
}

func TestPostgresUpgradeOfReleasedSchema2(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	createReleasedPostgres(t, ctx, db, "released", hourContract())
	if p, err := OpenPostgres(ctx, dsn, "released"); err == nil || !strings.Contains(err.Error(), "catalog-upgrade") {
		if p != nil {
			p.Close()
		}
		t.Fatalf("schema 2 opened without an upgrade: %v", err)
	}
	if err := UpgradePostgres(ctx, dsn, "released"); err != nil {
		t.Fatal(err)
	}
	upgraded := postgresShape(t, ctx, db)
	freshCtx, freshDSN, freshDB := postgresTestDatabase(t)
	fresh, err := CreatePostgres(freshCtx, freshDSN, "fresh", hourContract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	fresh.Close()
	if want := postgresShape(t, freshCtx, freshDB); !reflect.DeepEqual(upgraded, want) {
		t.Fatalf("upgraded schema differs from a fresh one:\nupgraded %q\nfresh    %q", upgraded, want)
	}
	p, err := OpenPostgres(ctx, dsn, "released")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	first := hourPublication("1:2", "a", 1, 1)
	recordPublication(t, p, first)
	if _, err := publishPostgresTest(ctx, p, 0, first); err != nil {
		t.Fatal(err)
	}
}

// Concurrent upgrades, as from every replica, serialize instead of deadlocking.
func TestPostgresConcurrentUpgrades(t *testing.T) {
	for round := 0; round < 3; round++ {
		ctx, dsn, db := postgresTestDatabase(t)
		createReleasedPostgres(t, ctx, db, "released", hourContract())
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = UpgradePostgres(ctx, dsn, "released")
			}(i)
		}
		wg.Wait()
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("round %d: %v | %v", round, errs[0], errs[1])
		}
	}
}

// A copy of a PostgreSQL catalog (restored from an online backup, dumped into
// another schema or database, or promoted onto a new timeline) shares the
// bucket and carries the original's pending keys. It must never claim them:
// the original may still publish them.
func TestPostgresCopyNeverClaimsTheOriginalsPendingKeys(t *testing.T) {
	ctx, _, sourceDB, source := postgresTestStore(t, hourContract())
	_, _, copyDB, copied := postgresTestStore(t, hourContract())
	p := hourPublication("1:2", "a", 1, 1)
	recordPublication(t, source, p)
	var sourceSchema string
	if err := sourceDB.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&sourceSchema); err != nil {
		t.Fatal(err)
	}
	// The copy took its snapshot while the attempt was in flight.
	if _, err := copyDB.ExecContext(ctx, `INSERT INTO pending_objects SELECT * FROM `+pq.QuoteIdentifier(sourceSchema)+`.pending_objects`); err != nil {
		t.Fatal(err)
	}
	if _, err := copyDB.ExecContext(ctx, `UPDATE pending_objects SET created_ms=created_ms-$1`, (48 * time.Hour).Milliseconds()); err != nil {
		t.Fatal(err)
	}
	if _, err := publishPostgresTest(ctx, source, 0, p); err != nil {
		t.Fatal(err)
	}
	if keys, err := copied.ClaimUnpublishedObjects(ctx, time.Hour, 10); err != nil || len(keys) != 0 {
		t.Fatalf("copy claimed the original's keys: %v %v", keys, err)
	}
	if counts, err := copied.UnpublishedObjects(ctx); err != nil || counts != (UnpublishedObjectCounts{Foreign: 2}) {
		t.Fatalf("copy counts: %+v %v", counts, err)
	}
	// Its own pending keys remain collectable.
	own := hourPublication("1:2", "a", 1, 1)
	own.Packs[0].DataKey, own.Packs[0].IndexKey = "tenants/1:2/attempts/copy/x.pack", "tenants/1:2/attempts/copy/x.json"
	recordPublication(t, copied, own)
	if _, err := copyDB.ExecContext(ctx, `UPDATE pending_objects SET created_ms=created_ms-$1 WHERE object_key LIKE '%copy%'`, (48 * time.Hour).Milliseconds()); err != nil {
		t.Fatal(err)
	}
	if keys, err := copied.ClaimUnpublishedObjects(ctx, time.Hour, 10); err != nil || len(keys) != 2 {
		t.Fatalf("copy did not claim its own keys: %v %v", keys, err)
	}
}
