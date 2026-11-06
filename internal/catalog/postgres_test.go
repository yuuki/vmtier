package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
)

// PostgreSQL tests require an explicitly selected disposable database. Each test
// creates and removes only its own random schema; the caller's schema is unused.
func postgresTestDatabase(t *testing.T) (context.Context, string, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("VMTIER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set VMTIER_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal("open PostgreSQL test connection:", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatal("connect to PostgreSQL test database:", err)
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "vmtier_test_" + hex.EncodeToString(nonce[:])
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE"); err != nil {
			t.Error("remove PostgreSQL test schema:", err)
		}
	})
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("invalid PostgreSQL test connection URL")
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		// The generated schema uses only letters, digits, and underscores, so
		// it is also a valid unquoted lib/pq connection-string value.
		dsn += " search_path=" + schema
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal("open isolated PostgreSQL test connection:", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return ctx, dsn, db
}

func postgresTestStore(t *testing.T, c Contract) (context.Context, string, *sql.DB, *Postgres) {
	t.Helper()
	ctx, dsn, db := postgresTestDatabase(t)
	s, err := CreatePostgres(ctx, dsn, "test", c, BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return ctx, dsn, db, s
}

func postgresStatus(t *testing.T, ctx context.Context, s *Postgres) Status {
	t.Helper()
	state, err := s.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestPostgresExplicitInitialization(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	if s, err := OpenPostgres(ctx, dsn, "test"); !errors.Is(err, ErrNotInitialized) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("opening missing state: %v", err)
	}
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema()`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("open created state: tables=%d err=%v", tables, err)
	}
	s, err := CreatePostgres(ctx, dsn, "test", contract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	before := postgresStatus(t, ctx, s)
	if before.Generation != 0 || before.Blob.Region != "us-east-1" || before.ID != "test" || before.SchemaVersion != PostgreSQLSchemaVersion {
		t.Fatalf("unexpected initialized state: %+v", before)
	}
	if duplicate, err := CreatePostgres(ctx, dsn, "replacement", contract(), BlobLocator{Type: "s3", Bucket: "other"}); err == nil {
		_ = duplicate.Close()
		t.Fatal("initialization replaced existing state")
	}
	if after := postgresStatus(t, ctx, s); !reflect.DeepEqual(before, after) {
		t.Fatalf("duplicate initialization changed state: %+v", after)
	}
	if wrong, err := OpenPostgres(ctx, dsn, "wrong-id"); err == nil {
		_ = wrong.Close()
		t.Fatal("wrong catalog ID accepted")
	}
	if _, err := db.ExecContext(ctx, `UPDATE catalog_state SET schema_version=schema_version+1`); err != nil {
		t.Fatal(err)
	}
	if newer, err := OpenPostgres(ctx, dsn, "test"); err == nil || !strings.Contains(err.Error(), "schema") {
		if newer != nil {
			_ = newer.Close()
		}
		t.Fatalf("unsupported schema accepted: %v", err)
	}
}

func TestPostgresInitializationFailureIsAtomic(t *testing.T) {
	ctx, dsn, db := postgresTestDatabase(t)
	// Fail after initialization has created some of its tables. The existing
	// table must survive and none of the new catalog tables may persist.
	if _, err := db.ExecContext(ctx, `CREATE TABLE packs(marker TEXT); INSERT INTO packs VALUES('preserve')`); err != nil {
		t.Fatal(err)
	}
	if s, err := CreatePostgres(ctx, dsn, "test", contract(), BlobLocator{Type: "s3", Bucket: "test"}); err == nil {
		_ = s.Close()
		t.Fatal("initialization replaced a conflicting table")
	}
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema()`).Scan(&tables); err != nil || tables != 1 {
		t.Fatalf("failed initialization left partial tables: count=%d err=%v", tables, err)
	}
	var marker string
	if err := db.QueryRowContext(ctx, `SELECT marker FROM packs`).Scan(&marker); err != nil || marker != "preserve" {
		t.Fatalf("initialization changed existing table: marker=%q err=%v", marker, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE packs`); err != nil {
		t.Fatal(err)
	}
	s, err := CreatePostgres(ctx, dsn, "test", contract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal("retry after rolled-back initialization:", err)
	}
	defer s.Close()
}

func TestPostgresPublicationReplayAndRestart(t *testing.T) {
	c := contract()
	c.Tenants = append(c.Tenants, "3:4")
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	ctx, dsn, _, s := postgresTestStore(t, c)
	first := sqlitePublication("1:2", "a", 100, 2)
	if g, err := s.PublishWindow(ctx, 0, first); err != nil || g != 1 {
		t.Fatalf("publish: generation=%d err=%v", g, err)
	}
	if g, err := s.PublishWindow(ctx, 1, sqlitePublication("3:4", "a", 100, 0)); err != nil || g != 2 {
		t.Fatalf("independent tenant: generation=%d err=%v", g, err)
	}
	plan, err := s.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 200})
	if err != nil || plan.CoverageEnd != 100 || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 2 {
		t.Fatalf("incomplete source coverage: %+v err=%v", plan, err)
	}
	if g, err := s.PublishWindow(ctx, 2, sqlitePublication("1:2", "b", 100, 0)); err != nil || g != 3 {
		t.Fatalf("empty window: generation=%d err=%v", g, err)
	}
	plan, err = s.Select(ctx, Selection{Tenant: "1:2", Start: 199, End: 200})
	if err != nil || plan.Generation != 3 || plan.CoverageEnd != 200 || len(plan.Windows) != 2 || len(plan.Windows[1].Packs) != 0 {
		t.Fatalf("complete snapshot: %+v err=%v", plan, err)
	}
	// A lost response is retried with the original publication, even when
	// another stream has since advanced the catalog's generation.
	if g, err := s.PublishWindow(ctx, 0, first); err != nil || g != 3 {
		t.Fatalf("replay after a later commit: generation=%d err=%v", g, err)
	}
	changed := first
	changed.Packs = append([]PackRef(nil), first.Packs...)
	changed.Packs[0].DataSHA256 = strings.Repeat("c", 64)
	if _, err := s.PublishWindow(ctx, 0, changed); !errors.Is(err, ErrInvalidPublication) {
		t.Fatalf("different committed publication accepted: %v", err)
	}
	before := postgresStatus(t, ctx, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if after := postgresStatus(t, ctx, reopened); !reflect.DeepEqual(before, after) {
		t.Fatalf("restart changed state: before=%+v after=%+v", before, after)
	}
	afterPlan, err := reopened.Select(ctx, Selection{Tenant: "1:2", Start: 199, End: 200})
	if err != nil || !reflect.DeepEqual(plan, afterPlan) {
		t.Fatalf("restart changed plan: %+v err=%v", afterPlan, err)
	}
	boundary, err := reopened.Select(ctx, Selection{Tenant: "1:2", Start: 200, End: 300})
	if err != nil || len(boundary.Windows) != 0 {
		t.Fatalf("half-open boundary included preceding window: %+v err=%v", boundary, err)
	}
}

func TestPostgresPublicationFailureIsAtomic(t *testing.T) {
	ctx, _, db, s := postgresTestStore(t, contract())
	// The first pack can be inserted, then the second fails in the database.
	// The window, every pack, key reservations, coverage, and generation must
	// all roll back; a subsequent retry must be able to use the same keys.
	if _, err := db.ExecContext(ctx, `ALTER TABLE packs ADD CONSTRAINT injected_storage_failure CHECK (ordinal <> 1)`); err != nil {
		t.Fatal(err)
	}
	p := sqlitePublication("1:2", "a", 100, 2)
	if _, err := s.PublishWindow(ctx, 0, p); err == nil {
		t.Fatal("injected storage failure succeeded")
	}
	state := postgresStatus(t, ctx, s)
	if state.Generation != 0 || state.Coverage[0].End != 100 {
		t.Fatalf("failed transaction advanced coverage: %+v", state)
	}
	for _, table := range []string{"windows", "packs", "object_keys"} {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+pq.QuoteIdentifier(table)).Scan(&n); err != nil || n != 0 {
			t.Fatalf("partial rows persisted in %s: n=%d err=%v", table, n, err)
		}
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE packs DROP CONSTRAINT injected_storage_failure`); err != nil {
		t.Fatal(err)
	}
	if g, err := s.PublishWindow(ctx, 0, p); err != nil || g != 1 {
		t.Fatalf("retry after rollback: generation=%d err=%v", g, err)
	}
}

func TestPostgresRejectsInvalidTransitions(t *testing.T) {
	ctx, _, _, s := postgresTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 1)
	if _, err := s.PublishWindow(ctx, 1, first); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale generation accepted: %v", err)
	}
	if _, err := s.PublishWindow(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	before := postgresStatus(t, ctx, s)
	for _, tc := range []struct {
		name string
		edit func(*Publication)
	}{
		{"gap", func(p *Publication) { p.Start += 100; p.End += 100 }},
		{"unaligned", func(p *Publication) { p.Start++; p.End++ }},
		{"unknown tenant", func(p *Publication) { p.Tenant = "3:4" }},
		{"unknown source", func(p *Publication) { p.SourceID = "b" }},
		{"partial window", func(p *Publication) { p.End-- }},
		{"bad checksum", func(p *Publication) { p.Packs[0].DataSHA256 = "bad" }},
		{"reused data key", func(p *Publication) { p.Packs[0].DataKey = first.Packs[0].DataKey }},
		{"reused index key", func(p *Publication) { p.Packs[0].IndexKey = first.Packs[0].IndexKey }},
		{"data key used as index", func(p *Publication) { p.Packs[0].IndexKey = first.Packs[0].DataKey }},
		{"index key used as data", func(p *Publication) { p.Packs[0].DataKey = first.Packs[0].IndexKey }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := sqlitePublication("1:2", "a", 200, 1)
			tc.edit(&p)
			if _, err := s.PublishWindow(ctx, 1, p); !errors.Is(err, ErrInvalidPublication) {
				t.Fatalf("invalid transition accepted: %v", err)
			}
			if after := postgresStatus(t, ctx, s); !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected transition changed state: %+v", after)
			}
		})
	}
}

func TestPostgresIndependentWriters(t *testing.T) {
	for _, identical := range []bool{false, true} {
		t.Run(fmt.Sprintf("identical=%v", identical), func(t *testing.T) {
			c := contract()
			c.Tenants = append(c.Tenants, "3:4")
			ctx, dsn, _, first := postgresTestStore(t, c)
			second, err := OpenPostgres(ctx, dsn, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			start := make(chan struct{})
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			for i, s := range []*Postgres{first, second} {
				p := sqlitePublication(c.Tenants[i], "a", 100, 1)
				if identical {
					p = sqlitePublication("1:2", "a", 100, 1)
				}
				wg.Add(1)
				go func(s *Postgres, p Publication) {
					defer wg.Done()
					<-start
					_, err := s.PublishWindow(ctx, 0, p)
					errs <- err
				}(s, p)
			}
			close(start)
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
			wantSuccess, wantConflict := 1, 1
			if identical {
				wantSuccess, wantConflict = 2, 0
			}
			if success != wantSuccess || conflict != wantConflict || postgresStatus(t, ctx, first).Generation != 1 {
				t.Fatalf("success=%d conflict=%d, want success=%d conflict=%d and one commit", success, conflict, wantSuccess, wantConflict)
			}
		})
	}
}

func TestPostgresSelectionBoundsAndSnapshots(t *testing.T) {
	ctx, _, _, s := postgresTestStore(t, contract())
	for i := 0; i < 2; i++ {
		if _, err := s.PublishWindow(ctx, uint64(i), sqlitePublication("1:2", "a", 100+int64(i)*100, 2)); err != nil {
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
		t.Fatalf("bounded narrow query failed: %+v err=%v", plan, err)
	}
	if _, err := s.PublishWindow(ctx, 2, sqlitePublication("1:2", "a", 300, 0)); err != nil {
		t.Fatal(err)
	}
	if plan.Generation != 2 || plan.CoverageEnd != 300 || len(plan.Windows) != 1 {
		t.Fatalf("completed plan changed after publication: %+v", plan)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Select(canceled, Selection{Tenant: "1:2", Start: 100, End: 200}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled selection: %v", err)
	}
}

func TestPostgresSnapshotAcrossPublicationBoundary(t *testing.T) {
	ctx, _, db, reader := postgresTestStore(t, contract())
	writer, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	// Hold the reference table so Select reads identity and coverage, then
	// pauses before fetching windows. Commit a complete empty publication
	// at that exact boundary; a READ COMMITTED reader would mix snapshots.
	if _, err := writer.ExecContext(ctx, `LOCK TABLE windows IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	type result struct {
		plan QueryPlan
		err  error
	}
	done := make(chan result, 1)
	go func() {
		plan, err := reader.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 200})
		done <- result{plan, err}
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := db.QueryRowContext(waitCtx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE relation='windows'::regclass
			AND mode='AccessShareLock' AND NOT granted
		)`).Scan(&waiting)
		if err != nil {
			_ = writer.Rollback()
			<-done
			t.Fatal("wait for the selection's reference read:", err)
		}
		if waiting {
			break
		}
		select {
		case early := <-done:
			t.Fatalf("selection did not wait for reference lock: %+v err=%v", early.plan, early.err)
		case <-waitCtx.Done():
			_ = writer.Rollback()
			<-done
			t.Fatal("selection never reached the reference table")
		case <-ticker.C:
		}
	}
	publication := sqlitePublication("1:2", "a", 100, 0)
	digest, err := validatePublication(contract(), publication)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `INSERT INTO windows(tenant,source_id,start_ms,end_ms,generation,pack_count,digest) VALUES('1:2','a',100,200,1,0,$1)`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `UPDATE coverage SET end_ms=200 WHERE tenant='1:2' AND source_id='a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `UPDATE catalog_state SET generation=1`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	selected := <-done
	if selected.err != nil || selected.plan.Generation != 0 || selected.plan.CoverageEnd != 100 || len(selected.plan.Windows) != 0 {
		t.Fatalf("selection mixed publication snapshots: %+v err=%v", selected.plan, selected.err)
	}
	latest, err := reader.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 200})
	if err != nil || latest.Generation != 1 || latest.CoverageEnd != 200 || len(latest.Windows) != 1 || len(latest.Windows[0].Packs) != 0 {
		t.Fatalf("later selection did not see committed empty window: %+v err=%v", latest, err)
	}
}
