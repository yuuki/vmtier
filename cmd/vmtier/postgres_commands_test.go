package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
)

func postgresCLIConfig(t *testing.T) (context.Context, config.Config, string, string) {
	t.Helper()
	dsn := os.Getenv("VMTIER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set VMTIER_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	schema := "vmtier_cli_" + strings.ToLower(rand.Text())
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := db.ExecContext(cleanupCtx, "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("invalid test DSN")
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	c, _ := sqliteCLIConfig(t, t.TempDir(), true)
	c.Catalog = config.Catalog{Type: "postgres", ID: "postgres-cli", DSNEnv: "VMTIER_PG_CLI_TEST_DSN"}
	c.Service.MaxReplicas = 1
	c.Store.RequestsPerSecond, c.Store.MaxConcurrentRequests, c.Store.MaxQueuedRequests = 20, 2, 8
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Setenv(c.Catalog.DSNEnv, dsn)
	file := filepath.Join(t.TempDir(), "postgres.json")
	writeCLIConfig(t, file, c)
	return ctx, c, file, dsn
}

func TestPostgresCLIInitializationAndOnlineInspection(t *testing.T) {
	ctx, c, file, dsn := postgresCLIConfig(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if _, err := openConfiguredRepository(ctx, &c); err == nil {
		t.Fatal("uninitialized database opened")
	}
	init := []string{"catalog-init", "-backend", "postgres", "-config", file, "-id", c.Catalog.ID}
	if err := run(init); err != nil {
		t.Fatal(err)
	}
	if err := run(init); err == nil {
		t.Fatal("existing catalog overwritten")
	}
	db, err := catalog.OpenPostgres(ctx, dsn, c.Catalog.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lease, err := db.AcquireLease(ctx, "other-owner", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start, end := c.CoverageStart, c.CoverageStart+c.ArchiveContract().WindowMS
	if _, err := db.Fenced(lease).PublishWindow(ctx, 0, catalog.Publication{Tenant: "1:2", SourceID: "hot", Start: start, End: end}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"inspect", "-config", file},
		{"catalog-inspect", "-backend", "postgres", "-config", file},
		{"catalog-inspect", "-backend", "postgres", "-config", file, "-tenant", "1:2", "-start", "3600000", "-end", "7200000", "-max-windows", "1"},
		{"catalog-upgrade", "-backend", "postgres", "-config", file},
	} {
		if err := run(args); err != nil {
			t.Fatalf("online command %v: %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"catalog-init", "-backend", "postgres", "-config", file, "-id", "wrong"},
		{"catalog-inspect", "-backend", "postgres", "-config", file, "-id", "wrong"},
		{"catalog-backup", "-backend", "postgres", "-config", file, "-out", "unused"},
		{"catalog-restore", "-backend", "postgres", "-config", file, "-backup", "unused"},
		{"archive", "-config", file},
		{"gateway", "-config", file},
	} {
		if err := run(args); err == nil {
			t.Fatalf("unsafe command accepted: %v", args)
		}
	}
	// Empty windows have no blob references; online verification must not invent
	// a payload or require the active archiver to relinquish its lease.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	if err := run([]string{"verify", "-config", file}); err != nil {
		t.Fatal(err)
	}
	// Verification starts at the retained start once Cold retention retired
	// the oldest window.
	if _, err := db.Fenced(lease).PublishWindow(ctx, 1, catalog.Publication{Tenant: "1:2", SourceID: "hot", Start: end, End: 2*end - start}); err != nil {
		t.Fatal(err)
	}
	if n, err := db.RetireWindows(ctx, time.Duration(time.Now().UnixMilli()-end-c.ArchiveContract().WindowMS-c.ArchiveContract().WindowMS/2)*time.Millisecond, 1); err != nil || n != 1 {
		t.Fatalf("retire: %d %v", n, err)
	}
	if err := run([]string{"verify", "-config", file}); err != nil {
		t.Fatal(err)
	}
	changed := c
	changed.Store.Bucket = "different"
	if _, err := openConfiguredRepository(ctx, &changed); err == nil {
		t.Fatal("changed blob locator accepted")
	}
	changed = c
	changed.ArchiveAge = "4h"
	if err := changed.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := openConfiguredRepository(ctx, &changed); err == nil {
		t.Fatal("changed archive contract accepted")
	}
}
