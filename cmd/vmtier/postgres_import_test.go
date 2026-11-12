package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
)

func postgresImportCLISource(t *testing.T, ctx context.Context, c config.Config) (string, *catalog.SQLite) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "sqlite-source")
	blob, err := c.BlobLocator()
	if err != nil {
		t.Fatal(err)
	}
	source, err := catalog.CreateSQLite(ctx, dir, "sqlite-source", c.ArchiveContract(), blob)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	return dir, source
}

func assertPostgresImportDestinationMissing(t *testing.T, ctx context.Context, dsn, id string) {
	t.Helper()
	db, err := catalog.OpenPostgres(ctx, dsn, id)
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, catalog.ErrNotInitialized) {
		t.Fatalf("rejected import initialized destination: %v", err)
	}
}

func TestPostgresCLIImportsStoppedSQLiteWithoutBlobAccess(t *testing.T) {
	ctx, c, file, dsn := postgresCLIConfig(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	dir, source := postgresImportCLISource(t, ctx, c)
	contract := c.ArchiveContract()
	first := catalog.Publication{
		Tenant: c.Tenants[0], SourceID: c.Sources[0].ID,
		Start: c.CoverageStart, End: c.CoverageStart + contract.WindowMS,
		Packs: []catalog.PackRef{{
			DataKey: "tenants/1:2/attempts/migrate/data.pack", IndexKey: "tenants/1:2/attempts/migrate/index.json",
			DataSHA256: strings.Repeat("a", 64), IndexSHA256: strings.Repeat("b", 64), DataBytes: 12, IndexBytes: 34,
		}},
	}
	if err := source.RecordPendingObjects(ctx, first.Tenant, []string{first.Packs[0].DataKey, first.Packs[0].IndexKey}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishWindow(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	second := catalog.Publication{Tenant: first.Tenant, SourceID: first.SourceID, Start: first.End, End: first.End + contract.WindowMS, Packs: []catalog.PackRef{}}
	if _, err := source.PublishWindow(ctx, 1, second); err != nil {
		t.Fatal(err)
	}
	before, err := source.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	selection := catalog.Selection{Tenant: first.Tenant, Start: first.Start, End: second.End}
	beforePlan, err := source.Select(ctx, selection)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"catalog-import-postgres", "-config", file, "-from", dir, "-source-id", "sqlite-source", "-timeout", "1m"}
	output, err := capturePostgresImport(t, args)
	if err != nil {
		t.Fatal(err)
	}
	var reported catalog.Status
	if err := json.Unmarshal(output, &reported); err != nil {
		t.Fatalf("import did not print JSON catalog status: %v", err)
	}
	want := before
	want.ID, want.SchemaVersion = c.Catalog.ID, catalog.PostgreSQLSchemaVersion
	if !reflect.DeepEqual(reported, want) {
		t.Fatalf("import reported different metadata: got=%+v want=%+v", reported, want)
	}
	destination, err := catalog.OpenPostgres(ctx, dsn, c.Catalog.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	plan, err := destination.Select(ctx, selection)
	if err != nil {
		t.Fatal(err)
	}
	beforePlan.ID = c.Catalog.ID
	if !reflect.DeepEqual(plan, beforePlan) {
		t.Fatalf("CLI import changed existing object references or empty windows: got=%+v want=%+v", plan, beforePlan)
	}
	// The command releases its offline lock on return and leaves the retained
	// source unchanged. It does not restart the old archiver.
	retained, err := catalog.OpenSQLite(ctx, dir, "sqlite-source")
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	after, err := retained.Status(ctx)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("CLI import changed retained SQLite catalog: %+v err=%v", after, err)
	}
}

func TestPostgresCLIImportRefusesLiveSQLiteOwner(t *testing.T) {
	ctx, c, file, dsn := postgresCLIConfig(t)
	dir, _ := postgresImportCLISource(t, ctx, c)
	if err := runPostgresImport([]string{"catalog-import-postgres", "-config", file, "-from", dir, "-source-id", "sqlite-source"}); err == nil || !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("import did not reject live SQLite owner: %v", err)
	}
	assertPostgresImportDestinationMissing(t, ctx, dsn, c.Catalog.ID)
}

func TestPostgresCLIImportRejectsMismatchBeforeInitialization(t *testing.T) {
	for _, name := range []string{"policy", "blob locator", "source identity", "same identity"} {
		t.Run(name, func(t *testing.T) {
			ctx, c, file, dsn := postgresCLIConfig(t)
			dir, source := postgresImportCLISource(t, ctx, c)
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			sourceID := "sqlite-source"
			switch name {
			case "policy":
				c.ArchiveAge = "4h"
			case "blob locator":
				c.Store.Bucket = "different-object-location"
			case "source identity":
				sourceID = "wrong-source"
			case "same identity":
				c.Catalog.ID = sourceID
			}
			writeCLIConfig(t, file, c)
			if err := runPostgresImport([]string{"catalog-import-postgres", "-config", file, "-from", dir, "-source-id", sourceID}); err == nil {
				t.Fatalf("import accepted %s mismatch", name)
			}
			assertPostgresImportDestinationMissing(t, ctx, dsn, c.Catalog.ID)
		})
	}
}

func TestPostgresCLIImportRequiresExplicitBoundedArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"catalog-import-postgres"},
		{"catalog-import-postgres", "-config", "missing", "-from", "missing"},
		{"catalog-import-postgres", "-config", "missing", "-source-id", "source"},
		{"catalog-import-postgres", "-config", "missing", "-from", "missing", "-source-id", "source", "extra"},
		{"catalog-import-postgres", "-config", "missing", "-from", "missing", "-source-id", "source", "-timeout", "0"},
		{"catalog-import-postgres", "-config", "missing", "-from", "missing", "-source-id", "source", "-timeout", "-1s"},
		{"catalog-import-postgres", "-config", "missing", "-from", "missing", "-source-id", "source", "-timeout", "25h"},
	} {
		if err := runPostgresImport(args); err == nil {
			t.Fatalf("import accepted arguments %v", args)
		}
	}
	_, file := sqliteCLIConfig(t, t.TempDir(), false)
	if err := runPostgresImport([]string{"catalog-import-postgres", "-config", file, "-from", "missing", "-source-id", "source"}); err == nil || !strings.Contains(err.Error(), "catalog.type=postgres") {
		t.Fatalf("import accepted a non-PostgreSQL configuration: %v", err)
	}
}

func capturePostgresImport(t *testing.T, args []string) ([]byte, error) {
	t.Helper()
	output, err := os.CreateTemp(t.TempDir(), "import-output")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	previous := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = previous }()
	runErr := run(args)
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	return data, runErr
}
