package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
)

func TestSQLiteCLIAddStreams(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	ctx := context.Background()
	root := t.TempDir()
	c, file := sqliteCLIConfig(t, root, true)
	c.Catalog.Type, c.Catalog.Path, c.Catalog.ID = "sqlite", filepath.Join(root, "sqlite"), "sqlite-1"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	writeCLIConfig(t, file, c)
	if err := run([]string{"catalog-init", "-backend", "sqlite", "-config", file, "-path", c.Catalog.Path, "-id", c.Catalog.ID}); err != nil {
		t.Fatal(err)
	}
	grown := c
	grown.Tenants = []string{"1:2", "3:4"}
	grown.Sources = append([]config.Source{{ID: "cold-b", URL: "http://127.0.0.1:2"}}, c.Sources...)
	if err := grown.Validate(); err != nil {
		t.Fatal(err)
	}
	grownFile := filepath.Join(root, "grown.json")
	writeCLIConfig(t, grownFile, grown)
	for _, args := range [][]string{
		{"catalog-add-streams"},
		{"catalog-add-streams", "-config", grownFile, "-start", "-1"},
		{"catalog-add-streams", "-config", file},
		// The coverage origin of 1970 expired from Hot long ago.
		{"catalog-add-streams", "-config", grownFile, "-start", "3600000"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	shrunk := grown
	shrunk.Tenants = []string{"3:4"}
	if err := shrunk.Validate(); err != nil {
		t.Fatal(err)
	}
	shrunkFile := filepath.Join(root, "shrunk.json")
	writeCLIConfig(t, shrunkFile, shrunk)
	if err := run([]string{"catalog-add-streams", "-config", shrunkFile}); err == nil {
		t.Fatal("configuration without an existing tenant accepted")
	}
	if err := run([]string{"catalog-add-streams", "-config", grownFile}); err != nil {
		t.Fatal(err)
	}
	db, err := catalog.OpenSQLite(ctx, c.Catalog.Path, c.Catalog.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	status, err := db.Status(ctx)
	if err != nil || len(status.Coverage) != 4 {
		t.Fatalf("status after addition: %+v %v", status, err)
	}
	w := time.Hour.Milliseconds()
	eligible := (time.Now().Add(-3*time.Hour).UnixMilli() / w) * w
	for _, stream := range status.Coverage {
		added := stream.Tenant == "3:4" || stream.SourceID == "cold-b"
		if added && (stream.Start != stream.End || stream.Start < eligible-w || stream.Start > eligible) {
			t.Fatalf("added stream does not start at the eligible end %d: %+v", eligible, stream)
		}
		if !added && stream.Start != c.CoverageStart {
			t.Fatalf("existing stream changed: %+v", stream)
		}
	}
	// Both the earlier and the new configuration open the catalog.
	for _, cfg := range []config.Config{c, grown} {
		if err := validateConfiguredRepository(ctx, &cfg, db); err != nil {
			t.Fatalf("configuration %v refused: %v", cfg.Tenants, err)
		}
	}
	if err := validateConfiguredRepository(ctx, &shrunk, db); err != nil {
		t.Fatalf("subset configuration refused: %v", err)
	}
}
