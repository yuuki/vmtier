package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
)

func TestCatalogInitializationIsExplicitAndNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority")
	if err := run([]string{"catalog", "-path", path, "-id", "test-v2"}); err == nil {
		t.Fatal("missing state silently initialized")
	}
	if err := run([]string{"catalog-init", "-path", path, "-id", "test-v2"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"catalog-init", "-path", path, "-id", "test-v2"}); err == nil {
		t.Fatal("existing catalog overwritten by initialization")
	}
	if err := run([]string{"catalog", "-path", path, "-id", "wrong-id"}); err == nil {
		t.Fatal("wrong identity opened")
	}
}

func TestInspectWorksWithoutS3Credentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	root := t.TempDir()
	path := filepath.Join(root, "authority")
	state, err := catalog.NewLocal(path, "test-v2", true)
	if err != nil {
		t.Fatal(err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Store:   config.Store{Type: "s3", Bucket: "not-contacted", Endpoint: "http://127.0.0.1:1"},
		Catalog: config.Catalog{Type: "local", Path: path, ID: "test-v2"},
		Tenants: []string{"1:2"}, Sources: []config.Source{{ID: "hot", URL: "http://127.0.0.1:2"}},
		ArchiveAge: "3h", HotRetention: "240h", Lateness: "30m", SafetyMargin: "30m", Window: "1h", LatenessEnforced: true, CoverageStart: 3600000,
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "config.json")
	if err = os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = run([]string{"inspect", "-config", file}); err != nil {
		t.Fatalf("catalog-only inspection required blob credentials: %v", err)
	}
}
