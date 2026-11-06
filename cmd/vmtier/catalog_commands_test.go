package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	vmencoding "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

func sqliteCLIConfig(t *testing.T, root string, s3 bool) (config.Config, string) {
	t.Helper()
	c := config.Config{
		Store:   config.Store{Type: "local", Path: filepath.Join(root, "blobs")},
		Catalog: config.Catalog{Type: "local", Path: filepath.Join(root, "json"), ID: "source-v2"},
		Tenants: []string{"1:2"}, Sources: []config.Source{{ID: "hot", URL: "http://127.0.0.1:1"}},
		ArchiveAge: "3h", HotRetention: "240h", Lateness: "30m", SafetyMargin: "30m",
		Window: "1h", LatenessEnforced: true, CoverageStart: 3600000,
	}
	if s3 {
		c.Store = config.Store{Type: "s3", Bucket: "not-contacted", Endpoint: "http://127.0.0.1:1"}
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "config.json")
	writeCLIConfig(t, file, c)
	return c, file
}

func writeCLIConfig(t *testing.T, file string, c config.Config) {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteOfflineCommandsDoNotNeedBlobCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	root := t.TempDir()
	_, file := sqliteCLIConfig(t, root, true)
	dir := filepath.Join(root, "sqlite")
	init := []string{"catalog-init", "-backend", "sqlite", "-config", file, "-path", dir, "-id", "sqlite-1"}
	if err := run(init); err != nil {
		t.Fatal(err)
	}
	if err := run(init); err == nil {
		t.Fatal("SQLite initialization overwrote state")
	}
	if err := run([]string{"catalog-inspect", "-path", dir, "-id", "sqlite-1"}); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(root, "backup")
	if err := run([]string{"catalog-backup", "-path", dir, "-id", "sqlite-1", "-out", backup}); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(root, "restored")
	if err := run([]string{"catalog-restore", "-backup", backup, "-path", restored, "-id", "sqlite-2"}); err != nil {
		t.Fatal(err)
	}
	s, err := catalog.OpenSQLite(context.Background(), restored, "sqlite-2")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	status, err := s.Status(context.Background())
	if err != nil || status.ID != "sqlite-2" || status.Generation != 0 || status.Blob.Region != "us-east-1" {
		t.Fatalf("restored identity/contract: %+v, %v", status, err)
	}
	if err := run([]string{"catalog-inspect", "-path", restored, "-id", "sqlite-2"}); err == nil {
		t.Fatal("offline command opened an owned state directory")
	}
}

func TestSQLiteCommandsRejectMissingStateAndBadArguments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	for _, args := range [][]string{
		{"catalog-inspect", "-path", dir, "-id", "missing"},
		{"catalog-inspect", "-path", dir, "-id", "missing", "-start", "1"},
		{"catalog-inspect", "-path", dir, "-id", "missing", "-tenant", "1:2", "-start", "2", "-end", "1"},
		{"catalog-backup", "-path", dir, "-id", "missing"},
		{"catalog-restore", "-path", dir, "-id", "new"},
		{"catalog-import", "-path", dir, "-id", "new"},
		{"catalog-init", "-backend", "sqlite", "-path", dir, "-id", "new"},
		{"catalog", "-backend", "sqlite", "-path", dir, "-id", "new"},
	} {
		if err := run(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("invalid command initialized state: %v", err)
		}
	}
}

func TestSQLiteCLIImportsV2AndSelectsCommittedWindow(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	c, file := sqliteCLIConfig(t, root, false)
	legacy, err := catalog.NewLocal(c.Catalog.Path, c.Catalog.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := sqliteConfiguration(&c)
	start, end := c.CoverageStart, c.CoverageStart+contract.WindowMS
	// An empty export still has a complete native header and a verified index.
	native := vmencoding.MarshalInt64(nil, start)
	native = vmencoding.MarshalInt64(native, end-1)
	idx, data, err := pack.Build(bytes.NewReader(native), pack.Tenant{AccountID: 1, ProjectID: 2}, start, end, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := pack.MarshalIndex(idx, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := objectstore.NewLocal(c.Store.Path, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	dataKey, indexKey := "tenants/1:2/attempts/fixture/data.pack", "tenants/1:2/attempts/fixture/index.json"
	for key, body := range map[string][]byte{dataKey: data, indexKey: index} {
		if err := blobs.Put(ctx, key, body); err != nil {
			t.Fatal(err)
		}
	}
	if err := blobs.Close(); err != nil {
		t.Fatal(err)
	}
	m := catalog.Fresh(c.Catalog.ID)
	m.Contract = contract
	m.Entries = []catalog.Entry{{Tenant: "1:2", SourceID: "hot", SourceFingerprint: contract.Sources[0].Fingerprint, Start: start, End: end, DataKey: dataKey, IndexKey: indexKey, IndexSHA256: pack.Sum(index)}}
	if _, err = catalog.Commit(ctx, legacy, m, 0); err != nil {
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "sqlite")
	args := []string{"catalog-import", "-from", c.Catalog.Path, "-config", file, "-path", dest, "-id", "imported"}
	// A changed policy cannot silently reinterpret the legacy coverage.
	c.ArchiveAge = "4h"
	writeCLIConfig(t, file, c)
	if err := run(args); err == nil {
		t.Fatal("import accepted a different archive policy")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("rejected import left live state")
	}
	c.ArchiveAge = "3h"
	writeCLIConfig(t, file, c)
	if err := run(args); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"catalog-inspect", "-path", dest, "-id", "imported", "-tenant", "1:2", "-start", "3600000", "-end", "7200000", "-max-windows", "1", "-max-packs", "1"}); err != nil {
		t.Fatal(err)
	}
	s, err := catalog.OpenSQLite(ctx, dest, "imported")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	plan, err := s.Select(ctx, catalog.Selection{Tenant: "1:2", Start: start, End: end})
	if err != nil || plan.CoverageEnd != end || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 1 || plan.Windows[0].Packs[0].DataKey != dataKey {
		t.Fatalf("imported window differs: %+v, %v", plan, err)
	}
}
