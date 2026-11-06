package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

type importBlobs struct {
	objects map[string][]byte
	gets    []string
}

func (b *importBlobs) Get(_ context.Context, key string) ([]byte, error) {
	b.gets = append(b.gets, key)
	v, ok := b.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}
func (*importBlobs) GetRange(context.Context, string, int64, int64) ([]byte, error) {
	return nil, errors.New("import must not scrub payload ranges")
}
func (*importBlobs) Put(context.Context, string, []byte) error {
	return errors.New("import must not write blob objects")
}

func maintenanceFixture(t *testing.T) (*Manifest, *importBlobs, BlobLocator) {
	t.Helper()
	m := Fresh("original-v2")
	m.Generation = 2
	m.Contract = contract()
	m.Contract.Tenants = []string{"1:2", "7:9"}
	blobs := &importBlobs{objects: map[string][]byte{}}
	for start := int64(100); start < 300; start += 100 {
		for _, tenantID := range m.Contract.Tenants {
			tenant, err := pack.ParseTenant(tenantID)
			if err != nil {
				t.Fatal(err)
			}
			name := pack.MetricName(tenant, map[string]string{"__name__": "imported", "job": "test"}).MarshalNoAccountIDProjectID(nil)
			var block storage.Block
			block.Init(&storage.TSID{}, []int64{start + 10, start + 90}, []int64{5, 9}, 0, 64)
			payload := block.MarshalPortable(nil)
			native := encoding.MarshalInt64(nil, start)
			native = encoding.MarshalInt64(native, start+99)
			native = encoding.MarshalUint32(native, uint32(len(name)))
			native = append(native, name...)
			native = encoding.MarshalUint32(native, uint32(len(payload)))
			native = append(native, payload...)
			idx, data, err := pack.Build(bytes.NewReader(native), tenant, start, start+100, pack.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			index, err := pack.MarshalIndex(idx, pack.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			prefix := fmt.Sprintf("tenants/%s/attempts/import-%d/", tenantID, start)
			e := Entry{Tenant: tenantID, SourceID: "a", SourceFingerprint: m.Contract.Sources[0].Fingerprint,
				Start: start, End: start + 100, DataKey: prefix + "data.pack", IndexKey: prefix + "index.json", IndexSHA256: pack.Sum(index)}
			blobs.objects[e.IndexKey], blobs.objects[e.DataKey] = index, data
			m.Entries = append(m.Entries, e)
		}
	}
	return m, blobs, BlobLocator{Type: "s3", Bucket: "test-bucket", Region: "us-east-1", Prefix: "archive"}
}

func assertNoMaintenanceDestination(t *testing.T, dest string) {
	t.Helper()
	if _, err := os.Lstat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed maintenance published destination: %v", err)
	}
	stages, err := filepath.Glob(filepath.Join(filepath.Dir(dest), ".vmtier-stage-*"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("staging directories were not cleaned: %v %v", stages, err)
	}
}

func TestSQLiteImportV2PreservesReferencesAndCoverage(t *testing.T) {
	ctx := context.Background()
	m, blobs, locator := maintenanceFixture(t)
	dest := filepath.Join(t.TempDir(), "imported")
	s, err := ImportV2(ctx, dest, "new-sqlite", m, locator, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	status, err := s.Status(ctx)
	if err != nil || status.ID != "new-sqlite" || status.Generation != 4 || !EqualContract(status.Contract, m.Contract) || status.Blob != locator {
		t.Fatalf("import status: %+v %v", status, err)
	}
	for _, tenant := range m.Contract.Tenants {
		plan, err := s.Select(ctx, Selection{Tenant: tenant, Start: 100, End: 300})
		if err != nil || plan.CoverageEnd != 300 || len(plan.Windows) != 2 {
			t.Fatalf("imported plan: %+v %v", plan, err)
		}
		for _, window := range plan.Windows {
			if len(window.Packs) != 1 {
				t.Fatalf("expected one legacy pack: %+v", window)
			}
			ref := window.Packs[0]
			if ref.DataSHA256 != pack.Sum(blobs.objects[ref.DataKey]) || ref.DataBytes != int64(len(blobs.objects[ref.DataKey])) || ref.IndexSHA256 != pack.Sum(blobs.objects[ref.IndexKey]) {
				t.Fatal("import changed object references")
			}
		}
	}
	if len(blobs.gets) != len(m.Entries) {
		t.Fatalf("unexpected import I/O: %v", blobs.gets)
	}
	for _, key := range blobs.gets {
		if !strings.HasSuffix(key, "index.json") {
			t.Fatalf("import scrubbed a historical payload: %q", key)
		}
	}
	if _, err := ImportV2(ctx, dest, "other-id", m, locator, blobs); err == nil {
		t.Fatal("import overwrote destination")
	}
}

func TestSQLiteImportV2RejectsGapsAndInvalidIndexesAtomically(t *testing.T) {
	for _, scenario := range []string{"gap", "bad-checksum", "wrong-tenant", "missing-index", "unbound", "same-id"} {
		t.Run(scenario, func(t *testing.T) {
			m, blobs, locator := maintenanceFixture(t)
			id := "import-target"
			switch scenario {
			case "gap":
				m.Entries = m.Entries[2:]
			case "bad-checksum":
				blobs.objects[m.Entries[3].IndexKey][0] ^= 1
			case "wrong-tenant":
				e := &m.Entries[3]
				idx, err := pack.ParseIndex(blobs.objects[e.IndexKey], pack.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				idx.Tenant = pack.Tenant{AccountID: 8, ProjectID: 9}
				data, err := pack.MarshalIndex(idx, pack.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				blobs.objects[e.IndexKey], e.IndexSHA256 = data, pack.Sum(data)
			case "missing-index":
				delete(blobs.objects, m.Entries[3].IndexKey)
			case "unbound":
				m = Fresh("original-v2")
			case "same-id":
				id = m.ID
			}
			dest := filepath.Join(t.TempDir(), "failed")
			if db, err := ImportV2(context.Background(), dest, id, m, locator, blobs); err == nil {
				db.Close()
				t.Fatal("invalid import succeeded")
			}
			assertNoMaintenanceDestination(t, dest)
		})
	}
}

func TestSQLiteImportV2RetainsEmptyLegacyPack(t *testing.T) {
	m, blobs, locator := maintenanceFixture(t)
	e := &m.Entries[0]
	tenant, _ := pack.ParseTenant(e.Tenant)
	native := encoding.MarshalInt64(nil, e.Start)
	native = encoding.MarshalInt64(native, e.End-1)
	idx, data, err := pack.Build(bytes.NewReader(native), tenant, e.Start, e.End, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := pack.MarshalIndex(idx, pack.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e.IndexSHA256 = pack.Sum(index)
	blobs.objects[e.IndexKey], blobs.objects[e.DataKey] = index, data
	s, err := ImportV2(context.Background(), filepath.Join(t.TempDir(), "empty-import"), "empty-sqlite", m, locator, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	plan, err := s.Select(context.Background(), Selection{Tenant: e.Tenant, Start: e.Start, End: e.End})
	if err != nil || len(plan.Windows) != 1 || len(plan.Windows[0].Packs) != 1 || plan.Windows[0].Packs[0].DataBytes != 0 {
		t.Fatalf("empty legacy pack lost: %+v %v", plan, err)
	}
}

func TestSQLiteBackupRestoreSnapshotAndCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, blobs, locator := maintenanceFixture(t)
	root := t.TempDir()
	original, err := ImportV2(ctx, filepath.Join(root, "original"), "sqlite-original", m, locator, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	before, err := original.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(root, "backup")
	if err := original.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := original.Backup(ctx, backup); err == nil {
		t.Fatal("backup overwrote an existing destination")
	}
	if _, err := original.PublishWindow(ctx, before.Generation, Publication{Tenant: "1:2", SourceID: "a", Start: 300, End: 400}); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSQLite(ctx, backup, filepath.Join(root, "restored"), "sqlite-restored")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	after, err := restored.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before.ID = "sqlite-restored"
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("restore changed snapshot: before=%+v after=%+v", before, after)
	}
	plan, err := restored.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 400})
	if err != nil || plan.CoverageEnd != 300 || len(plan.Windows) != 2 {
		t.Fatalf("backup was not a stable snapshot: %+v %v", plan, err)
	}
	// Recovery can rearchive the missing suffix independently for each stream.
	gen, err := restored.PublishWindow(ctx, after.Generation, Publication{Tenant: "1:2", SourceID: "a", Start: 300, End: 400})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.PublishWindow(ctx, gen, Publication{Tenant: "7:9", SourceID: "a", Start: 300, End: 400}); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(ctx, filepath.Join(root, "restored"), "sqlite-restored")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	status, err := reopened.Status(ctx)
	if err != nil || status.Generation != after.Generation+2 {
		t.Fatalf("restored catch-up was not durable: %+v %v", status, err)
	}
}

func TestSQLiteRestoreRejectsDamagedBackupWithoutPartialDestination(t *testing.T) {
	for _, scenario := range []string{"checksum", "metadata", "references", "same-id", "existing-destination"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			m, blobs, locator := maintenanceFixture(t)
			root := t.TempDir()
			s, err := ImportV2(ctx, filepath.Join(root, "source"), "sqlite-source", m, locator, blobs)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			backup := filepath.Join(root, "backup")
			if scenario == "references" {
				// This is structurally valid SQLite, but not a complete publication.
				if _, err := s.db.ExecContext(ctx, "UPDATE windows SET pack_count=0 WHERE id=1"); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Backup(ctx, backup); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(root, "failed")
			newID := "sqlite-target"
			switch scenario {
			case "checksum":
				f, err := os.OpenFile(filepath.Join(backup, SQLiteFile), os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte("bad"), 128); err != nil {
					t.Fatal(err)
				}
				f.Close()
			case "metadata":
				meta, err := readBackupMetadata(filepath.Join(backup, backupMetadataFile))
				if err != nil {
					t.Fatal(err)
				}
				meta.Status.Generation++
				data, _ := json.Marshal(meta)
				if err := os.WriteFile(filepath.Join(backup, backupMetadataFile), data, 0600); err != nil {
					t.Fatal(err)
				}
			case "same-id":
				newID = "sqlite-source"
			case "existing-destination":
				if err := os.Mkdir(dest, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if restored, err := RestoreSQLite(ctx, backup, dest, newID); err == nil {
				restored.Close()
				t.Fatal("invalid restore succeeded")
			}
			if scenario == "existing-destination" {
				entries, err := os.ReadDir(dest)
				if err != nil || len(entries) != 0 {
					t.Fatalf("existing directory changed: %v %v", entries, err)
				}
			} else {
				assertNoMaintenanceDestination(t, dest)
			}
		})
	}
}
