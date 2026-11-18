package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/pack"
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

// slowFirstRead delays the first catalog read of a command.
type slowFirstRead struct {
	catalog.Store
	delay time.Duration
	done  bool
}

func (c *slowFirstRead) Read(ctx context.Context) (*catalog.Manifest, error) {
	if !c.done {
		c.done = true
		time.Sleep(c.delay)
	}
	return c.Store.Read(ctx)
}

// The archive command reads the catalog to choose its range before it
// archives. That read must count as elapsed time in the retention checks.
func TestArchiveCommandCountsRangeLookupAgainstRetention(t *testing.T) {
	c, _ := sqliteCLIConfig(t, t.TempDir(), false)
	now := time.Now()
	hour := time.Hour.Milliseconds()
	// One window is pending, and it leaves the safety margin 100ms from now.
	c.ArchiveAge = "1h"
	c.CoverageStart = (now.Add(-time.Hour).UnixMilli()/hour)*hour - hour
	c.HotRetention = (now.Sub(time.UnixMilli(c.CoverageStart)) + 30*time.Minute + 100*time.Millisecond).Truncate(time.Millisecond).String()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	blobs, err := c.OpenBlobs()
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := blobs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	local, err := catalog.NewLocal(c.Catalog.Path, c.Catalog.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	err = runArchive(context.Background(), &c, blobs, &slowFirstRead{Store: local, delay: 250 * time.Millisecond}, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "safety margin") {
		t.Fatalf("window archived after its safety margin elapsed during the range lookup: %v", err)
	}
}

// The JSON v2 gateway offers the same load balancer check as serve.
func TestGatewayServingCheckIgnoresArchiveLag(t *testing.T) {
	ctx := context.Background()
	c, _ := sqliteCLIConfig(t, t.TempDir(), false)
	blobs, err := c.OpenBlobs()
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := blobs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	local, err := catalog.NewLocal(c.Catalog.Path, c.Catalog.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	reader := c.Reader(blobs, local)
	mux := gatewayMux(&c, blobs, reader, gateway.New(reader, c.GatewayLimits()))
	status := func(path string) int {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder.Code
	}
	// No archive has committed: the catalog check of a query fails.
	if got := status("/serving"); got != http.StatusServiceUnavailable {
		t.Fatalf("/serving on an unbound catalog: status %d", got)
	}
	// One committed window at the coverage origin: coverage is far behind.
	m := catalog.Fresh(c.Catalog.ID)
	m.Contract = c.ArchiveContract()
	m.Entries = []catalog.Entry{{Tenant: "1:2", SourceID: "hot", SourceFingerprint: m.Contract.Sources[0].Fingerprint, Start: c.CoverageStart, End: c.CoverageStart + time.Hour.Milliseconds(), DataKey: "tenants/1:2/attempts/fixture/data.pack", IndexKey: "tenants/1:2/attempts/fixture/index.json", IndexSHA256: pack.Sum(nil)}}
	if _, err := catalog.Commit(ctx, local, m, 0); err != nil {
		t.Fatal(err)
	}
	if got := status("/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("/ready with missing coverage: status %d", got)
	}
	if got := status("/serving"); got != http.StatusOK {
		t.Fatalf("/serving with missing coverage: status %d", got)
	}
}
