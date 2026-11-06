package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func good() Config {
	return Config{Store: Store{Type: "local", Path: "ignored"}, Catalog: Catalog{Type: "local", Path: "catalog-disk", ID: "test-catalog"}, Tenants: []string{"7"}, Sources: []Source{{ID: "hot", URL: "http://localhost:8481/"}}, ArchiveAge: "2h", HotRetention: "24h", Lateness: "1h", SafetyMargin: "2h", Window: "1h", CoverageStart: 3600000, LatenessEnforced: true}
}
func TestValidateNormalizesIdentity(t *testing.T) {
	c := good()
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	if c.Tenants[0] != "7:0" || c.Sources[0].URL != "http://localhost:8481" {
		t.Fatal("identities not normalized")
	}
	a := c.Archive(3600000, 7200000)
	if a.Tenants[0] != "7:0" {
		t.Fatal("archive tenant not propagated")
	}
}
func TestInvalidConfig(t *testing.T) {
	cases := map[string]func(*Config){
		"catalog dot":                 func(c *Config) { c.Catalog.ID = "." },
		"source dotdot":               func(c *Config) { c.Sources[0].ID = ".." },
		"missing authority":           func(c *Config) { c.Catalog = Catalog{} },
		"s3 authority":                func(c *Config) { c.Catalog.Type = "s3" },
		"mixed local http":            func(c *Config) { c.Catalog.URL = "https://catalog.example" },
		"unsafe catalog id":           func(c *Config) { c.Catalog.ID = "a/b" },
		"unicode source id":           func(c *Config) { c.Sources[0].ID = "熱" },
		"same blob/catalog directory": func(c *Config) { c.Catalog.Path = "./ignored" },
		"missing blob path":           func(c *Config) { c.Store.Path = "" },
		"lateness":                    func(c *Config) { c.LatenessEnforced = false },
		"submillisecond":              func(c *Config) { c.ArchiveAge = "2h1ns" },
		"overflowsum":                 func(c *Config) { c.SafetyMargin = "2562047h"; c.Window = "2562047h" },
		"retentionbuffer":             func(c *Config) { c.HotRetention = "5h" },
		"alignment":                   func(c *Config) { c.CoverageStart++ },
		"duplicate tenant":            func(c *Config) { c.Tenants = []string{"7", "7:0"} },
		"duplicate sources":           func(c *Config) { c.Sources = append(c.Sources, Source{"other", "http://localhost:8481"}) },
		"userinfo":                    func(c *Config) { c.Sources[0].URL = "http://user:secret@localhost" },
		"source path":                 func(c *Config) { c.Sources[0].ID = "../hot" },
		"huge cache":                  func(c *Config) { c.CacheBytes = 1 << 40 },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			c := good()
			fn(&c)
			if e := c.Validate(); e == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
}
func TestValidateArchiveRange(t *testing.T) {
	c := good()
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, r := range [][2]int64{{0, 3600000}, {3600000, 3600000}, {3600001, 7200000}, {3600000, 7200001}} {
		if e := c.ValidateArchiveRange(r[0], r[1]); e == nil {
			t.Fatalf("accepted %v", r)
		}
	}
	if e := c.ValidateArchiveRange(3600000, 10800000); e != nil {
		t.Fatal(e)
	}
}
func TestReadRejectsUnknownTrailingAndOversized(t *testing.T) {
	for _, body := range []string{`{"unknown":true}`, `{} {}`, strings.Repeat(" ", (1<<20)+1)} {
		p := filepath.Join(t.TempDir(), "config.json")
		if e := os.WriteFile(p, []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := Read(p); e == nil {
			t.Fatal("accepted bad JSON config")
		}
	}
}

func TestHTTPAuthorityConfiguration(t *testing.T) {
	c := good()
	c.Catalog = Catalog{Type: "http", URL: "https://catalog.example/", ID: "prod-v2"}
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	if c.Catalog.URL != "https://catalog.example" {
		t.Fatal("URL not normalized")
	}
	for _, address := range []string{"https://secret@catalog.example", "https://catalog.example/?token=secret", "ftp://catalog.example", "https://catalog.example/prefix", "https://catalog.example/#fragment"} {
		c.Catalog.URL = address
		if e := c.Validate(); e == nil {
			t.Fatalf("accepted unsafe authority URL: %s", address)
		}
	}
}
func TestLocalAuthorityMustBeExplicitlyInitialized(t *testing.T) {
	c := good()
	c.Catalog.Path = filepath.Join(t.TempDir(), "absent")
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	if _, e := c.OpenCatalog(); e == nil {
		t.Fatal("missing authority silently created")
	}
	if _, e := os.Stat(filepath.Join(c.Catalog.Path, "catalog.json")); e == nil {
		t.Fatal("catalog state unexpectedly created")
	}
}
func TestArchiveAndReaderUseIndependentAuthority(t *testing.T) {
	c := good()
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	a := c.Archive(3600000, 7200000)
	if a.CoverageStart != c.CoverageStart {
		t.Fatal("coverage origin not propagated")
	}
	r := c.Reader(nil, nil)
	if r.DedupMS != 1 || r.CoverageStart != c.CoverageStart {
		t.Fatal("reader contract not propagated")
	}
}
