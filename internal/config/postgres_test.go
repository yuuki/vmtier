package config

import (
	"strings"
	"testing"
	"time"
)

func postgresConfig() Config {
	c := good()
	c.Catalog = Catalog{Type: "postgres", ID: "pg-catalog", DSNEnv: "VMTIER_CONFIG_TEST_DSN"}
	c.Service.MaxReplicas = 1
	c.Store = Store{Type: "s3", Bucket: "test", RequestsPerSecond: 20, MaxConcurrentRequests: 2, MaxQueuedRequests: 8}
	return c
}

func TestPostgresConfigurationAndCredentials(t *testing.T) {
	c := postgresConfig()
	t.Setenv(c.Catalog.DSNEnv, "")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.ArchiveLeaseTTL() != 30*time.Second {
		t.Fatal("missing default lease duration")
	}
	if _, err := c.PostgresDSN(); err == nil {
		t.Fatal("unset connection environment accepted")
	}
	t.Setenv(c.Catalog.DSNEnv, "postgres://user:private@example.test/catalog")
	if dsn, err := c.PostgresDSN(); err != nil || !strings.HasPrefix(dsn, "postgres://") {
		t.Fatal("explicit connection environment not resolved")
	}
	if _, err := c.OpenCatalog(); err == nil {
		t.Fatal("PostgreSQL exposed as legacy whole-manifest store")
	}
	c.Service.ArchiveLeaseTTL = "3s"
	if err := c.Validate(); err != nil || c.ArchiveLeaseTTL() != 3*time.Second {
		t.Fatal("valid lease duration rejected:", err)
	}
}

func TestPostgresRejectsUnsafeDeploymentConfiguration(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"missing environment selector": func(c *Config) { c.Catalog.DSNEnv = "" },
		"literal connection string":    func(c *Config) { c.Catalog.DSNEnv = "postgres://private" },
		"mixed path":                   func(c *Config) { c.Catalog.Path = "state" },
		"mixed URL":                    func(c *Config) { c.Catalog.URL = "https://catalog.test" },
		"local blobs":                  func(c *Config) { c.Store = Store{Type: "local", Path: "objects"} },
		"implicit budget":              func(c *Config) { c.Store.RequestsPerSecond = 0 },
		"short lease":                  func(c *Config) { c.Service.ArchiveLeaseTTL = "2s" },
		"long lease":                   func(c *Config) { c.Service.ArchiveLeaseTTL = "25h" },
		"submillisecond lease":         func(c *Config) { c.Service.ArchiveLeaseTTL = "3s1ns" },
	} {
		t.Run(name, func(t *testing.T) {
			c := postgresConfig()
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("unsafe PostgreSQL configuration accepted")
			}
		})
	}
	c := good()
	c.Service.ArchiveLeaseTTL = "30s"
	if err := c.Validate(); err == nil {
		t.Fatal("lease setting silently ignored for legacy catalog")
	}
}
