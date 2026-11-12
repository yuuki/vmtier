package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/pack"
)

// SQLite management is offline and refuses a directory owned by serve.
func initializeSQLite(ctx context.Context, file, dir, id string) error {
	c, err := config.Read(file)
	if err != nil {
		return err
	}
	contract, blob := sqliteConfiguration(c)
	s, err := catalog.CreateSQLite(ctx, dir, id, contract, blob)
	if err != nil {
		return err
	}
	defer s.Close()
	return printCatalogStatus(ctx, s)
}

func sqliteConfiguration(c *config.Config) (catalog.Contract, catalog.BlobLocator) {
	return c.ArchiveContract(), catalog.BlobLocator{
		Type: c.Store.Type, Path: c.Store.Path, Endpoint: c.Store.Endpoint,
		Region: c.Store.Region, Bucket: c.Store.Bucket, Prefix: c.Store.Prefix,
	}
}

func printCatalogStatus(ctx context.Context, s catalog.Repository) error {
	status, err := s.Status(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(status)
}

func runCatalogMaintenance(args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	backend := fs.String("backend", "sqlite", "catalog maintenance backend: sqlite or postgres")
	path := fs.String("path", "", "SQLite catalog directory; a fresh destination for import/restore")
	id := fs.String("id", "", "expected catalog ID, or a new ID for import/restore")
	var f maintenanceFlags
	if err := f.define(fs, args[0]); err != nil {
		return err
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *backend == "postgres" {
		if fs.NArg() != 0 || *path != "" || f.configuration == "" {
			return errors.New("PostgreSQL maintenance requires -backend postgres -config FILE, with no path")
		}
		return runPostgresCatalogCommand(args[0], f.configuration, *id, f.tenant, f.start, f.end, f.maxWindows, f.maxPacks)
	}
	if *backend != "sqlite" {
		return errors.New("catalog maintenance backend must be sqlite or postgres")
	}
	if fs.NArg() != 0 || *path == "" || *id == "" {
		return errors.New("catalog maintenance requires -path and -id, with no positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "catalog-upgrade":
		return upgradeSQLite(ctx, *path, *id)
	case "catalog-import":
		if f.from == "" || f.configuration == "" {
			return errors.New("catalog-import requires -from and -config")
		}
		return importSQLite(ctx, f.configuration, f.from, *path, *id)
	case "catalog-restore":
		if f.backup == "" {
			return errors.New("catalog-restore requires -backup and a new -id")
		}
		return restoreSQLite(ctx, f.backup, *path, *id)
	case "catalog-backup":
		if f.out == "" {
			return errors.New("catalog-backup requires -out")
		}
		return backupSQLite(ctx, *path, *id, f.out)
	}
	// define accepts no other operation.
	return inspectSQLite(ctx, *path, *id, f)
}

// maintenanceFlags holds the catalog maintenance flags other than -backend,
// -path and -id.
type maintenanceFlags struct {
	configuration, from, out, backup, tenant string
	start, end                               int64
	maxWindows, maxPacks                     int
}

// define registers -config and the flags of one maintenance operation.
func (f *maintenanceFlags) define(fs *flag.FlagSet, operation string) error {
	fs.StringVar(&f.configuration, "config", "", "policy/blob configuration (required for PostgreSQL and v2 import)")
	switch operation {
	case "catalog-import":
		fs.StringVar(&f.from, "from", "", "stopped JSON v2 catalog directory")
	case "catalog-backup":
		fs.StringVar(&f.out, "out", "", "fresh backup directory")
	case "catalog-restore":
		fs.StringVar(&f.backup, "backup", "", "backup directory")
	case "catalog-upgrade":
		// No flags of its own.
	case "catalog-inspect":
		fs.StringVar(&f.tenant, "tenant", "", "optional tenant for a bounded query plan")
		fs.Int64Var(&f.start, "start", 0, "inclusive epoch milliseconds (requires -tenant and -end)")
		fs.Int64Var(&f.end, "end", 0, "exclusive epoch milliseconds (requires -tenant and -start)")
		fs.IntVar(&f.maxWindows, "max-windows", 0, "query-plan window cap (zero uses the repository default)")
		fs.IntVar(&f.maxPacks, "max-packs", 0, "query-plan pack cap (zero uses the repository default)")
	default:
		return errors.New("unsupported catalog maintenance operation")
	}
	return nil
}

// upgradeSQLite upgrades a stopped catalog in place and prints its status.
func upgradeSQLite(ctx context.Context, dir, id string) error {
	if err := catalog.UpgradeSQLite(ctx, dir, id); err != nil {
		return err
	}
	s, err := catalog.OpenSQLite(ctx, dir, id)
	if err != nil {
		return err
	}
	defer s.Close()
	return printCatalogStatus(ctx, s)
}

func restoreSQLite(ctx context.Context, backup, dest, id string) error {
	s, err := catalog.RestoreSQLite(ctx, backup, dest, id)
	if err != nil {
		return err
	}
	defer s.Close()
	return printCatalogStatus(ctx, s)
}

func backupSQLite(ctx context.Context, dir, id, out string) error {
	s, err := catalog.OpenSQLite(ctx, dir, id)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.Backup(ctx, out)
}

// inspectSQLite prints the catalog status, or with -tenant, the query plan of
// [-start,-end).
func inspectSQLite(ctx context.Context, dir, id string, f maintenanceFlags) error {
	if f.tenant == "" && (f.start != 0 || f.end != 0 || f.maxWindows != 0 || f.maxPacks != 0) {
		return errors.New("query-plan flags require -tenant, -start and -end")
	}
	if f.tenant != "" && (f.start <= 0 || f.end <= f.start) {
		return errors.New("query plans require a positive -start and an exclusive -end greater than -start")
	}
	s, err := catalog.OpenSQLite(ctx, dir, id)
	if err != nil {
		return err
	}
	defer s.Close()
	if f.tenant == "" {
		return printCatalogStatus(ctx, s)
	}
	parsed, err := pack.ParseTenant(f.tenant)
	if err != nil {
		return err
	}
	plan, err := s.Select(ctx, catalog.Selection{
		Tenant: parsed.String(), Start: f.start, End: f.end, MaxWindows: f.maxWindows, MaxPacks: f.maxPacks,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(plan)
}

func importSQLite(ctx context.Context, file, from, dest, id string) error {
	c, err := config.Read(file)
	if err != nil {
		return err
	}
	// The supplied ID pins the stopped source; no HTTP source is contacted.
	legacy, err := catalog.NewLocal(from, c.Catalog.ID, false)
	if err != nil {
		return err
	}
	m, _, err := catalog.Load(ctx, legacy)
	closeErr := legacy.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	contract, blob := sqliteConfiguration(c)
	if !catalog.EqualContract(m.Contract, contract) {
		return errors.New("source catalog contract differs from configured policy; refusing import")
	}
	if id == m.ID {
		return errors.New("catalog-import requires a new catalog ID")
	}
	blobs, err := c.OpenBlobs()
	if err != nil {
		return err
	}
	if closer, ok := blobs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	s, err := catalog.ImportV2(ctx, dest, id, m, blob, blobs)
	if err != nil {
		return fmt.Errorf("import v2 catalog: %w", err)
	}
	defer s.Close()
	return printCatalogStatus(ctx, s)
}
