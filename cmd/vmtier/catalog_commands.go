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
	return printSQLiteStatus(ctx, s)
}

func sqliteConfiguration(c *config.Config) (catalog.Contract, catalog.BlobLocator) {
	return c.ArchiveContract(), catalog.BlobLocator{
		Type: c.Store.Type, Path: c.Store.Path, Endpoint: c.Store.Endpoint,
		Region: c.Store.Region, Bucket: c.Store.Bucket, Prefix: c.Store.Prefix,
	}
}

func printSQLiteStatus(ctx context.Context, s *catalog.SQLite) error {
	status, err := s.Status(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(status)
}

func runCatalogMaintenance(args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	path := fs.String("path", "", "SQLite catalog directory; a fresh destination for import/restore")
	id := fs.String("id", "", "expected catalog ID, or a new ID for import/restore")
	var configuration, from, out, backup, tenant string
	var start, end int64
	var maxWindows, maxPacks int
	switch args[0] {
	case "catalog-import":
		fs.StringVar(&configuration, "config", "", "v2 source policy/blob configuration (catalog.id pins the source identity)")
		fs.StringVar(&from, "from", "", "stopped JSON v2 catalog directory")
	case "catalog-backup":
		fs.StringVar(&out, "out", "", "fresh backup directory")
	case "catalog-restore":
		fs.StringVar(&backup, "backup", "", "backup directory")
	case "catalog-inspect":
		fs.StringVar(&tenant, "tenant", "", "optional tenant for a bounded query plan")
		fs.Int64Var(&start, "start", 0, "inclusive epoch milliseconds (requires -tenant and -end)")
		fs.Int64Var(&end, "end", 0, "exclusive epoch milliseconds (requires -tenant and -start)")
		fs.IntVar(&maxWindows, "max-windows", 0, "query-plan window cap (zero uses the repository default)")
		fs.IntVar(&maxPacks, "max-packs", 0, "query-plan pack cap (zero uses the repository default)")
	default:
		return errors.New("unsupported catalog maintenance operation")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *path == "" || *id == "" {
		return errors.New("catalog maintenance requires -path and -id, with no positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if args[0] == "catalog-import" {
		if from == "" || configuration == "" {
			return errors.New("catalog-import requires -from and -config")
		}
		return importSQLite(ctx, configuration, from, *path, *id)
	}
	if args[0] == "catalog-restore" {
		if backup == "" {
			return errors.New("catalog-restore requires -backup and a new -id")
		}
		s, err := catalog.RestoreSQLite(ctx, backup, *path, *id)
		if err != nil {
			return err
		}
		defer s.Close()
		return printSQLiteStatus(ctx, s)
	}
	if args[0] == "catalog-backup" && out == "" {
		return errors.New("catalog-backup requires -out")
	}
	if args[0] == "catalog-inspect" {
		if tenant == "" && (start != 0 || end != 0 || maxWindows != 0 || maxPacks != 0) {
			return errors.New("query-plan flags require -tenant, -start and -end")
		}
		if tenant != "" && (start <= 0 || end <= start) {
			return errors.New("query plans require a positive -start and an exclusive -end greater than -start")
		}
	}
	s, err := catalog.OpenSQLite(ctx, *path, *id)
	if err != nil {
		return err
	}
	defer s.Close()
	if args[0] == "catalog-backup" {
		return s.Backup(ctx, out)
	}
	if tenant != "" {
		parsed, err := pack.ParseTenant(tenant)
		if err != nil {
			return err
		}
		plan, err := s.Select(ctx, catalog.Selection{
			Tenant: parsed.String(), Start: start, End: end, MaxWindows: maxWindows, MaxPacks: maxPacks,
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(plan)
	}
	return printSQLiteStatus(ctx, s)
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
	return printSQLiteStatus(ctx, s)
}
