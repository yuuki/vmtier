package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
)

// runAddStreams adds the tenants and sources of -config that its catalog lacks.
// A SQLite catalog must be offline (its directory lock refuses a live serve).
// A PostgreSQL catalog changes online: processes still configured without the
// new streams keep serving and archiving the streams they know.
func runAddStreams(args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	file := fs.String("config", "", "configuration listing the existing and the new tenants and sources")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" {
		return errors.New("catalog-add-streams requires -config FILE")
	}
	c, err := config.Read(*file)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := openCatalogForStreams(ctx, c)
	if err != nil {
		return err
	}
	defer db.Close()
	added, err := addStreams(ctx, c, db)
	if err != nil {
		return err
	}
	status, err := db.Status(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Added  []catalog.StreamCoverage `json:"added"`
		Status catalog.Status           `json:"status"`
	}{added, status})
}

// streamCatalog is a repository that can add streams.
type streamCatalog interface {
	catalog.Repository
	catalog.StreamAdder
}

// openCatalogForStreams opens the configured catalog without requiring the
// configuration's contract, which is about to grow, to match it.
func openCatalogForStreams(ctx context.Context, c *config.Config) (streamCatalog, error) {
	switch c.Catalog.Type {
	case "sqlite":
		db, err := catalog.OpenSQLite(ctx, c.Catalog.Path, c.Catalog.ID)
		if err != nil {
			return nil, err
		}
		return db, nil
	case "postgres":
		dsn, err := c.PostgresDSN()
		if err != nil {
			return nil, err
		}
		db, err := catalog.OpenPostgres(ctx, dsn, c.Catalog.ID)
		if err != nil {
			return nil, err
		}
		return db, nil
	}
	return nil, errors.New("catalog-add-streams requires catalog.type=sqlite or postgres")
}

// addStreams checks the configuration against the stored catalog and adds its
// new streams. The catalog starts them at the archive-age boundary of its own
// clock, which the gateway already requires, so they need no catch-up.
func addStreams(ctx context.Context, c *config.Config, db streamCatalog) ([]catalog.StreamCoverage, error) {
	status, err := db.Status(ctx)
	if err != nil {
		return nil, err
	}
	blob, err := c.BlobLocator()
	if err != nil {
		return nil, err
	}
	if blob != status.Blob {
		return nil, errors.New("catalog blob locator differs from configuration")
	}
	next := c.ArchiveContract()
	if !catalog.ContractCovers(next, status.Contract) {
		return nil, errors.New("configuration must keep the timing contract and every tenant and source of the catalog")
	}
	return db.AddStreams(ctx, next)
}
