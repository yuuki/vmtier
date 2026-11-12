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
	start := fs.Int64("start", 0, "first window start of the new streams in epoch milliseconds (default: the next window to become eligible)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" || *start < 0 {
		return errors.New("catalog-add-streams requires -config FILE and an optional nonnegative -start")
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
	added, err := addStreams(ctx, c, db, *start, time.Now())
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
// new streams at origin, by default the next window to become eligible. An
// earlier origin makes the archiver catch up from Hot, so its first window
// must not have expired from Hot retention or its safety margin.
func addStreams(ctx context.Context, c *config.Config, db streamCatalog, origin int64, now time.Time) ([]catalog.StreamCoverage, error) {
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
	cfg := c.Archive(0, 0)
	w := cfg.Window.Milliseconds()
	if origin == 0 {
		origin = max((now.Add(-cfg.Age).UnixMilli()/w)*w, status.Contract.CoverageStart)
	}
	if origin <= now.Add(-cfg.Retention+cfg.SafetyMargin).UnixMilli() {
		return nil, fmt.Errorf("stream origin %d is no longer exportable from Hot within its retention and safety margin", origin)
	}
	return db.AddStreams(ctx, next, origin)
}
