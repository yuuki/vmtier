package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
)

func runPostgresImport(args []string) error {
	if len(args) == 0 {
		return errors.New("catalog-import-postgres requires command arguments")
	}
	fs := flag.NewFlagSet("catalog-import-postgres", flag.ContinueOnError)
	file := fs.String("config", "", "PostgreSQL configuration with a new catalog ID")
	from := fs.String("from", "", "stopped SQLite catalog directory")
	sourceID := fs.String("source-id", "", "expected SQLite catalog ID")
	timeout := fs.Duration("timeout", 30*time.Minute, "maximum metadata import duration (positive, at most 24h)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" || *from == "" || *sourceID == "" {
		return errors.New("catalog-import-postgres requires -config FILE -from SQLITE_DIRECTORY -source-id ID, with no positional arguments")
	}
	if *timeout <= 0 || *timeout > 24*time.Hour {
		return errors.New("catalog-import-postgres timeout must be positive and at most 24h")
	}
	c, err := config.Read(*file)
	if err != nil {
		return err
	}
	if c.Catalog.Type != "postgres" {
		return errors.New("catalog-import-postgres requires catalog.type=postgres")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	// Opening the source takes the same lifetime lock as serve. The source
	// remains owned until validation, metadata commit, and inspection finish.
	source, err := catalog.OpenSQLite(ctx, *from, *sourceID)
	if err != nil {
		return err
	}
	defer source.Close()
	state, err := source.Status(ctx)
	if err != nil {
		return err
	}
	if !catalog.EqualContract(state.Contract, c.ArchiveContract()) {
		return errors.New("SQLite source catalog contract differs from configured PostgreSQL policy; refusing import")
	}
	blob, err := c.BlobLocator()
	if err != nil {
		return err
	}
	if state.Blob != blob {
		return errors.New("SQLite source blob locator differs from configured PostgreSQL blob locator; refusing import")
	}
	dsn, err := c.PostgresDSN()
	if err != nil {
		return err
	}
	// This command never opens the blob store: it preserves and verifies
	// catalog references, not historical payload bytes.
	destination, err := catalog.ImportSQLiteToPostgres(ctx, source, dsn, c.Catalog.ID)
	if err != nil {
		return err
	}
	defer destination.Close()
	return printCatalogStatus(ctx, destination)
}
