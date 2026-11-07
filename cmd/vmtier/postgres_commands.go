package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/pack"
)

func initializePostgres(ctx context.Context, file, id string) error {
	c, err := postgresCommandConfig(file, id)
	if err != nil {
		return err
	}
	dsn, err := c.PostgresDSN()
	if err != nil {
		return err
	}
	blob, err := c.BlobLocator()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := catalog.CreatePostgres(ctx, dsn, c.Catalog.ID, c.ArchiveContract(), blob)
	if err != nil {
		return err
	}
	defer db.Close()
	return printCatalogStatus(ctx, db)
}

func postgresCommandConfig(file, id string) (*config.Config, error) {
	c, err := config.Read(file)
	if err != nil {
		return nil, err
	}
	if c.Catalog.Type != "postgres" || (id != "" && id != c.Catalog.ID) {
		return nil, errors.New("PostgreSQL command requires catalog.type=postgres and matching configured catalog ID")
	}
	return c, nil
}

func runPostgresCatalogCommand(operation, file, id, tenant string, start, end int64, maxWindows, maxPacks int) error {
	if operation != "catalog-inspect" && operation != "catalog-upgrade" {
		return errors.New("PostgreSQL catalog backup/restore uses native PostgreSQL backup and PITR tooling; this command does not support PostgreSQL import")
	}
	if tenant == "" && (start != 0 || end != 0 || maxWindows != 0 || maxPacks != 0) {
		return errors.New("query-plan flags require -tenant, -start and -end")
	}
	if tenant != "" && (start <= 0 || end <= start) {
		return errors.New("query plans require a positive -start and an exclusive -end greater than -start")
	}
	c, err := postgresCommandConfig(file, id)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if operation == "catalog-upgrade" {
		dsn, err := c.PostgresDSN()
		if err != nil {
			return err
		}
		if err := catalog.UpgradePostgres(ctx, dsn, c.Catalog.ID); err != nil {
			return err
		}
	}
	db, err := openConfiguredRepository(ctx, c)
	if err != nil {
		return err
	}
	defer db.Close()
	if tenant == "" {
		return printCatalogStatus(ctx, db)
	}
	parsed, err := pack.ParseTenant(tenant)
	if err != nil {
		return err
	}
	plan, err := db.Select(ctx, catalog.Selection{Tenant: parsed.String(), Start: start, End: end, MaxWindows: maxWindows, MaxPacks: maxPacks})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(plan)
}
