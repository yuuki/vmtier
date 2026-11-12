package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

func runSQLiteOffline(ctx context.Context, operation string, c *config.Config) error {
	if operation != "inspect" && operation != "verify" {
		return errors.New("SQLite archive and gateway share one owner; use vmtier serve -config FILE")
	}
	db, err := openConfiguredSQLite(ctx, c)
	if err != nil {
		return err
	}
	defer db.Close()
	if operation == "inspect" {
		return printCatalogStatus(ctx, db)
	}
	return verifyRepository(ctx, c, db)
}

// PostgreSQL metadata inspection is online. A verification pass fixes its
// coverage prefix at the initial snapshot; later publications are not included.
func runPostgresMaintenance(ctx context.Context, operation string, c *config.Config) error {
	if operation != "inspect" && operation != "verify" {
		return errors.New("PostgreSQL archiving requires fenced ownership; use vmtier serve -config FILE")
	}
	db, err := openConfiguredRepository(ctx, c)
	if err != nil {
		return err
	}
	defer db.Close()
	if operation == "inspect" {
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return printCatalogStatus(readCtx, db)
	}
	return verifyRepository(ctx, c, db)
}

func verifyRepository(ctx context.Context, c *config.Config, db catalog.Repository) error {
	blobs, err := c.OpenBlobs()
	if err != nil {
		return err
	}
	if closer, ok := blobs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	state, err := db.Status(readCtx)
	cancel()
	if err != nil {
		return err
	}
	if state.Generation == 0 {
		return errors.New("no committed catalog windows to verify")
	}
	// Include source windows ahead of the minimum tenant coverage. Each plan
	// remains bounded to one logical time window during the scrub.
	// Cold retention removed the windows below each stream's start.
	starts, ends := make(map[string]int64), make(map[string]int64)
	for _, stream := range state.Coverage {
		if stream.End > ends[stream.Tenant] {
			ends[stream.Tenant] = stream.End
		}
		if start, ok := starts[stream.Tenant]; !ok || stream.Start < start {
			starts[stream.Tenant] = stream.Start
		}
	}
	for _, tenant := range state.Contract.Tenants {
		if err := verifyTenantWindows(ctx, db, blobs, state, tenant, starts[tenant], ends[tenant]); err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(state)
}

// verifyTenantWindows verifies the packs of a tenant's windows from its
// earliest retained start up to coverageEnd, selecting one window at a time.
// Windows published after the state snapshot are skipped.
func verifyTenantWindows(ctx context.Context, db catalog.Repository, blobs objectstore.BlobStore, state catalog.Status, tenant string, retainedStart, coverageEnd int64) error {
	for start := max(retainedStart, state.Contract.CoverageStart); start < coverageEnd; {
		end := start + state.Contract.WindowMS
		if end <= start {
			return errors.New("verification interval overflows")
		}
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		plan, err := db.Select(readCtx, catalog.Selection{Tenant: tenant, Start: start, End: end})
		cancel()
		if err != nil {
			return err
		}
		for _, window := range plan.Windows {
			if window.Generation > state.Generation {
				continue
			}
			for _, ref := range window.Packs {
				if err := verifyCatalogPack(ctx, blobs, window, ref); err != nil {
					return fmt.Errorf("verify tenant %s source %s window %d: %w", tenant, window.SourceID, start, err)
				}
			}
		}
		start = end
	}
	return nil
}

func verifyCatalogPack(ctx context.Context, blobs objectstore.BlobStore, window catalog.Window, ref catalog.PackRef) error {
	b, err := blobs.Get(ctx, ref.IndexKey)
	if err != nil {
		return err
	}
	if int64(len(b)) != ref.IndexBytes || pack.Sum(b) != ref.IndexSHA256 {
		return errors.New("index size/checksum mismatch")
	}
	idx, err := pack.ParseIndex(b, pack.Limits{})
	if err != nil {
		return err
	}
	if idx.Tenant.String() != window.Tenant || idx.Start != window.Start || idx.End != window.End || idx.Size != ref.DataBytes || idx.SHA256 != ref.DataSHA256 {
		return errors.New("index disagrees with catalog descriptor")
	}
	data, err := blobs.Get(ctx, ref.DataKey)
	if err != nil {
		return err
	}
	if int64(len(data)) != ref.DataBytes || pack.Sum(data) != ref.DataSHA256 {
		return errors.New("pack size/checksum mismatch")
	}
	return loadPackBlocks(idx, data)
}
