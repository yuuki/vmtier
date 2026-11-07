package main

import (
	"context"
	"errors"
	"fmt"

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
		return printSQLiteStatus(ctx, db)
	}
	blobs, err := c.OpenBlobs()
	if err != nil {
		return err
	}
	if closer, ok := blobs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	state, err := db.Status(ctx)
	if err != nil {
		return err
	}
	if state.Generation == 0 {
		return errors.New("no committed SQLite windows to verify")
	}
	// Include source windows ahead of the minimum tenant coverage. Each plan
	// remains bounded to one logical time window during the offline scrub.
	ends := make(map[string]int64)
	for _, stream := range state.Coverage {
		if stream.End > ends[stream.Tenant] {
			ends[stream.Tenant] = stream.End
		}
	}
	for _, tenant := range state.Contract.Tenants {
		if err := verifySQLiteTenant(ctx, db, blobs, state.Contract, tenant, ends[tenant]); err != nil {
			return err
		}
	}
	return printSQLiteStatus(ctx, db)
}

// verifySQLiteTenant verifies the packs of a tenant's windows from the coverage
// start up to coverageEnd, selecting one window at a time.
func verifySQLiteTenant(ctx context.Context, db *catalog.SQLite, blobs objectstore.BlobStore, contract catalog.Contract, tenant string, coverageEnd int64) error {
	for start := contract.CoverageStart; start < coverageEnd; {
		end := start + contract.WindowMS
		if end <= start {
			return errors.New("verification interval overflows")
		}
		plan, err := db.Select(ctx, catalog.Selection{Tenant: tenant, Start: start, End: end})
		if err != nil {
			return err
		}
		for _, window := range plan.Windows {
			for _, ref := range window.Packs {
				if err := verifySQLitePack(ctx, blobs, window, ref); err != nil {
					return fmt.Errorf("verify tenant %s source %s window %d: %w", tenant, window.SourceID, start, err)
				}
			}
		}
		start = end
	}
	return nil
}

func verifySQLitePack(ctx context.Context, blobs objectstore.BlobStore, window catalog.Window, ref catalog.PackRef) error {
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
		return errors.New("index disagrees with SQLite descriptor")
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
