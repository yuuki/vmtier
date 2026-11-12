package archive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
)

// Collector deletes the objects of archive attempts that never published. It
// deletes only keys that the catalog recorded before their upload and then
// claimed, so it never touches published objects, objects of other catalogs
// sharing the bucket, or objects uploaded before the ledger existed. Its
// requests go through the same blob store, and so the same S3 budget, as the
// archiver and the read gateway.
type Collector struct {
	Store   objectstore.Deleter
	Catalog catalog.ObjectLedger
	// MinAge is how long a key stays pending before it may be claimed. It only
	// keeps the collector from discarding attempts that are still running.
	MinAge time.Duration
	// Batch bounds the keys of one claim; MaxBatches bounds a pass.
	Batch, MaxBatches int
}

// CollectResult counts one pass. Claimed includes keys claimed by an earlier
// pass whose deletion did not finish.
type CollectResult struct {
	Claimed int `json:"claimed"`
	Deleted int `json:"deleted"`
}

const (
	defaultCollectBatch      = 256
	defaultCollectMaxBatches = 16
)

// RunPass claims, deletes and forgets keys batch by batch until none are left
// or MaxBatches ran. A key whose deletion fails stays claimed and is retried
// by the next pass; it can no longer be published either way.
func (c *Collector) RunPass(ctx context.Context) (CollectResult, error) {
	var out CollectResult
	if c.Store == nil || c.Catalog == nil {
		return out, errors.New("collector requires a catalog and a blob store that can delete")
	}
	batch, batches := c.Batch, c.MaxBatches
	if batch == 0 {
		batch = defaultCollectBatch
	}
	if batches == 0 {
		batches = defaultCollectMaxBatches
	}
	if batch < 1 || batch > catalog.MaxClaimedObjects || batches < 1 {
		return out, errors.New("invalid collector batch bounds")
	}
	for range batches {
		keys, err := c.Catalog.ClaimUnpublishedObjects(ctx, c.MinAge, batch)
		if err != nil {
			return out, fmt.Errorf("claim unpublished objects: %w", err)
		}
		out.Claimed += len(keys)
		deleted := make([]string, 0, len(keys))
		var deleteErr error
		for _, key := range keys {
			if err := c.Store.Delete(ctx, key); err != nil {
				deleteErr = fmt.Errorf("delete unpublished object: %w", err)
				break
			}
			deleted = append(deleted, key)
		}
		// Forget what was deleted even after a failure, so that the next pass
		// retries only the rest. A failed forget only repeats deletions.
		if err := c.Catalog.ForgetDeletedObjects(ctx, deleted); err != nil {
			return out, errors.Join(deleteErr, fmt.Errorf("forget deleted objects: %w", err))
		}
		out.Deleted += len(deleted)
		if deleteErr != nil {
			return out, deleteErr
		}
		if len(keys) < batch {
			return out, nil
		}
	}
	return out, nil
}
