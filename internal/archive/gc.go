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
	// Retention, when positive, retires windows that ended at least Retention
	// before Now, before collecting. Retired objects are deleted once MinAge
	// has passed, so MinAge must exceed the longest query.
	Retention time.Duration
	Window    time.Duration
	Origin    int64
	Retire    catalog.Retainer
	Now       func() time.Time
}

// RetentionBoundary is the first window start that Cold retention keeps at
// now: windows that end at or before it are retired. Like the archive-age
// boundary it is a multiple of the window, which the coverage origin is too.
func RetentionBoundary(now time.Time, retention, window time.Duration) int64 {
	w := window.Milliseconds()
	return ((now.UnixMilli() - retention.Milliseconds()) / w) * w
}

const defaultRetireBatch = 64

// CollectResult counts one pass. Claimed includes keys claimed by an earlier
// pass whose deletion did not finish.
type CollectResult struct {
	Retired int `json:"retired"`
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
	if c.Retention > 0 {
		retired, err := c.retire(ctx, batches)
		out.Retired = retired
		if err != nil {
			return out, err
		}
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

// retire retires the windows below the retention boundary in bounded
// transactions. Nothing is retired before the boundary passes the origin.
func (c *Collector) retire(ctx context.Context, batches int) (int, error) {
	if c.Retire == nil || c.Now == nil || c.Window <= 0 {
		return 0, errors.New("Cold retention requires a catalog, a clock and the window")
	}
	before := RetentionBoundary(c.Now(), c.Retention, c.Window)
	if before <= c.Origin {
		return 0, nil
	}
	retired := 0
	for range batches {
		n, err := c.Retire.RetireWindows(ctx, before, defaultRetireBatch)
		retired += n
		if err != nil {
			return retired, fmt.Errorf("retire windows before %d: %w", before, err)
		}
		if n < defaultRetireBatch {
			break
		}
	}
	return retired, nil
}
