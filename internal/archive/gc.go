package archive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
)

// Collector deletes the objects of archive attempts that never published and,
// with Cold retention, of windows retired from the catalog. It deletes only
// keys that the catalog recorded and then claimed: never a published object,
// never an object uploaded before the ledger existed, and never a pending key
// of another catalog sharing the bucket (see ADR 0006 for PostgreSQL copies).
// Its requests go through the same blob store, and so the same S3 budget, as
// the archiver and the read gateway.
type Collector struct {
	Store   objectstore.Deleter
	Catalog catalog.ObjectLedger
	// MinAge is how long a key stays pending before it may be claimed. It only
	// keeps the collector from discarding attempts that are still running.
	// Retired keys also wait at least catalog.MinRetiredObjectAge.
	MinAge time.Duration
	// Batch bounds the keys of one claim and the windows of one retirement;
	// MaxBatches bounds the claims and retirements of a pass.
	Batch, MaxBatches int
	// Retention, when positive, retires the windows older than Retention by
	// the catalog's clock (catalog.RetentionBoundary) before collecting.
	Retention time.Duration
	Retire    catalog.Retainer
}

// CollectResult counts one pass. Claimed includes keys claimed by an earlier
// pass whose deletion did not finish. More reports that the pass stopped at
// its bounds with work left, so the caller should run another soon.
type CollectResult struct {
	Retired int  `json:"retired"`
	Claimed int  `json:"claimed"`
	Deleted int  `json:"deleted"`
	More    bool `json:"more"`
}

const (
	defaultCollectBatch      = 256
	defaultCollectMaxBatches = 16
	defaultRetireBatch       = 64
)

// RunPass retires, then claims, deletes and forgets keys batch by batch until
// none are left or MaxBatches ran. A key whose deletion fails stays claimed and
// is deferred behind the other claimed keys, so it can never be published and
// cannot block the rest; the pass reports the failure. A batch in which every
// deletion fails ends the pass, since the store is then failing as a whole.
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
		retired, more, err := c.retire(ctx, batches)
		out.Retired, out.More = retired, more
		if err != nil {
			return out, err
		}
	}
	var failures []error
	failedBefore := map[string]bool{}
	for i := range batches {
		keys, err := c.Catalog.ClaimUnpublishedObjects(ctx, c.MinAge, batch)
		if err != nil {
			return out, errors.Join(append(failures, fmt.Errorf("claim unpublished objects: %w", err))...)
		}
		out.Claimed += len(keys)
		deleted := make([]string, 0, len(keys))
		var failed []string
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				failures = append(failures, err)
				break
			}
			// A key that failed earlier in this pass waits for the next one.
			if failedBefore[key] {
				failed = append(failed, key)
				continue
			}
			if err := c.Store.Delete(ctx, key); err != nil {
				failed, failedBefore[key] = append(failed, key), true
				if len(failures) < 8 {
					failures = append(failures, fmt.Errorf("delete unpublished object %s: %w", key, err))
				}
				continue
			}
			deleted = append(deleted, key)
		}
		// Forget what was deleted, so that the next pass retries only the
		// rest; a failed forget only repeats deletions.
		if err := c.Catalog.ForgetDeletedObjects(ctx, deleted); err != nil {
			return out, errors.Join(append(failures, fmt.Errorf("forget deleted objects: %w", err))...)
		}
		out.Deleted += len(deleted)
		if err := c.Catalog.DeferClaimedObjects(ctx, failed); err != nil {
			return out, errors.Join(append(failures, fmt.Errorf("defer undeleted objects: %w", err))...)
		}
		if ctx.Err() != nil || (len(deleted) == 0 && len(failed) > 0) {
			return out, errors.Join(failures...)
		}
		if len(keys) < batch {
			return out, errors.Join(failures...)
		}
		if i == batches-1 {
			out.More = true
		}
	}
	return out, errors.Join(failures...)
}

// retire retires windows below the retention boundary in bounded
// transactions, and reports whether the bound left windows to retire.
func (c *Collector) retire(ctx context.Context, batches int) (int, bool, error) {
	if c.Retire == nil {
		return 0, false, errors.New("Cold retention requires a catalog that retires windows")
	}
	retired := 0
	for range batches {
		n, err := c.Retire.RetireWindows(ctx, c.Retention, defaultRetireBatch)
		retired += n
		if err != nil {
			return retired, false, fmt.Errorf("retire windows older than %s: %w", c.Retention, err)
		}
		if n < defaultRetireBatch {
			return retired, false, nil
		}
	}
	return retired, true, nil
}
