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
	batch, batches, err := c.bounds()
	if err != nil {
		return out, err
	}
	if c.Retention > 0 {
		retired, more, err := c.retire(ctx, batches)
		out.Retired, out.More = retired, more
		if err != nil {
			return out, err
		}
	}
	pass := deletePass{failedBefore: map[string]bool{}}
	for i := range batches {
		keys, err := c.Catalog.ClaimUnpublishedObjects(ctx, c.MinAge, batch)
		if err != nil {
			return out, pass.fail("claim unpublished objects", err)
		}
		out.Claimed += len(keys)
		deleted, failed := c.deleteClaimed(ctx, keys, &pass)
		// Forget what was deleted, so that the next pass retries only the
		// rest; a failed forget only repeats deletions.
		if err := c.Catalog.ForgetDeletedObjects(ctx, deleted); err != nil {
			return out, pass.fail("forget deleted objects", err)
		}
		out.Deleted += len(deleted)
		if err := c.Catalog.DeferClaimedObjects(ctx, failed); err != nil {
			return out, pass.fail("defer undeleted objects", err)
		}
		if ctx.Err() != nil || len(keys) < batch || (len(deleted) == 0 && len(failed) > 0) {
			return out, errors.Join(pass.failures...)
		}
		if i == batches-1 {
			out.More = true
		}
	}
	return out, errors.Join(pass.failures...)
}

// bounds returns the keys of one claim and the claims of one pass.
func (c *Collector) bounds() (int, int, error) {
	if c.Store == nil || c.Catalog == nil {
		return 0, 0, errors.New("collector requires a catalog and a blob store that can delete")
	}
	batch, batches := c.Batch, c.MaxBatches
	if batch == 0 {
		batch = defaultCollectBatch
	}
	if batches == 0 {
		batches = defaultCollectMaxBatches
	}
	if batch < 1 || batch > catalog.MaxClaimedObjects || batches < 1 {
		return 0, 0, errors.New("invalid collector batch bounds")
	}
	return batch, batches, nil
}

// maxReportedDeleteFailures bounds the errors one pass reports.
const maxReportedDeleteFailures = 8

// deletePass holds what one pass learned across its batches: the errors to
// report and the keys whose deletion already failed.
type deletePass struct {
	failures     []error
	failedBefore map[string]bool
}

// fail joins a catalog error that ends the pass to the errors seen before it.
func (p *deletePass) fail(step string, err error) error {
	return errors.Join(append(p.failures, fmt.Errorf("%s: %w", step, err))...)
}

// deleteClaimed deletes the objects of claimed keys and returns the keys it
// deleted and those that failed. It stops early when ctx ends; the keys it did
// not reach stay claimed for the next pass.
func (c *Collector) deleteClaimed(ctx context.Context, keys []string, p *deletePass) (deleted, failed []string) {
	deleted = make([]string, 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			p.failures = append(p.failures, err)
			break
		}
		// A key that failed earlier in this pass waits for the next one.
		if p.failedBefore[key] {
			failed = append(failed, key)
			continue
		}
		if err := c.Store.Delete(ctx, key); err != nil {
			failed, p.failedBefore[key] = append(failed, key), true
			if len(p.failures) < maxReportedDeleteFailures {
				p.failures = append(p.failures, fmt.Errorf("delete unpublished object %s: %w", key, err))
			}
			continue
		}
		deleted = append(deleted, key)
	}
	return deleted, failed
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
