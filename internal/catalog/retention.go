package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxRetiredWindows bounds the windows of one RetireWindows call, and so the
// size of its transaction.
const MaxRetiredWindows = 256

// Retainer applies Cold retention. RetireWindows removes, in one transaction,
// at most limit of the oldest published windows that end at or before
// RetentionBoundary of the catalog's clock, oldest start first across streams
// and in order from each stream's start, and advances each stream's start
// past them. Their object keys move to the object ledger as retired, so
// garbage collection deletes the objects once MinRetiredObjectAge has passed,
// after any query that selected them earlier has finished. A stream's start
// never moves back, and coverage end and generation are unchanged.
type Retainer interface {
	RetireWindows(ctx context.Context, retention time.Duration, limit int) (int, error)
}

// RetentionBoundary is the window end at or before which Cold retention
// retires windows at nowMS: (now - retention) / window * window with Go's
// truncating division, one window earlier. That window of margin keeps data
// younger than the retention for a gateway whose clock lags the catalog's by
// less than a window (spec/lean, retention_complete).
func RetentionBoundary(nowMS, retentionMS, windowMS int64) int64 {
	return (nowMS-retentionMS)/windowMS*windowMS - windowMS
}

func validateRetirement(retention time.Duration, limit int) error {
	if retention <= 0 || retention%time.Millisecond != 0 {
		return errors.New("retention must be a positive whole number of milliseconds")
	}
	if limit < 1 || limit > MaxRetiredWindows {
		return fmt.Errorf("retirement limit must be in [1,%d]", MaxRetiredWindows)
	}
	return nil
}

// retirementBound is the latest window start that a retirement at nowMS may
// remove, and false when the boundary has not passed the coverage origin.
func retirementBound(c Contract, nowMS int64, retention time.Duration) (int64, bool) {
	before := RetentionBoundary(nowMS, retention.Milliseconds(), c.WindowMS)
	return before - c.WindowMS, before > c.CoverageStart
}

// retiredWindow is a stored window chosen for retirement.
type retiredWindow struct {
	id               int64
	tenant, sourceID string
	start, end       int64
}

// streamRetirements groups windows chosen oldest first across streams by
// stream, checks that each group continues its stream from its start, and
// returns the streams' new starts.
func streamRetirements(coverage []StreamCoverage, windows []retiredWindow) (map[string]int64, error) {
	starts := make(map[string]int64, len(coverage))
	for _, stream := range coverage {
		starts[stream.Tenant+"\x00"+stream.SourceID] = stream.Start
	}
	changed := map[string]int64{}
	for _, w := range windows {
		key := w.tenant + "\x00" + w.sourceID
		start, ok := starts[key]
		if !ok {
			return nil, errors.New("stored window belongs to no configured stream")
		}
		next, err := nextRetirements(start, []retiredWindow{w})
		if err != nil {
			return nil, err
		}
		starts[key], changed[key] = next, next
	}
	return changed, nil
}

// nextRetirements checks that windows continue a stream from start and
// returns the stream's new start.
func nextRetirements(start int64, windows []retiredWindow) (int64, error) {
	for _, w := range windows {
		if w.start != start || w.end <= w.start {
			return 0, errors.New("stored windows are not contiguous from the stream start")
		}
		start = w.end
	}
	return start, nil
}
