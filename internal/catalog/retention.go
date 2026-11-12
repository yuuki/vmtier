package catalog

import (
	"context"
	"errors"
	"fmt"
)

// MaxRetiredWindows bounds the windows of one RetireWindows call, and so the
// size of its transaction.
const MaxRetiredWindows = 256

// Retainer applies Cold retention. RetireWindows removes, in one transaction,
// at most limit of the oldest published windows that end at or before before,
// in order from each stream's start, and advances the stream's start past
// them. Their object keys move to the object ledger as retired, so garbage
// collection deletes the objects once the ledger's minimum age has passed,
// after any query that selected them earlier has finished. A stream's start
// never moves back, and coverage end and generation are unchanged.
type Retainer interface {
	RetireWindows(ctx context.Context, before int64, limit int) (int, error)
}

func validateRetirement(before int64, limit int) error {
	if before < 0 {
		return errors.New("retention boundary must be nonnegative")
	}
	if limit < 1 || limit > MaxRetiredWindows {
		return fmt.Errorf("retirement limit must be in [1,%d]", MaxRetiredWindows)
	}
	return nil
}

// retiredWindow is a stored window chosen for retirement.
type retiredWindow struct {
	id         int64
	start, end int64
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
