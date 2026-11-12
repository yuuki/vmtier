package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Object keys pass through two states before they are forgotten. An archive
// attempt records each key as pending before its PUT. Publication consumes the
// pending record of every key it references, in the same transaction that makes
// the window visible. Garbage collection claims old pending keys by moving them
// to deleting, deletes their objects, then forgets them. Publication and a claim
// of the same key are serialized by the catalog, so a claimed key can never be
// published and a published key can never be claimed. Safety therefore depends
// on neither clocks, leases nor object listings: the age a key must reach before
// it is claimed only keeps garbage collection from discarding running attempts.
const (
	pendingState  = "pending"
	retiredState  = "retired"
	deletingState = "deleting"
)

// schemaVersionError explains a catalog of another schema: an older one needs
// catalog-upgrade, a newer one a newer release.
func schemaVersionError(stored, supported int) error {
	if stored > supported {
		return fmt.Errorf("catalog schema %d is newer than schema %d of this release; use a release that supports it", stored, supported)
	}
	return fmt.Errorf("unsupported catalog schema %d; explicit catalog-upgrade to schema %d or migration required", stored, supported)
}

// validLedgerState reports whether a stored ledger state is known. Retired keys
// belonged to windows that Cold retention removed from the catalog.
func validLedgerState(state string) bool {
	return state == pendingState || state == retiredState || state == deletingState
}

const (
	// MaxRecordedObjects bounds the keys of one RecordPendingObjects call.
	MaxRecordedObjects = 2 * MaxPacksPerWindow
	// MaxClaimedObjects bounds the keys of one claim or forget call.
	MaxClaimedObjects = 4096
	// MinPendingObjectAge is the smallest age at which a pending key can be
	// claimed; the service requires at least its archive attempt timeout.
	MinPendingObjectAge = time.Millisecond
	// MinRetiredObjectAge is the smallest age at which a retired key can be
	// claimed, whatever age the caller asks for: a query that selected the
	// window before its retirement reads for at most the gateway's 30-second
	// request cap. The catalog enforces it because retired keys are shared by
	// every process, whatever its own configuration.
	MinRetiredObjectAge = 10 * time.Minute
)

// retiredAge is the age a retired key must reach before a claim.
func retiredAge(minAge time.Duration) time.Duration { return max(minAge, MinRetiredObjectAge) }

// ErrUnrecordedObject reports a publication that references a key which this
// catalog did not record as a pending upload, or which garbage collection has
// already claimed. The attempt must be retried with fresh keys.
var ErrUnrecordedObject = errors.New("object key is not a pending upload of this catalog")

// ObjectLedger records the object keys of archive attempts so that the objects
// of attempts that never publish can be deleted. Only keys recorded by this
// catalog are ever returned for deletion: published objects, objects of other
// catalogs sharing the bucket, and objects older than the ledger are not.
type ObjectLedger interface {
	// RecordPendingObjects records keys under tenant before they are uploaded.
	// It never performs object I/O and fails if a key is known already.
	RecordPendingObjects(ctx context.Context, tenant string, keys []string) error
	// ClaimUnpublishedObjects returns at most limit keys whose objects may be
	// deleted, oldest first: keys claimed earlier but not yet forgotten, by
	// their last claim or deferral, pending keys recorded at least minAge ago
	// and retired keys recorded at least max(minAge, MinRetiredObjectAge) ago,
	// by their record time. It claims the unclaimed ones; a claimed key can no
	// longer be published. Ages use the catalog's clock. A PostgreSQL catalog
	// claims only the pending keys recorded in its current database
	// incarnation (see ADR 0006).
	ClaimUnpublishedObjects(ctx context.Context, minAge time.Duration, limit int) ([]string, error)
	// DeferClaimedObjects moves claimed keys whose deletion failed behind the
	// other claimed keys, so that one key that cannot be deleted does not block
	// the rest. Keys that are not claimed are left unchanged.
	DeferClaimedObjects(ctx context.Context, keys []string) error
	// ForgetDeletedObjects removes claimed keys whose objects were deleted.
	// Keys that are not claimed are left unchanged.
	ForgetDeletedObjects(ctx context.Context, keys []string) error
	// UnpublishedObjects counts the recorded keys in each state.
	UnpublishedObjects(ctx context.Context) (UnpublishedObjectCounts, error)
}

// UnpublishedObjectCounts counts the keys no window references. Pending keys
// may still belong to a running attempt; retired keys belonged to windows that
// Cold retention removed; deleting keys have been claimed by garbage
// collection and their objects may still exist.
type UnpublishedObjectCounts struct {
	Pending  int64 `json:"pending"`
	Retired  int64 `json:"retired"`
	Deleting int64 `json:"deleting"`
	// Foreign counts pending keys recorded in another PostgreSQL database
	// incarnation, before a restore, copy or failover. They are never claimed.
	Foreign int64 `json:"foreign"`
}

// validatePendingKeys requires distinct object keys of one configured tenant.
func validatePendingKeys(c Contract, tenant string, keys []string) error {
	found := false
	for _, t := range c.Tenants {
		found = found || t == tenant
	}
	if !found {
		return fmt.Errorf("%w: tenant is not configured", ErrInvalidPublication)
	}
	if len(keys) == 0 || len(keys) > MaxRecordedObjects {
		return fmt.Errorf("%w: invalid number of pending object keys", ErrInvalidPublication)
	}
	seen := make(map[string]bool, len(keys))
	prefix := "tenants/" + tenant + "/"
	for _, key := range keys {
		if !validObjectKey(key, prefix) || seen[key] {
			return fmt.Errorf("%w: invalid or repeated pending object key", ErrInvalidPublication)
		}
		seen[key] = true
	}
	return nil
}

// validateClaim checks the arguments of ClaimUnpublishedObjects.
func validateClaim(minAge time.Duration, limit int) error {
	if minAge < MinPendingObjectAge || minAge%time.Millisecond != 0 {
		return errors.New("pending object age must be whole milliseconds of at least 1ms")
	}
	if limit < 1 || limit > MaxClaimedObjects {
		return fmt.Errorf("claim limit must be in [1,%d]", MaxClaimedObjects)
	}
	return nil
}

// validateForget requires a bounded set of distinct, well-formed keys.
func validateForget(keys []string) ([]string, error) {
	if len(keys) > MaxClaimedObjects {
		return nil, fmt.Errorf("forget at most %d keys at once", MaxClaimedObjects)
	}
	out := append([]string(nil), keys...)
	sort.Strings(out)
	for i, key := range out {
		if !validObjectKey(key, "tenants/") || (i > 0 && out[i-1] == key) {
			return nil, errors.New("invalid or repeated object key")
		}
	}
	return out, nil
}

// checkClaimedKey refuses to return a key that is no valid object key under a
// configured tenant: a damaged ledger must not direct deletions elsewhere.
func checkClaimedKey(c Contract, key string) error {
	for _, tenant := range c.Tenants {
		if validObjectKey(key, "tenants/"+tenant+"/") {
			return nil
		}
	}
	return errors.New("catalog object ledger holds a key outside the configured tenants")
}
