package catalog

import (
	"context"
	"errors"
	"fmt"
)

// ContractCovers reports whether a stored contract serves a configuration:
// the timing fields and deduplication are equal, and every configured tenant
// and source, with its fingerprint, is in the stored contract. Streams added
// to a catalog later make its contract cover the earlier configuration, so a
// process still running that configuration keeps serving and archiving the
// streams it knows while the deployment rolls to the new one.
func ContractCovers(stored, configured Contract) bool {
	if stored.CoverageStart != configured.CoverageStart || stored.WindowMS != configured.WindowMS || stored.AgeMS != configured.AgeMS || stored.LatenessMS != configured.LatenessMS || stored.DedupMS != configured.DedupMS {
		return false
	}
	tenants := make(map[string]bool, len(stored.Tenants))
	for _, tenant := range stored.Tenants {
		tenants[tenant] = true
	}
	for _, tenant := range configured.Tenants {
		if !tenants[tenant] {
			return false
		}
	}
	sources := make(map[string]string, len(stored.Sources))
	for _, source := range stored.Sources {
		sources[source.ID] = source.Fingerprint
	}
	for _, source := range configured.Sources {
		if fingerprint, ok := sources[source.ID]; !ok || fingerprint != source.Fingerprint {
			return false
		}
	}
	return true
}

// StreamAdder adds tenants or sources to an initialized catalog.
//
// AddStreams replaces the stored contract with next, which must cover it and
// add at least one tenant or source. Every new tenant/source pair starts and
// ends at origin, an aligned start no earlier than the coverage origin: the
// archiver fills it from there, and the range below it is outside the query
// guarantee for that stream. Existing streams, windows and the generation are
// unchanged. The caller checks that Hot can still export origin's window.
type StreamAdder interface {
	AddStreams(ctx context.Context, next Contract, origin int64) ([]StreamCoverage, error)
}

// newStreams validates a contract change and returns the streams it adds.
func newStreams(current, next Contract, origin int64) ([]StreamCoverage, error) {
	if err := validateContract(next); err != nil {
		return nil, err
	}
	next = canonicalContract(next)
	if !ContractCovers(next, current) {
		return nil, errors.New("new contract must keep the timing contract and every existing tenant and source")
	}
	if origin < current.CoverageStart || (origin-current.CoverageStart)%current.WindowMS != 0 {
		return nil, fmt.Errorf("stream origin %d must be a window start no earlier than the coverage origin", origin)
	}
	existing := make(map[string]bool, len(current.Tenants)*len(current.Sources))
	for _, tenant := range current.Tenants {
		for _, source := range current.Sources {
			existing[tenant+"\x00"+source.ID] = true
		}
	}
	var added []StreamCoverage
	for _, tenant := range next.Tenants {
		for _, source := range next.Sources {
			if !existing[tenant+"\x00"+source.ID] {
				added = append(added, StreamCoverage{Tenant: tenant, SourceID: source.ID, Start: origin, End: origin})
			}
		}
	}
	if len(added) == 0 {
		return nil, errors.New("new contract adds no tenant or source")
	}
	return added, nil
}
