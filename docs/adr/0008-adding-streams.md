# ADR 0008: Adding tenants and sources to a catalog

## Status

Accepted and implemented for the service backends (#48). It relies on the
per-stream start that [Cold retention](0007-cold-retention.md) introduced and
needs no schema change of its own.

## Context

Tenants and sources were part of the immutable catalog contract. Adding a tenant
meant a new catalog that had to re-archive every tenant from Hot, which is
impossible for history Hot no longer holds, or a second deployment. The
gateway's coverage guarantee, readiness and the archiver all assumed that every
stream starts at the catalog's coverage origin.

## Decision

- `catalog-add-streams -config FILE` replaces the stored contract
  with the configuration's, which must keep the timing contract, the blob
  locator and every existing tenant and source with its URL fingerprint, and
  add at least one tenant or source. Every new tenant/source pair gets a
  coverage row that starts and ends at one aligned origin, in one catalog
  transaction (PostgreSQL: under the catalog state row lock that publication
  takes). Existing streams, windows and the generation are unchanged.
- The catalog chooses the origin: the archive-age boundary of its own clock
  (the database clock for PostgreSQL), which the gateway already requires. A
  new stream therefore needs no catch-up and is complete from the start, and
  no host clock or operator input can place it too early (expired from Hot) or
  in the future (where Hot would expire unarchived data unnoticed). History
  before the origin is not archived for the new stream.
- The range below a new stream's origin is outside the query guarantee for that
  stream, like the range before the catalog's coverage origin and below a
  retained start. `spec/lean`'s `retention_complete` is stated for a stream's
  own origin and covers it.
- PostgreSQL processes accept a stored contract that covers their configuration
  (equal timing fields and deduplication, configured tenants and sources a
  subset of the stored ones, equal fingerprints) instead of requiring equality.
  A replica still configured without the new streams keeps serving the tenants
  it knows and archiving the streams it knows, which lets replicas roll to the
  new configuration one by one. Readiness considers the streams a process is
  configured for, so an old replica stays ready while a new stream it does not
  know waits for a replica configured with it; per-stream lag metrics cover
  every stream of the catalog.
- SQLite runs the command offline: its directory lock refuses a live `serve`.
  A SQLite `serve` must be configured with exactly the catalog's streams, since
  its single owner is the only archiver. PostgreSQL runs the command online.

## Alternatives

| Alternative | Reason not selected |
| --- | --- |
| An operator-chosen earlier origin (`-start`) for catch-up | The first window could leave Hot before the archiver reaches it, or the origin could lie in the future; both leave a stream that never completes |
| A per-stream origin column next to the retained start | The retained start already is the lower bound of a stream's guarantee; a second column would only differ before the first retirement |
| Require equal contracts and a full stop to change them | PostgreSQL deployments would need a complete outage for a metadata change |
| Default the origin to the coverage origin | The first windows are usually gone from Hot, and the archiver would then fail forever |
| Remove tenants or sources, or change what is behind a source ID | Removal interacts with retention and readers; resharding behind an ID cannot be detected by the gateway |

## Consequences

A tenant's coverage end is the minimum over its sources. A source added to an
existing tenant starts at the required end, so the tenant stays complete as
long as a replica configured with the new source archives it; until one holds
the archiver lease, the new stream falls behind one window at a time and that
tenant's queries over the missing range fail. Stream starts are visible in
`catalog-inspect` and the `vmtier_stream_coverage_start_ms` metric. A
PostgreSQL replica configured with fewer tenants or sources than its catalog is
not refused at startup; the per-stream lag metrics show the streams it leaves
unarchived.
