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

- `catalog-add-streams -config FILE [-start MS]` replaces the stored contract
  with the configuration's, which must keep the timing contract, the blob
  locator and every existing tenant and source with its URL fingerprint, and
  add at least one tenant or source. Every new tenant/source pair gets a
  coverage row that starts and ends at one aligned origin, in one catalog
  transaction (PostgreSQL: under the catalog state row lock that publication
  takes). Existing streams, windows and the generation are unchanged.
- The origin defaults to the next window to become eligible, so a new stream
  needs no catch-up and is complete from the start. An earlier `-start` makes
  the archiver catch up from Hot; it is refused when its first window has
  already left Hot retention and its safety margin.
- The range below a new stream's origin is outside the query guarantee for that
  stream, like the range before the catalog's coverage origin and below a
  retained start. `spec/lean`'s `retention_complete` is stated for a stream's
  own origin and covers it.
- Processes accept a stored contract that covers their configuration (equal
  timing fields and deduplication, configured tenants and sources a subset of
  the stored ones, equal fingerprints) instead of requiring equality. A process
  still configured without the new streams keeps serving the tenants it knows
  and archiving the streams it knows, which lets PostgreSQL replicas roll to the
  new configuration one by one. Readiness considers every stream of the catalog,
  so a deployment where no process archives a stream reports it.
- SQLite runs the command offline: its directory lock refuses a live `serve`.
  PostgreSQL runs it online.

## Alternatives

| Alternative | Reason not selected |
| --- | --- |
| A per-stream origin column next to the retained start | The retained start already is the lower bound of a stream's guarantee; a second column would only differ before the first retirement |
| Require equal contracts and a full stop to change them | PostgreSQL deployments would need a complete outage for a metadata change |
| Default the origin to the coverage origin | The first windows are usually gone from Hot, and the archiver would then fail forever |
| Remove tenants or sources, or change what is behind a source ID | Removal interacts with retention and readers; resharding behind an ID cannot be detected by the gateway |

## Consequences

A tenant's coverage end is the minimum over its sources, so adding a source to
an existing tenant keeps that tenant's required coverage waiting on the new
stream: with an earlier `-start`, queries over the catch-up range fail until the
new source has archived it. Stream starts are visible in `catalog-inspect` and
the `vmtier_stream_coverage_start_ms` metric. A process configured with fewer
tenants or sources than its catalog is no longer refused at startup; readiness
and per-stream lag metrics show the streams it leaves unarchived.
