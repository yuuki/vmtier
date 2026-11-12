# ADR 0007: Cold retention

## Status

Accepted and implemented for the service backends (#44). It extends the object
ledger of [ADR 0006](0006-orphan-garbage-collection.md) and is opt-in through
`cold_retention`. SQLite schema 2 and PostgreSQL schema 3, not yet released when
this was added, carry the retention columns together with the ledger.

## Context

Published windows were kept forever, so storage grew linearly with time and a
deployment that must bound retention, or delete data for legal reasons, was
unsuitable. An S3 lifecycle rule is no substitute: it cannot tell which objects
the catalog references, and queries then fail on missing objects instead of on a
documented boundary.

Deleting published data raises questions that orphan collection did not:

- A query may have selected a window before it was removed and still read its
  objects afterwards.
- Every gateway must agree where Cold data begins, including PostgreSQL
  processes whose clocks differ.
- A backup taken before a deletion references windows whose objects are gone.
- The query guarantee proved in `spec/lean` must say which samples it covers.

## Decision

- `cold_retention` (whole milliseconds, greater than `hot_retention`) enables
  retention. It requires garbage collection, which deletes the objects, with
  `service.orphan_gc_min_age` of at least 10 minutes.
- Before collecting, the collector asks the catalog to retire every window that
  ends at or before `floor((now - cold_retention) / window) * window - window`,
  where `now` is the catalog's clock: the database clock for PostgreSQL, so that
  every replica retires against one clock. The window subtracted at the end is a
  margin for gateways whose clocks lag the catalog's. Nothing is retired until
  the boundary passes the coverage origin.
- `RetireWindows` removes, in one catalog transaction, the oldest windows across
  streams (oldest start first, found through an index on `windows.start_ms`),
  at most 64 per transaction and 16 transactions per pass, with their packs (and
  PostgreSQL key reservations). It records their object keys in the ledger as
  `retired`, advances each stream's `start_ms` past them, and counts them in
  `retired_windows`. A pass that stops at that bound is followed by another
  after the next archive pass, so a backlog drains instead of waiting for the
  garbage collection interval. Coverage end and generation are unchanged:
  the generation keeps counting publications, so it equals the windows still
  present plus the retired ones. PostgreSQL takes the catalog state row lock
  first, like publication and lease changes; retirement needs no lease.
- Garbage collection claims retired keys like pending ones, once they reach the
  minimum age and at least `MinRetiredObjectAge` (10 minutes), which the catalog
  enforces whatever age a process asks for: replicas may run different
  configurations. A query holding a plan selected before the retirement reads
  for at most the gateway's 30-second request cap, far below that age. Publication
  never consumes a retired key, and recording refuses one, so a retired window
  cannot reappear.
- Each stream's start is part of the catalog state, not computed from a
  process's clock. Selection finds only the windows that remain; the range
  below the start is outside the guarantee, exactly like the range before the
  coverage origin, and queries return what Hot still holds there. This matches
  VictoriaMetrics' own retention, where queries beyond retention return what
  remains instead of failing.
- A stream's start never moves back. Lengthening `cold_retention` only pauses
  retirement until the boundary passes the stored start again; shortening it
  retires more on the next pass.
- Backup/restore and the SQLite-to-PostgreSQL import keep the starts, the
  retired count and the retired ledger records; their validation requires each
  stream's windows to be contiguous from its start. A restored backup that is
  older than a retirement references windows whose objects are gone. Queries
  over them fail on the missing objects rather than returning less, until the
  restored service's first collection pass retires those windows again, because
  the boundary depends only on the clock and the configuration.
- `spec/VmtierPublication.tla` lets retirement remove a stream's oldest window
  at any time and checks coverage, generation, the published objects and that
  every retired object stays recorded for deletion; readers and the minimum age
  remain outside the model. `spec/lean` proves that the boundary of a
  retirement that runs is never younger than `cold_retention` plus a window,
  and that an accepted query misses no accepted sample younger than
  `cold_retention` as long as the catalog's clock is at most one window ahead
  of the gateway's: each lies in Cold's retained range or in Hot.

## Alternatives

| Alternative | Reason not selected |
| --- | --- |
| Refuse queries that reach below the retained start | Dashboards spanning more than the retention would always fail; the coverage origin already set the precedent of an unguaranteed earlier range |
| Compute the retained start from each process's clock | PostgreSQL gateways with skewed clocks would disagree, and a gateway behind the deleting process could treat a removed window as missing coverage |
| Delete objects in the retirement transaction | Object I/O must stay outside transactions, and queries holding older plans would fail |
| A lifecycle rule on object age | Cannot distinguish published from unpublished objects or follow the catalog |
| Per-tenant retention | No requirement yet; the per-stream start allows it later without a format change |

## Consequences

Storage of published data is bounded by `cold_retention` plus one window and
the minimum age, as long as retirement and deletion keep up; their throughput
is bounded per pass, and passes repeat while a backlog remains. On a versioned
bucket, deletion only adds delete markers, and storage is reclaimed by a
lifecycle rule that expires noncurrent versions.

Retirement deletes objects, not just references of one catalog. A catalog
restored from a backup or imported from another one shares object keys with
its source; when either retires a window, the other loses that window's
objects too. Rolling back to the source catalog is safe only while it, too,
has retired everything older than the retention.
Retention needs `DeleteObject` permission, like garbage collection. Data older
than `cold_retention` is gone for good: restoring an older catalog backup does
not bring it back, and its queries fail until retirement catches up. Windows
that ingress or an outage left unarchived are not affected: retirement never
passes a stream's coverage end.
