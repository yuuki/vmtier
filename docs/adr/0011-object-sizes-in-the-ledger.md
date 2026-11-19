# ADR 0011: Object sizes in the object ledger

## Status

Accepted and implemented for the service backends (#59). SQLite schema 3 and
PostgreSQL schema 4 add the size column; `catalog-upgrade` applies them offline.

## Context

The object ledger of [ADR 0006](0006-orphan-garbage-collection.md) records every
object key that no window references: keys of running or failed attempts
(`pending`), keys of windows that [Cold retention](0007-cold-retention.md)
removed (`retired`), and keys that garbage collection claimed (`deleting`). It
recorded no sizes, so `vmtier_catalog_unpublished_objects` counted objects but
not bytes. Garbage collection is opt-in; without it, the objects of failed
attempts stay in the bucket for ever. Cold retention requires garbage
collection, so the objects of retired windows wait only for its next passes.
vmtier could not say how much of the bucket either kind uses.

Providers report bucket usage themselves (for example the per-bucket usage of
Sakura Object Storage, #15), but that total mixes the catalog's published
objects, its unreferenced objects, objects of other catalogs that share the
prefix, older versions, and objects that vmtier never wrote. vmtier cannot list
the bucket to separate them: correctness does not depend on LIST
([ADR 0002](0002-independent-catalog.md)), and a listing cannot tell catalogs
apart (ADR 0006).

Both sizes are known when the ledger records a key. `uploadPack` builds the
payload and the index and computes their lengths before it records the keys and
starts the first PUT. A retired window's pack descriptors hold the lengths of
its objects.

## Decision

- Add a nullable `object_bytes` column to `pending_objects`, bounded like a pack
  descriptor (0 to 64 MiB). SQLite schema 3 and PostgreSQL schema 4 add it.
- `RecordPendingObjects` takes every key with the length of the object that the
  following PUT stores. Recording refuses a size outside the bound.
- Cold retention copies each key's size from its pack descriptor when it moves
  the window's keys to the ledger. Claims, deferrals and publication do not
  change a size; forgetting a key removes it.
- Keys recorded before the upgrade keep a NULL size. The upgrade does not
  derive one: that would need object I/O (a GET or HEAD per key) during an
  offline catalog change, and the objects of failed attempts may not exist at
  all. Garbage collection claims and forgets these keys like any other.
- `UnpublishedObjects` returns, per state, the sum of the recorded sizes and the
  number of keys without one. `/metrics` exposes them as
  `vmtier_catalog_unpublished_object_bytes{state}` and
  `vmtier_catalog_unpublished_objects_unsized{state}`, next to the existing
  counts, from the same read-only transaction, and omits all three when the
  catalog cannot be read.
- `catalog-upgrade` upgrades SQLite schema 1 or 2 and PostgreSQL schema 1, 2 or
  3 in one transaction, as before. `catalog-restore` upgrades an older backup in
  its staging copy and checks every stored size. `catalog-import-postgres`
  copies the sizes and compares their sum and the number of keys without a size
  with the source before it commits. The backup metadata format does not change.

## Consequences

- A recorded size is the length vmtier meant to store, not a measurement of
  the bucket. A `pending` key whose PUT never ran or failed, and a `deleting`
  key whose object is already deleted, still count until garbage collection
  forgets them; an object that vmtier never recorded (left before the ledger
  existed, or written by other software) never counts. While a state has keys
  without a size, its byte gauge is a lower bound for the recorded keys.
- With the committed pack lengths (`vmtier_catalog_committed_object_bytes`), an
  operator can split a provider's bucket usage into published objects,
  unreferenced objects vmtier knows, and the rest.
- The publication protocol is unchanged: the size is not read by publication,
  claims or retention decisions, so `spec/VmtierPublication.tla` models no size.
- As with the earlier schema changes, a release that predates the new schema
  refuses an upgraded catalog. Upgrade requires stopping the writers (SQLite)
  or the legacy writers (PostgreSQL) and a backup first; no automatic downgrade
  is provided.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| HEAD every ledger key at scrape time | Object I/O per scrape and per key, against the shared S3 budget; a missing object would still be ambiguous |
| LIST the prefix and subtract the catalog | Rejected by ADR 0002 and ADR 0006: it mixes catalogs and needs LIST |
| A separate size table | Two tables to keep consistent in every ledger transaction for no gain over a column |
| Backfill sizes during the upgrade with GET requests | Object I/O inside an offline schema change; objects of failed attempts may be partial or absent |
