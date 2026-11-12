# ADR 0006: Garbage collection of unpublished attempt objects

## Status

Accepted and implemented for the service backends (#43). SQLite schema 2 and
PostgreSQL schema 3 add the object ledger; `catalog-upgrade` applies them
offline. Garbage collection is opt-in through `service.orphan_gc_interval`.
Cold retention (#44) and compaction (#45) remain separate decisions.

## Context

A failed or abandoned archive attempt (export or verification failure, timeout,
lost PostgreSQL lease, crash before commit, a lost commit response followed by a
retry) can leave pack and index objects under fresh attempt keys that the
catalog never references. vmtier deleted nothing, so bucket usage grew with the
failure rate ([limits](../limits.md)).

The obvious design lists the bucket and deletes what the catalog does not
reference. It is unsafe in two ways:

- It races with attempts in flight. An object that is uploaded and read back
  but not yet published looks unreferenced. Bounding the race needs timing
  assumptions (object timestamps, lease expiry, clock skew) that the publication
  protocol otherwise avoids.
- One bucket and prefix can hold objects of several catalogs: the catalog kept
  for rollback after `catalog-restore`, the SQLite source of a PostgreSQL
  migration, an imported JSON v2 catalog, or a misconfigured second deployment.
  Object keys do not name their catalog, and a listing cannot tell them apart.

The design also states that correctness does not depend on LIST and that the
catalog cannot be rebuilt from an object listing.

## Decision

- Add an object ledger to the catalog (`pending_objects`: key, creation time,
  state `pending` or `deleting`). An attempt records each pack's two keys as
  pending in a short transaction before their PUT. Recording refuses a key that
  is published or already recorded.
- The publication transaction consumes the pending record of every key it
  references and fails with `ErrUnrecordedObject` when one is missing or claimed.
  A replay of a committed window with the same digest still succeeds without
  consuming anything. Imports of objects an existing catalog references (JSON v2
  import) do not go through the ledger.
- Garbage collection claims pending keys older than a minimum age by moving them
  to `deleting` in one transaction, deletes their objects with ordinary
  `DeleteObject` requests (a missing object counts as deleted), then forgets the
  records. Claimed keys that were not forgotten are returned first by the next
  claim, so an interrupted pass resumes.
- A claim and a publication of the same key are serialized by the catalog: one
  SQLite connection, or a PostgreSQL row lock (`FOR UPDATE SKIP LOCKED` on the
  claim side). A claimed key can never be published and a published key can
  never be claimed. Safety depends on neither clocks, leases nor object
  listings; the minimum age (`service.orphan_gc_min_age`, default 24 hours, at
  least `service.archive_timeout`) only keeps garbage collection from discarding
  attempts that are still running. A claim also refuses a record whose key is
  published or outside the configured tenants, so a damaged ledger cannot direct
  a deletion at a published object.
- `serve` runs garbage collection between archive passes when
  `service.orphan_gc_interval` is set; with PostgreSQL only in the archiver
  lease holder, which keeps DELETE traffic in one process. Every request,
  including SDK retries, uses the process's shared S3 budget.
- `catalog-backup`/`catalog-restore` and the SQLite-to-PostgreSQL import keep the
  ledger. These run with the writer stopped, so a pending key in them belongs to
  an attempt that can no longer publish in either catalog. A restore of a schema
  1 backup upgrades the staging copy after its checksum was verified.
- `spec/VmtierPublication.tla` models the ledger and lets garbage collection
  claim, delete and forget at any time. `VmtierPublication_NoPendingCheck.cfg`
  shows that without the pending check a claimed key is published and its object
  deleted.

## Alternatives

| Alternative | Reason not selected |
| --- | --- |
| LIST the prefix and delete unreferenced objects older than a grace period | Deletes objects of other catalogs sharing the prefix; safety rests on timestamps, lease expiry and clock skew; needs LIST |
| Namespace new keys by catalog ID and LIST only that namespace | Changes the object key format; still needs a grace period against in-flight attempts and LIST |
| Keep tombstones instead of requiring a pending record | Publication would accept unrecorded keys; tombstones must be kept forever to stay safe |
| S3 lifecycle rules on the attempt prefix | Cannot tell published from unpublished objects |
| Collect orphans that existed before the upgrade | They were never recorded; telling them apart needs exactly the listing comparison rejected above |

## Consequences

Each pack costs one more small catalog transaction before its upload, and
publication deletes the pending records it consumes. Ledger rows exist only for
keys of attempts that did not publish, until garbage collection forgets them.

Existing catalogs need an offline `catalog-upgrade` (SQLite schema 1 to 2,
PostgreSQL schema 1 or 2 to 3); a release before this one refuses the upgraded
catalog. Garbage collection needs `DeleteObject` permission on the prefix in
addition to PUT and GET; deployments that do not enable it need nothing new.

Orphans created before the upgrade were never recorded and are never collected.
An upload that is still in flight when its object is deleted can store it again;
that object stays an orphan, unpublishable because its key is no longer pending.
Published windows remain append-only and are never deleted.
