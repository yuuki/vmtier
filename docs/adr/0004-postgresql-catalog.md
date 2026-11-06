# ADR 0004: A PostgreSQL catalog foundation for high availability

## Status

Accepted for staged implementation in [issue #25](https://github.com/yuuki/vmtier/issues/25).
Stage 1 adds the repository implementation and extracts its domain interface.
Stage 2 enables PostgreSQL service/maintenance access with DB-time leases and
transaction-enforced fencing. [ADR 0003](0003-first-release-scope.md) continues to
describe the retained SQLite deployment. Database topology, gateway endpoint,
request budgets, migration, and failure qualification are explicit further work.

## Context

Multi-host availability is now a concrete requirement. The current local catalog
lock, combined service, and process-local S3 budget assume one owner. Sharing a
SQLite file or replicating backups does not provide safe automatic failover.
The existing metadata model already supports atomic window publication, bounded
time selection, immutable objects, and independent tenant/source coverage.

## Decision

Use PostgreSQL as an optional external metadata authority and retain SQLite for
single-host deployments. Reader and archiver code depend on a `Repository` with
`Status`, `Select`, `PublishWindow`, and owner-managed `Close` operations. Retain
the old JSON v2 manifest interface only for legacy compatibility.

The PostgreSQL implementation uses explicit transactional initialization in a
dedicated existing database/schema. Normal open never creates metadata. It pins
identity, schema version, VM version, archive contract, and blob locator. Queries
use the selected schema explicitly rather than falling through a search path.

Serialize publications by locking the catalog state row before checking the
expected generation. Publish window metadata, all pack references, object-key
reservations, coverage, and generation in one transaction. Identical committed
publications can be replayed after later commits; changed descriptors are rejected.
Object-key uniqueness spans both data and index roles. Source export, upload, and
read-back verification remain outside database transactions.

Read status and bounded selections in read-only repeatable-read transactions.
Resolve coverage and references from the same snapshot and release the transaction
before object I/O. Preserve explicit empty windows and fail closed when the
authority is unavailable. GC is absent, so reader leases are unnecessary at this
stage. Use the writable primary; routing reads to lagging replicas is not part
of this design.

## Subsequent stages

1. Enable PostgreSQL service configuration with DB-time archiver leases and a
   monotonically increasing fencing token checked in the publication transaction.
   Initially only the lease holder archives; both gateways can serve reads.
2. Provide a redundant Cold RPC endpoint and verify stock-vmselect reconnection.
   Allocate bucket-wide S3 budgets across instances, including SDK retries.
3. Import a stopped SQLite writer's metadata and validate contract, coverage,
   empty windows, and pack references before cutover. Existing objects need not
   move. A stale SQLite authority cannot simply resume after PostgreSQL advances.
4. Qualify database failover, ambiguous commit responses, stale owners, network
   partitions, and recovery timing. RPO 0 requires synchronous replication,
   promotion eligibility, and old-primary fencing for the stated failure model.
   Async replication and backups do not establish that guarantee.

The first stage did not expose PostgreSQL through `serve`; stage 2 replaces its
archive ownership assumption with renewable leases and fencing. Shared bucket
request limits still require explicit multi-instance allocation.
Per-stream concurrent archiving and removal of the global generation bottleneck
can follow measured need. Multi-region writes, GC, and format changes are excluded.

## Alternatives

PostgreSQL fits the relational metadata and range queries without introducing a
new publication protocol. An etcd authority with S3 manifests requires additional
index and manifest management. Raft-backed SQLite requires a different client and
operational model. S3-only publication still conflicts with the no-conditional-PUT
requirement in [ADR 0002](0002-independent-catalog.md).

## Validation

Real PostgreSQL integration tests exercise independent clients and metadata
transactions. Existing SQLite and stock-VM differential checks cover the interface
refactor. These tests do not establish synchronous replication durability, automatic
failover, provider availability, or supported HA recovery times. See the
[PostgreSQL catalog guide](../postgres-catalog.md) for setup and current boundaries.
