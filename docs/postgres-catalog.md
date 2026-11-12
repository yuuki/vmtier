# PostgreSQL catalog and service

The PostgreSQL implementation follows [issue #25](https://github.com/yuuki/vmtier/issues/25).
It implements the same domain operations used by the SQLite reader and archiver:
`Status`, `PublishWindow`, and bounded `Select`. The
[architecture decision](adr/0004-postgresql-catalog.md) records the staged HA design.
Executed gateway, migration, and database fault checks are recorded in
[local HA qualification](postgres-ha-validation.md), including their limits.

`serve` supports PostgreSQL with an active-standby archiver and independently
available read gateways. All candidates share one catalog and S3 blob locator.
The database selects one archive owner using a renewable lease. The
[SQLite service guide](serve.md) describes the retained single-host alternative.

## Configuration and lifecycle

Start with [the PostgreSQL example](../examples/serve-postgres.json). Supply the
DSN through the environment variable named by `catalog.dsn_env`; configuration
files contain no database password. Provision a dedicated database/schema and
least-privilege credentials. Select the writable primary through a stable HA
endpoint and use verified TLS for remote connections. PostgreSQL `serve` requires
shared S3 storage and explicit aggregate request limits. Set
`service.max_replicas` to the maximum simultaneous S3 clients sharing that budget;
each receives a fixed share. See [HA deployment](ha-deployment.md) for replica,
maintenance, rollout, and endpoint requirements.

```sh
export VMTIER_POSTGRES_DSN='postgres://USER@DB-ENDPOINT/DATABASE?sslmode=verify-full'
.bin/vmtier catalog-init -backend postgres -config postgres.json -id CATALOG-ID
.bin/vmtier serve -config postgres.json -listen 127.0.0.1:8489 -http 127.0.0.1:8490
.bin/vmtier inspect -config postgres.json
.bin/vmtier verify -config postgres.json
```

Replace the example source, bucket, identity, coverage origin, and lateness
assertion before initialization. Both backends pin the current VM release and
revision; an older catalog requires the [VM upgrade procedure](compatibility.md#codec-and-upgrades),
not a schema upgrade or backend import. Normal startup never initializes missing state.
Contract, identity, and blob-locator checks precede object-store access. The
service's read readiness does not require owning the archive lease; a standby can
serve all covered queries. S3 or catalog failures still fail required reads.

`service.archive_lease_ttl` defaults to 30 seconds (whole milliseconds, 3 seconds
through 24 hours). Each process uses a fresh owner ID, renews while archiving, and
cancels archive work on renewal failure. Shutdown drains that work before releasing
ownership. Expiration is checked against database time. A monotonically increasing
token is checked under the same transaction lock used for publication, so an old
owner cannot publish after takeover. Raw unfenced PostgreSQL publication is rejected.
Already uploaded objects from a losing attempt remain unreferenced until
[garbage collection](serve.md#garbage-collection) deletes them. When
`service.orphan_gc_interval` is set, only the lease holder collects, between its
archive passes; its claims and every publication are ordered by row locks on the
object ledger, so a claimed key can never be published by any process.

Schema 2 introduces leases and schema 3 the object ledger and the Cold retention
columns. The schema upgrade
requires an already compatible VM pin; it does not convert v1.128.0 catalogs or
packs to v1.155.0. Stop writers of the older schema before an explicit upgrade,
which moves schema 1 or 2 to 3 in one transaction:

```sh
.bin/vmtier catalog-upgrade -backend postgres -config postgres.json
```

The upgrade preserves published metadata and identity, initializes an unowned
lease when upgrading from schema 1, adds an empty object ledger, and starts every
stream at the coverage origin. Older binaries
reject the new schema. Back up before upgrading; no automatic schema downgrade is
provided. Objects of attempts that failed before the upgrade were never recorded
and are not collected.

PostgreSQL inspection and payload verification can run online. Verification checks
a bounded prefix of the initially observed coverage and shares the operator's S3
request budget allocation; later publications are checked by a subsequent run.
SQLite `catalog-backup`/`catalog-restore` do not back up PostgreSQL. Use PostgreSQL
native backups and WAL/PITR, test restoration, and fence the old authority before
cutover. A backup alone cannot recover expired Hot history or missing S3 objects.

`catalog-add-streams -config FILE` adds tenants or sources online. A process still
configured without them keeps serving, archiving and reporting readiness for the
streams it knows, because the stored contract covers its configuration. Roll every
replica to the new configuration promptly: only a replica configured with a new
stream archives it, so the new stream falls behind until such a replica holds the
archiver lease. See [adding tenants or sources](serve.md#add-tenants-or-sources).

Pending object keys record the database incarnation they were written in, so a
catalog restored with point-in-time recovery or from a dump never collects the
objects of attempts that the original may still publish; see
[garbage collection](serve.md#garbage-collection). Concurrent `catalog-upgrade`
runs, for example one per replica, wait for each other.

To move an existing stopped SQLite service without rewriting objects, use
[`catalog-import-postgres`](postgres-migration.md). It creates a fresh destination
under a new identity and verifies metadata equivalence before committing.

## Repository behavior

`CreatePostgres` explicitly initializes a catalog in an existing dedicated database
and schema; it refuses existing catalog tables. `OpenPostgres` opens initialized
state and checks the expected catalog ID and supported schema. Neither operation
creates a database, changes database users, or contacts object storage. The DSN's
selected schema is pinned for subsequent SQL. Schema DDL is needed for initialization;
normal runtime access should be limited to the catalog tables and sequences.

Publication locks the catalog state row, validates generation and stream continuity,
consumes the pending ledger record of every referenced key, and commits the window,
ordered pack references, object keys, coverage, and generation atomically. A key
that garbage collection claimed fails the publication; the archiver retries with
fresh keys. Zero packs represent a verified empty window. Same-publication replay
succeeds after later commits, while changed content and key reuse across data/index
roles fail. On an ambiguous commit response, reload coverage and the committed
window before deciding whether to replay; do not blindly regenerate new object keys.

Cold retention locks the catalog state row like publication, removes the oldest
windows with their packs and key reservations, records their keys as retired, and
advances each stream's start, all in one transaction.

Read-only repeatable-read transactions provide a consistent status or query plan.
Selection uses the existing window, pack, and descriptor-byte caps. Object I/O takes
place after the transaction ends. Published objects remain immutable and are never
deleted. Database failure is an error, even if object data is cached.

Blocked PostgreSQL transport reads/writes have a five-second idle timeout,
including the driver's separate cancellation connection. A timed-out socket is
closed and discarded. Progressing multi-statement operations can take longer;
this is not a whole-operation latency guarantee. A timeout during commit is an
ambiguous outcome and must be reconciled like a lost response. This bound also
means a prolonged synchronous-replication wait can return an error to the caller.

## Build and integration tests

Bootstrap downloads the committed Go module versions, verifies their checksums,
and regenerates the vendor tree with `go mod vendor`. The PostgreSQL driver is
`github.com/lib/pq v1.12.3`; dependencies are not updated during bootstrap. See [third-party notices](../THIRD_PARTY_NOTICES.md).

Use a **disposable test database** with permission to create/drop test schemas.
The integration suite creates isolated schemas and removes them on completion.
It does not need an object store or production credentials.

```sh
./scripts/bootstrap.sh
export VMTIER_TEST_POSTGRES_DSN='postgres://vmtier:vmtier@127.0.0.1:5432/vmtier_test?sslmode=disable'
make build test race vet
```

The credentials above are only an example for a disposable loopback test server;
`sslmode=disable` is not a remote deployment recommendation. Without the environment
variable, PostgreSQL integration tests explicitly skip and the existing tests run.
CI provisions a disposable PostgreSQL service so these checks do not silently skip.

Tests cover explicit initialization, pinned identity/schema, atomic publication,
independent-client generation conflicts, replay, invalid transitions, key reuse,
empty windows, bounded selections, and reopening committed state. A single-server
test does not exercise PostgreSQL promotion or prove end-to-end HA. PostgreSQL 17
is the selected integration-test major version; other majors require validation.
