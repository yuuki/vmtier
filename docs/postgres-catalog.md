# PostgreSQL catalog and service

The PostgreSQL implementation follows [issue #25](https://github.com/yuuki/vmtier/issues/25).
It implements the same domain operations used by the SQLite reader and archiver:
`Status`, `PublishWindow`, and bounded `Select`. The
[architecture decision](adr/0004-postgresql-catalog.md) records the staged HA design.

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
shared S3 storage and explicit request limits.

```sh
export VMTIER_POSTGRES_DSN='postgres://USER@DB-ENDPOINT/DATABASE?sslmode=verify-full'
.bin/vmtier catalog-init -backend postgres -config postgres.json -id CATALOG-ID
.bin/vmtier serve -config postgres.json -listen 127.0.0.1:8489 -http 127.0.0.1:8490
.bin/vmtier inspect -config postgres.json
.bin/vmtier verify -config postgres.json
```

Replace the example source, bucket, identity, coverage origin, and lateness
assertion before initialization. Normal startup never initializes missing state.
Contract, identity, and blob-locator checks precede object-store access. The
service's read readiness does not require owning the archive lease; a standby can
serve all covered queries. S3 or catalog failures still fail required reads.

`service.archive_lease_ttl` defaults to 30 seconds (whole milliseconds, 3 seconds
through 24 hours). Each process uses a fresh owner ID, renews while archiving, and
cancels archive work on renewal failure. Shutdown drains that work before releasing
ownership. Expiration is checked against database time. A monotonically increasing
token is checked under the same transaction lock used for publication, so an old
owner cannot publish after takeover. Raw unfenced PostgreSQL publication is rejected.
Already uploaded objects from a losing attempt can remain unreferenced; no GC runs.

Schema 2 introduces leases. Stop schema-1 writers before an explicit upgrade:

```sh
.bin/vmtier catalog-upgrade -backend postgres -config postgres.json
```

The upgrade preserves published metadata and identity, and initializes an unowned
lease. Older binaries reject the new schema. Back up before upgrading; no automatic
schema downgrade is provided.

PostgreSQL inspection and payload verification can run online. Verification checks
a bounded prefix of the initially observed coverage and shares the operator's S3
request budget allocation; later publications are checked by a subsequent run.
SQLite `catalog-backup`/`catalog-restore` do not back up PostgreSQL. Use PostgreSQL
native backups and WAL/PITR, test restoration, and fence the old authority before
cutover. A backup alone cannot recover expired Hot history or missing S3 objects.

## Repository behavior

`CreatePostgres` explicitly initializes a catalog in an existing dedicated database
and schema; it refuses existing catalog tables. `OpenPostgres` opens initialized
state and checks the expected catalog ID and supported schema. Neither operation
creates a database, changes database users, or contacts object storage. The DSN's
selected schema is pinned for subsequent SQL. Schema DDL is needed for initialization;
normal runtime access should be limited to the catalog tables and sequences.

Publication locks the catalog state row, validates generation and stream continuity,
and commits the window, ordered pack references, object keys, coverage, and generation
atomically. Zero packs represent a verified empty window. Same-publication replay
succeeds after later commits, while changed content and key reuse across data/index
roles fail. On an ambiguous commit response, reload coverage and the committed
window before deciding whether to replay; do not blindly regenerate new object keys.

Read-only repeatable-read transactions provide a consistent status or query plan.
Selection uses the existing window, pack, and descriptor-byte caps. Object I/O takes
place after the transaction ends. Published objects remain immutable and GC is not
implemented. Database failure is an error, even if object data is cached.

Blocked PostgreSQL transport reads/writes have a five-second idle timeout,
including the driver's separate cancellation connection. A timed-out socket is
closed and discarded. Progressing multi-statement operations can take longer;
this is not a whole-operation latency guarantee. A timeout during commit is an
ambiguous outcome and must be reconciled like a lost response. This bound also
means a prolonged synchronous-replication wait can return an error to the caller.

## Build and integration tests

Bootstrap pins and verifies the `github.com/lib/pq v1.10.9` archive in addition to
the existing VM and SQLite dependencies. It generates the vendor tree without
updating unrelated modules. See [third-party notices](../THIRD_PARTY_NOTICES.md).

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
