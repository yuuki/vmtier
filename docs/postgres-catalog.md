# PostgreSQL catalog foundation

The PostgreSQL repository is stage 1 of [issue #25](https://github.com/yuuki/vmtier/issues/25).
It implements the same domain operations used by the SQLite reader and archiver:
`Status`, `PublishWindow`, and bounded `Select`. The
[architecture decision](adr/0004-postgresql-catalog.md) records the staged HA design.

**The CLI and `serve` do not yet accept PostgreSQL configuration.** Continue to use
the [SQLite service guide](serve.md) for the existing deployment. Archiver fencing,
multiple gateways, cluster-wide S3 budgets, SQLite migration, and database failover
qualification are later stages. Do not deploy multiple archivers merely because
the database accepts multiple connections.

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
