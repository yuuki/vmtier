# SQLite catalog foundation

This implements the catalog foundation of [the first-release design](design.md).
It is available through an internal Go repository and **offline management commands**.
The existing `archive`, `gateway`, `inspect`, `verify`, and catalog HTTP service still
use JSON catalog v2. They do not read or publish SQLite catalogs yet. The single-owner
`serve` process and runtime integration are the next step.

Do not set `catalog.type=sqlite`: that setting is not supported yet. Imported SQLite
state is a migration candidate for inspection and testing, not a replacement query
backend at this implementation stage.

Executed tests and remaining verification are recorded in
[stage 1 validation](sqlite-catalog-validation.md).

## Repository behavior

- Explicit initialization, pinned catalog identity, and an exclusive state-directory
  lock. Missing, corrupt, wrong-ID, and unsupported-schema state are rejected.
- SQLite schema 1 with WAL, FULL synchronization, foreign keys, and one connection
  for short metadata transactions. S3 operations stay outside transactions.
- Immutable archive contract and blob locator: an absolute local path or S3
  endpoint/region/bucket/prefix, without credentials.
- Atomic publication per tenant/source/window, with multiple packs or an explicit
  empty interval. Coverage is contiguous per stream; unrelated streams can advance.
- Expected-generation checks and idempotent replay, including after later commits.
  Different content for a committed window and reuse of published keys are rejected.
- Indexed, bounded tenant/time selection. The returned references outlive the short
  transaction because objects are immutable and GC is not implemented.
- Offline v2 import, consistent backup, and restore to a fresh directory and new ID.

The repository deliberately does not implement the old whole-manifest `catalog.Store`
interface. Its operations are `CreateSQLite`, `OpenSQLite`, `Status`, `PublishWindow`,
`Select`, `ImportV2`, `Backup`, and `RestoreSQLite`. Publication assumes the caller
has verified source completeness and uploaded objects. The catalog does not contact
VM or independently prove that an empty publication represents an empty source.

## Build and initialization

Use normal bootstrap/build commands. Bootstrap verifies the pinned `go-sqlite3
v1.14.32` archive and generates vendor files. CGO compiles bundled SQLite, so a C
compiler is required. The default build needs no separate SQLite service or system
SQLite library. See [third-party notices](../THIRD_PARTY_NOTICES.md).

Use a valid existing-format configuration for archive policy and blob location.
The usual tenant/source, timing, and `lateness_enforced` checks apply. Initialization
does not prove capacity or enforce ingestion policy. S3 credentials and connectivity
are not needed to initialize or inspect catalog metadata.

```sh
.bin/vmtier catalog-init -backend sqlite \
  -config config.json -path .state/sqlite-catalog -id catalog-sqlite-001
.bin/vmtier catalog-inspect \
  -path .state/sqlite-catalog -id catalog-sqlite-001
```

The explicit `-id` identifies the new catalog. Legacy `catalog.type` and connection
fields do not select a SQLite runtime. Initialization binds the contract and creates
per-stream checkpoints at coverage start; it does not create archived coverage.

Inspect a bounded reference plan with half-open millisecond timestamps, replacing
these example times with an archived interval:

```sh
.bin/vmtier catalog-inspect -path .state/sqlite-catalog -id catalog-sqlite-001 \
  -tenant 1:2 -start 1791244800000 -end 1791266400000 \
  -max-windows 100 -max-packs 200
```

The result contains references and complete coverage end, **not metric query results
or an assertion that the requested interval is fully covered**. The future reader
must still apply age/coverage rules and verify object contents.

Default selection caps are 4,096 windows, 4,096 packs, and 16 MiB of descriptors.
Internal API maxima are 100,000 windows/packs and 64 MiB of plan budget. One window
has at most 1,024 references, each subject to 64 MiB data / 16 MiB index limits.
These are resource bounds, not performance guarantees. SQLite does not retain the
old 10,000-entry archive lifetime limit.

## Import JSON v2

Stop the old archiver and catalog service, retaining their state and objects for
rollback. The importer reads the local v2 directory, not its HTTP API. Configuration
`catalog.id` pins the source identity, and the configured policy must match it.
The destination must not exist; its parent must already exist.

```sh
mkdir -p .state
.bin/vmtier catalog-import -config config.json \
  -from .state/json-catalog -path .state/imported-sqlite -id catalog-sqlite-002
.bin/vmtier catalog-inspect -path .state/imported-sqlite -id catalog-sqlite-002
```

The importer checks every referenced index's checksum, codec, tenant, and logical
window, one index at a time. It records the index's data checksum/size without a full
historical payload scrub. Existing objects are not rewritten. Each v2 entry becomes
a one-pack window, including legacy zero-byte empty packs. SQLite generations count
imported stream windows; they do not continue the old authority's generation counter.
Coverage and references are the migration invariants.

Unbound state, policy mismatch, gaps, bad indexes, and existing destinations fail.
Staging remains private until the whole import succeeds. Gap repair and all-S3 v1
migration are not automated. Keep the old writer stopped during the import: v2's
file adapter does not provide a lifetime process lock. After inspection, resume the
existing JSON archiver/catalog deployment for actual queries and archival. The
SQLite candidate is then only a point-in-time snapshot; import fresh state before
a future cutover. Runtime cutover and source catch-up await service integration.

## Offline backup and restore

Stop all owners of the SQLite directory. A second offline command cannot open owned
state. Backup and restore do not need S3 access:

```sh
mkdir -p .state/backups
.bin/vmtier catalog-backup -path .state/imported-sqlite -id catalog-sqlite-002 \
  -out .state/backups/catalog-001
.bin/vmtier catalog-restore -backup .state/backups/catalog-001 \
  -path .state/restored-sqlite -id catalog-sqlite-003
```

Backup creates a fresh directory containing `catalog.db` and `metadata.json`.
SQLite `VACUUM INTO` creates the image; metadata records identity, generation,
contract, coverage, and checksum. Copy the complete backup to another failure
domain. A copy of a live database's main file may omit WAL contents.

Restore checks database length/checksum, schema, metadata, contiguous coverage,
publication digests, and pack references. A new ID and nonexistent destination are
required. Existing state is never overwritten. Restore does not contact S3, scrub
payloads, discover orphans, or recover commits missing from an older backup.
Repository tests exercise restored publication/catch-up mechanics, not VM recovery.

Operational recovery still requires backup age plus restore/catch-up time to fit Hot
headroom. A backup cannot restore expired source history. Unsupported schema versions
are rejected; no schema upgrade exists yet.

## Remaining scope

This is a single-host component requiring correct local filesystem locks and sync.
A directory lock does not exclude another host or directory targeting the same
bucket. HA, distributed fencing, online backups, and automatic restore are absent.

The new repository represents multiple packs and independent streams. The MVP
archiver still emits one pack per stream/window and publishes JSON across configured
streams. Pack rotation, shared S3 throttling, LRU caching, range coalescing, and
runtime use of this repository remain separate work.
