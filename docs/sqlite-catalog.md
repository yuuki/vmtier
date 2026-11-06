# SQLite catalog, maintenance, and migration

SQLite is the catalog backend for the single-owner [`serve` runtime](serve.md).
The service owns the repository, archive scheduler, and read RPC gateway for its
lifetime. Catalog initialization, import, backup, restore, inspection, and payload
verification are **offline operations**: stop `serve` before opening its state
directory with another command.

Use `catalog.type="sqlite"` with an explicit state path and ID for `serve`, `inspect`,
and `verify`. Standalone `archive` and `gateway` reject SQLite configurations; they
remain part of the legacy JSON v2 deployment. The catalog HTTP service also remains
a JSON v2 tool. SQLite has no public catalog HTTP API.

The [stage 1 validation](sqlite-catalog-validation.md) records the original repository
and offline-command checks. Current runtime evidence and unexecuted checks are in
[service validation](serve-validation.md); the remaining implementation stages are
tracked in the [first-release design](design.md).

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

Use a valid configuration for archive policy and blob location, such as the
[SQLite service example](../examples/serve-sakura.json).
The usual tenant/source, timing, and `lateness_enforced` checks apply. Initialization
does not prove capacity or enforce ingestion policy. S3 credentials and connectivity
are not needed to initialize or inspect catalog metadata.

```sh
.bin/vmtier catalog-init -backend sqlite \
  -config config.json -path .state/sqlite-catalog -id catalog-sqlite-001
.bin/vmtier catalog-inspect \
  -path .state/sqlite-catalog -id catalog-sqlite-001
```

The explicit `-id` identifies the new catalog. For runtime use, set the configuration
to the same path and ID:

```json
{
  "catalog": {
    "type": "sqlite",
    "path": ".state/sqlite-catalog",
    "id": "catalog-sqlite-001"
  }
}
```

Initialization binds the archive contract and normalized blob locator and creates
per-stream checkpoints at coverage start; it does not create archived coverage.
`serve` never initializes missing state. A different contract, ID, or blob locator
fails startup before the S3 client is opened.

Inspect a bounded reference plan with half-open millisecond timestamps, replacing
these example times with an archived interval:

```sh
.bin/vmtier catalog-inspect -path .state/sqlite-catalog -id catalog-sqlite-001 \
  -tenant 1:2 -start 1791244800000 -end 1791266400000 \
  -max-windows 100 -max-packs 200
```

The result contains references and complete coverage end, **not metric query results
or an assertion that the requested interval is fully covered**. The service reader
applies age/coverage rules and verifies fetched object contents separately.

The configuration-based maintenance commands are also available while `serve` is
stopped:

```sh
.bin/vmtier inspect -config config.json
.bin/vmtier verify -config config.json
```

`inspect` reports SQLite status. `verify` performs a full historical payload scrub
through bounded selections, one logical time window at a time, including source
windows ahead of a tenant's minimum coverage. Neither command can bypass the state
directory's lifetime ownership lock. A full scrub can take significant time and
S3 requests; allow enough Hot headroom for maintenance and subsequent catch-up.

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
file adapter does not provide a lifetime process lock.

To cut over after inspection:

1. Create a service configuration with `catalog.type="sqlite"`,
   `catalog.path=".state/imported-sqlite"`, and `catalog.id="catalog-sqlite-002"`.
   Preserve the imported archive contract and blob locator, including source IDs
   and URLs, tenants, timing, coverage start, and object-store location.
2. Keep the old writer stopped and start `vmtier serve -config FILE` against the
   imported state. The scheduler resumes from each stream's committed coverage.
   Missing eligible windows can be caught up only while Hot still provides them
   within the configured retention safety margin.
3. Check `/ready`, coverage, Cold-only queries, and Hot/Cold boundary queries before
   switching the query frontend to the new listener or considering a retention
   change. vmtier never changes Hot retention itself.

If the old JSON deployment is resumed instead, the imported database is only a
point-in-time snapshot. Import fresh state under a new ID before a later cutover.
Retain the old authority and objects until rollback and recovery needs are resolved.

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
Repository tests exercise restored publication/catch-up mechanics; those checks do
not establish recovery from a real VM or object-store disaster.

Set a service configuration to `catalog.type="sqlite"`, the restored path, and
the new ID, preserving the saved contract and blob locator. Once the old owner is
isolated, start `serve`. Its scheduler automatically resumes from restored coverage
and rearchives the missing suffix while Hot retention permits. `/ready` remains
unsuccessful until all required streams catch up. Restoring the metadata alone
does not restore missing object bytes or expired Hot history.

Operational recovery still requires backup age plus restore/catch-up time to fit Hot
headroom. A backup cannot restore expired source history. Unsupported schema versions
are rejected; no schema upgrade exists yet.

## Remaining scope

This is a single-host component requiring correct local filesystem locks and sync.
A directory lock does not exclude another host or directory targeting the same
bucket. HA, distributed fencing, online backups, and automatic restore are absent.

The repository and reader support multiple packs and independent streams. The
SQLite archiver publishes independently per tenant/source/window, emits zero packs
for a verified empty window, and still emits one pack for a nonempty window. The
service shares one S3 request budget across archival and reads. Streaming pack
rotation, incremental oracle parsing, LRU caching, and range coalescing remain
separate implementation stages. Actual Sakura compatibility, capacity, natural Hot
retention transitions, and deployment ingress enforcement still require testing.
