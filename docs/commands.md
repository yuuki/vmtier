# Command reference

Build `.bin/vmtier` using [getting started](getting-started.md). The command table
reflects dispatch in [`cmd/vmtier`](../cmd/vmtier); use `COMMAND -h` to inspect flags.
The CLI uses Go flag parsing and returns a nonzero status for help as well as
errors. `version` prints the build version and commit, Go version, catalog/schema,
and pinned VM versions.

| Command | Backend and purpose | Procedure |
| --- | --- | --- |
| `serve -config FILE` | SQLite or PostgreSQL: scheduler, read RPC, health and metrics | [SQLite](serve.md), [PostgreSQL](postgres-catalog.md) |
| `preflight -config FILE -start MS -end MS` | SQLite/PostgreSQL candidate policy: verify one source window without publication | [Preflight](preflight.md) |
| `catalog-init -backend sqlite -config FILE -path DIR -id ID` | Explicit SQLite initialization | [Initialization](sqlite-catalog.md#build-and-initialization) |
| `catalog-init -backend postgres -config FILE -id ID` | Explicit PostgreSQL initialization | [PostgreSQL](postgres-catalog.md#configuration-and-lifecycle) |
| `inspect -config FILE` | SQLite/PostgreSQL status or JSON v2 manifest | [SQLite](sqlite-catalog.md), [PostgreSQL](postgres-catalog.md) |
| `verify -config FILE` | Validate referenced objects and native blocks | [SQLite](sqlite-catalog.md), [PostgreSQL](postgres-catalog.md) |
| `catalog-inspect -path DIR -id ID` | Offline SQLite status and optional bounded reference plan | [Inspection](sqlite-catalog.md#build-and-initialization) |
| `catalog-import` | Offline local JSON v2 to new SQLite authority | [Import](sqlite-catalog.md#import-json-v2) |
| `catalog-inspect -backend postgres -config FILE` | Online PostgreSQL status and optional bounded reference plan | [PostgreSQL](postgres-catalog.md) |
| `catalog-upgrade -backend postgres -config FILE` | Explicit PostgreSQL schema upgrade (1 or 2 to 3) with old writers stopped | [Upgrade](postgres-catalog.md#configuration-and-lifecycle) |
| `catalog-upgrade -path DIR -id ID` | Offline SQLite schema upgrade (1 to 2) | [Upgrade](sqlite-catalog.md#upgrade-schema-1) |
| `catalog-add-streams -config FILE` | Add the configuration's new tenants and sources (SQLite offline, PostgreSQL online) | [Add streams](serve.md#add-tenants-or-sources) |
| `catalog-import-postgres -config FILE -from DIR -source-id ID` | Stopped SQLite metadata to fresh PostgreSQL catalog/new ID | [Migration](postgres-migration.md) |
| `catalog-backup`, `catalog-restore` | Offline SQLite backup and restore under a new ID | [Recovery](sqlite-catalog.md#offline-backup-and-restore) |
| `catalog-init -path DIR -id ID` | JSON v2 initialization (default backend) | [Legacy deployment](legacy-json-v2.md) |
| `catalog -path DIR -id ID` | JSON v2 HTTP authority | [Legacy deployment](legacy-json-v2.md) |
| `archive -config FILE` | JSON v2 one-shot archive; optional `-start MS -end MS` | [Legacy deployment](legacy-json-v2.md) |
| `gateway -config FILE` | JSON v2 read RPC and health/metrics server | [Legacy deployment](legacy-json-v2.md) |
| `version` | Build identity, format/schema, and VM compatibility identity | [Releases](releasing.md), [compatibility](compatibility.md) |

Stop `serve` before opening its SQLite directory with any maintenance command.
PostgreSQL inspection and verification can run online, with a reserved share of
the aggregate S3 budget for verification; backups use PostgreSQL native tooling.
`preflight` does not open a catalog. `archive`, `gateway`, and the catalog HTTP
service cannot be substituted for either service backend.

Time-range flags use UTC epoch **milliseconds** and half-open `[start,end)` bounds.
Preflight requires exactly one configured tenant/source and one logical window;
select `-tenant` and `-source` when the configuration contains several. See the
linked guides for required flags and prerequisites of import, backup, and restore.

Configuration is strict JSON: unknown fields and trailing data are rejected.
Use the [SQLite](../examples/serve-sakura.json) or
[PostgreSQL](../examples/serve-postgres.json) configuration template;
[service settings](serve.md), [archive bounds](archive-packs.md), and
[memory accounting](query-memory.md) explain effective defaults. Credentials belong
in the environment. Examples deliberately require a real coverage origin and
externally enforced lateness before they can run.
