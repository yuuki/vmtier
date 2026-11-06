# Configuration examples

Use [Getting started](../docs/getting-started.md) for a self-contained demo; the
harness generates its own configuration. These files are templates for an
operator-managed deployment and deliberately require editing before use. Run
[capacity preflight](../docs/preflight.md) with your candidate SQLite configuration
before initialization; optional pack and query limits are described in the
[service configuration reference](../docs/serve.md#configuration-reference).

| File | Runtime | Object backend |
| --- | --- | --- |
| [serve-sakura.json](serve-sakura.json) | Current SQLite `vmtier serve` | Sakura S3 endpoint with a shared request budget |
| [config.json](config.json) | Legacy JSON v2 `archive` / `gateway` | S3-compatible loopback endpoint |
| [sakura.json](sakura.json) | Legacy JSON v2 `archive` / `gateway` | Sakura S3 endpoint |

For the current runtime, copy `serve-sakura.json` to your own configuration file
and follow the [service guide](../docs/serve.md). Set the bucket, prefix, catalog
path and ID, tenants, Hot-only source URLs, and archive/retention policy for your
deployment. Create the bucket separately. Sakura is a target provider; actual
service compatibility and performance remain unqualified.

All templates have `coverage_start_ms: 0` and `lateness_enforced: false`.
Choose a positive, UTC window-aligned coverage origin while the source history
is still queryable. Set `lateness_enforced` only after every ingestion path enforces
the configured lateness bound; the flag does not enforce it. Duration values use
Go syntax, such as `720h`, not `30d`.

Supply S3 credentials through `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and,
when needed, `AWS_SESSION_TOKEN`. Do not add credentials to JSON or commit local
configuration files containing deployment details. Relative filesystem paths are
resolved from the process's working directory, not the configuration file's directory.

To use local blobs with SQLite in development, replace the entire `store` object
with `{"type":"local","path":".state/objects"}`. Remove S3 request-budget fields;
they are invalid for local storage. Keep `catalog.type` as `sqlite` and use a
catalog directory distinct from the blob directory. Local blobs do not provide
an S3 durability guarantee.

`catalog.type: "local"` means the **legacy JSON catalog** on disk; it does not
select SQLite. The standalone `archive` and `gateway` commands reject SQLite
configurations. Use [legacy operations](../docs/operations.md) only for the JSON
v2 examples, and [catalog maintenance](../docs/sqlite-catalog.md) to migrate an
existing archive.
