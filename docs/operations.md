# Operations and recovery

This runbook covers shared service behavior and the backend-specific maintenance
boundaries. Start with [getting started](getting-started.md), the
[SQLite service](serve.md), or the [PostgreSQL service](postgres-catalog.md).
For multiple gateways, database failover, and archiver fencing, follow
[HA deployment](ha-deployment.md).
Separate catalog/archiver/gateway commands and manual JSON state recovery belong
to the [legacy JSON v2 guide](legacy-json-v2.md).

## Deployment requirements

For SQLite, use persistent local catalog storage and one owner for the bucket.
Its directory lock only excludes owners using that directory on the same host.
For PostgreSQL, use a stable writable-primary endpoint, shared S3, and one logical
redundant Cold RPC endpoint. The database lease fences archiver publication; the
operator must separately enforce database-primary fencing and the maximum number
of S3 clients sharing the configured budget. Keep RPC and HTTP listeners on a
trusted network with access controls and TLS termination as appropriate. Neither
service backend exposes a catalog HTTP mutation API.

Archive sources must be Hot-only stock vmselect endpoints covering all Hot shards.
Keep tenant IDs, source IDs/URLs, and shard topology stable. Enforce the lateness
bound on every ingestion path before setting `lateness_enforced=true`. Block
mutation APIs at the query frontend. Validate the [time and coverage contract](limits.md)
and run [preflight](preflight.md) before initializing a catalog.

## Maintenance and backup

For SQLite, stop `serve` before inspection, payload verification, import, backup,
or restore.
Use the [offline catalog commands](sqlite-catalog.md); they acquire the same lock
and refuse a live owner. Allow enough Hot retention headroom for downtime and
catch-up. Copy complete backups to another failure domain. Do not copy only the
main file of a live SQLite database: committed state can still be in the WAL.

Restore into a fresh directory with a new catalog ID, preserve the saved contract
and blob locator, and isolate the old owner before restarting. The scheduler
rearchives missing windows only while Hot still retains them. Metadata backups
cannot recover missing object bytes or expired source data. Compare Cold-only
and Hot/Cold boundary queries before cutover; see [recovery evidence](recovery-validation.md)
for the tested scenarios and their limits.

PostgreSQL `inspect` and `verify` can run online. Reserve an S3 budget share for
verification; it checks the initially observed coverage while archiving can
continue. Use PostgreSQL native backups and WAL/PITR, test restoration, and fence
the old authority before promoting or switching databases. Database replication
is not a backup, and a metadata restore cannot restore missing payloads. For
SQLite-to-PostgreSQL cutover, use the [stopped-writer migration](postgres-migration.md);
a stale SQLite catalog cannot resume after PostgreSQL publication advances.

## Sakura Object Storage

The deployment requirement assumes conditional PUT is unavailable on Sakura Object Storage; this is not a claim that the public API documentation specifies every conditional-header behavior. vmtier sends no conditional PUT and does not use ETags for correctness.

The adapter sends Content-MD5 in line with the checksum scope in the [official API documentation](https://manual.sakura.ad.jp/cloud/objectstorage/api.html). It independently verifies stored content with read-back GET and SHA-256. The [April 9, 2026 announcement](https://cloud.sakura.ad.jp/news/2026/04/09/objectstorage-checksum/) reports removal of earlier SDK checksum compatibility restrictions; an older AWS SDK is not a deployment requirement.

The documented bucket limits are 100 requests/second and 100 concurrent requests for the [standard plan](https://manual.sakura.ad.jp/cloud/objectstorage/about.html), and 10 requests/second with one concurrent request for the [archive plan](https://manual.sakura.ad.jp/cloud/objectstorage/archiveplan.html). The legacy separate-process MVP cannot share a bucket request budget across gateways and archivers. The SQLite `serve` runtime now shares one paced budget across archival, reads, metadata, verification, and every SDK retry attempt, defaulting to 80 requests/second, eight in flight, and 64 queued requests. An archive-plan candidate is eight requests/second, one in flight, and 64 queued; it remains unqualified on the actual provider. All clients using the bucket must fit the aggregate provider limits; a dedicated prefix alone does not isolate them.

For PostgreSQL, the configured S3 totals are aggregate budgets divided by
`service.max_replicas`. Include rolling replacements and online verification in
that maximum; static allocation does not discover or exclude extra clients. See
[budget allocation](ha-deployment.md#static-s3-budget-allocation) for rounding, pacing,
and failure behavior.

No actual Sakura bucket or credentials have been used to validate connectivity, SigV4, visibility after PUT, Range GET, quotas, recovery, or performance. The local Moto backend and proxy that rejects conditional requests and removes ETags approximate these API constraints; they are not service certification. See the [SQLite service example](../examples/serve-sakura.json), [service budget metrics](serve.md), and [legacy JSON v2 example](../examples/sakura.json).

## Monitoring

Liveness reports a running process. Readiness checks configured coverage and retention headroom, but does not prove that actual VM retention and ingestion rules match the configuration. Monitor archive outcomes, last commit, unarchived windows, lag, retention headroom, object requests/bytes/errors, gateway failures, and query-limit rejections.

The gateway exposes `/health`, `/ready`, and `/metrics`. `/ready` returns HTTP 503 unless contiguous coverage for every tenant and source reaches `floor((now-H)/window)*window`. Reading an old catalog successfully is insufficient. A query requiring an unarchived interval fails; a query entirely within previously committed coverage can still work while archival of newer windows is delayed.

| Metric | Meaning |
| --- | --- |
| `vmtier_ready` | Result of catalog and coverage checks. |
| `vmtier_coverage_end_ms{tenant=...}` | Contiguous coverage end across all configured sources. |
| `vmtier_archive_lag_seconds{tenant=...}` | Difference from the currently required archive boundary. |
| `vmtier_retention_headroom_seconds{tenant=...}` | `coverage_end + R - now`, using configured retention rather than an automatically read VM setting. |
| `vmtier_query_requests_total`, `vmtier_query_errors_total`, `vmtier_blocks_total` | Gateway requests, errors, and returned blocks; metadata uses the same read path. |
| `vmtier_object_get_requests_total`, `vmtier_object_range_requests_total`, `vmtier_object_download_bytes_total`, `vmtier_object_put_requests_total` | Per-process object-store I/O. |
| `vmtier_cache_hits_total`, `vmtier_cache_misses_total` | Immutable index and block cache activity. |

For the legacy one-shot archiver, an external scheduler collects structured JSON
logs, exit code, and standard output. Both service backends expose archive metrics
directly; PostgreSQL also exposes `vmtier_archive_leader` for local lease ownership.
Metrics also consult the catalog. Monitor catalog health, persistent-disk capacity
and I/O errors, and backup status separately.

Retention headroom is the finite time until the oldest unarchived sample can no longer be queried or exported. An alert does not extend it. VM can exclude samples through its retention filter even while their physical parts remain on disk, so physical file existence is not extra recovery time.

### Service capacity and failure accounting

Both service backends expose cache, query admission, and failed-window signals.
Catalog size/count/committed-byte observations below are implemented for SQLite
only; PostgreSQL reports `vmtier_catalog_capacity_available=0` and omits those
unavailable catalog gauges. Use PostgreSQL monitoring for its storage usage.
These observations do not list the blob backend:

| Metric | Accounting |
| --- | --- |
| `vmtier_catalog_file_bytes{file="database\|wal\|shared_memory"}` | Apparent local file lengths, including reusable/free pages; not filesystem allocated blocks. WAL and shared-memory files may be absent and then contribute zero. |
| `vmtier_catalog_committed_object_bytes{kind="data\|index"}` | Sum of byte lengths in committed pack descriptors, retained across restart. Excludes unreferenced/failed attempts, other prefixes, provider versions, and provider overhead; this is **not measured bucket usage**. |
| `vmtier_catalog_windows`, `vmtier_catalog_packs` | Committed logical windows (including empty windows) and immutable pack references. |
| `vmtier_catalog_capacity_available` | One only when the current bounded observation succeeds. Capacity gauges are omitted on failure, rather than reporting stale values or false zeroes. |
| `vmtier_cache_used_bytes`, `vmtier_cache_limit_bytes`, `vmtier_cache_entries`, `vmtier_cache_inflight_reads` | Retained immutable raw bytes plus keys and a conservative per-entry bookkeeping allowance; excludes allocator/GC overhead, transient parsed indexes and active-query allocations. In-flight shared reads are bounded separately. |
| `vmtier_query_memory_limit_bytes`, `vmtier_query_work_bytes`, `vmtier_query_memory_reserved_bytes` | Shared admission allowance, reservation per accepted query, and currently reserved work. These are accounting limits, not measurements of process RSS. |
| `vmtier_query_limit_refusals_total{reason=...}` | Resource refusals with a fixed reason vocabulary: `memory_admission`, `shared_reads`, `read_bytes`, `decoded_bytes`, `catalog_plan`, `blocks`, `series`, or `response_bytes`. Other backend failures and cancellations do not masquerade as limit refusals. |
| `vmtier_archive_failed_windows_total{tenant=...,source=...,reason=...}` | Errored attempts for configured tenant/source pairs only; reasons are `error`, `timeout`, and `canceled`. A retry increments again; skipped/backoff windows do not. Shutdown cancellation is counted if a window was attempted. No timestamp or error-message labels are used. |

Attempt/refusal counters and cache/admission state start over on process restart;
committed totals are read afresh from SQLite. An errored attempt is not necessarily
a new missing window: failure after commit acknowledgement can leave the window
committed, so interpret failures together with coverage and lag. The service does
not estimate orphan bytes or perform automatic GC. Use provider accounting for
actual bucket growth and host monitoring for disk availability and peak RSS.
Each capacity scrape aggregates the complete `packs` and `windows` tables on
the catalog's single SQLite connection. The shared five-second scrape context
bounds the observation time, not the number of rows scanned. Large catalogs or
frequent scrapes can therefore contend with publication and query selection;
include scrape load in the catalog-capacity qualification tracked by
[#14](https://github.com/yuuki/vmtier/issues/14).
Compare rising failed-window/refusal rates with declining retention headroom
before increasing workloads or resource limits. These signals do not extend the
time available to recover an unarchived suffix.

Cache and query-admission/refusal metrics are also available in the retained
JSON v2 gateway. Catalog capacity totals describe SQLite only; failed-window
metrics also describe the active PostgreSQL archiver. The SQL capacity totals use
a constant-memory scan of committed descriptors under the scrape timeout; they
do not add an in-memory catalog mirror.

## Failure and restart behavior

| Failure | Response |
| --- | --- |
| Export timeout, missing source, or interrupted native stream | Do not advance the catalog. Restore the source and retry while Hot can still export the data. |
| Native export truncated at a record boundary, or a missing conflicting-timestamp block | EOF and counts alone do not establish completeness. The independent raw JSON matrix comparison checks every series, timestamp, and value and rejects mismatches. |
| Upload or read-back verification failure | Uncommitted objects are invisible to queries. Retry the archive. |
| Catalog generation conflict | Reload identity, generation, and coverage and reconcile the attempted window. Investigate unintended writers if conflicts persist. Do not replace CAS with an unconditional catalog overwrite. |
| PostgreSQL archiver lease expires or renewal fails | Cancel that owner's archive attempt; committed reads remain available. A successor must acquire a new fencing token before publication. |
| PostgreSQL primary unavailable or endpoint points to a standby | Fail authoritative reads and writes. Restore routing to an eligible writable primary using the deployment's fencing and promotion procedure. |
| Process stops just after commit | Reload the authoritative catalog. Do not infer coverage from the presence or absence of a success log. |
| Missing or corrupt object, or GET failure | Fail the query. Restore the original bytes, verify them, and retest; do not replace the failure with an empty result. |
| Hot retention deadline exceeded | Do not commit an empty window as success. Missing data needs another complete recovery copy. |

Published object keys are never reused. A new attempt uses a fresh nonce and keys, so a lost PUT response or generation conflict can leave orphan objects. Queries read only catalog references. After a lost commit response, reload the authority before deciding what remains to publish. vmtier supports bounded, single-PUT objects; multipart abort and cleanup are out of scope.

## Late writes, backfill, and deletion

The `H > L` contract assumes accepted writes arrive before a window is archived. If an older sample is written to Hot afterward, vmtier does not guarantee that it will survive in Cold once Hot retention removes it. A read gateway cannot detect or reject violations on a direct write path.

There are no deltas, overlap re-exports, backfill updates, or tombstones in vmtier. Treat committed windows as immutable. If exceptional backfill is necessary, rebuild the complete archive under a **new prefix** from an isolated source holding the corrected full history, compare it with the reference, and switch to a new gateway with VM result caching disabled. This is a manual migration approach, not a tested automatic backfill command.

Although the gateway rejects delete, write, and administration RPCs, a delete sent through the top-level vmselect can modify Hot before Cold returns an error. Block the read frontend's HTTP mutation APIs at the access-control layer too. Deleting a series only from Hot can make it reappear from Cold. Environments requiring deletion need tombstones or a safe archive rebuild; vmtier does not provide deletion management.

## Retention changes and rollback

vmtier does not shorten Hot retention automatically. Before changing retention, check contiguous coverage for all tenants and sources, query the affected range through Cold alone, exercise failure responses, verify the lateness contract and recovery copies, and assess worst-case archive lag. These checks do not create protection against an unbounded outage.

If Hot still holds the entire history, removing the Cold node restores a Hot-only frontend. If older history is absent from Hot, removing the gateway is not a complete rollback: restore and validate the data in an isolated VM with sufficient retention before switching. Do not restore into an active vmstorage data directory.

## GC, Cold retention, and upgrades

vmtier performs no GC or Cold TTL deletion. Its append-only catalog retains existing entries, and it deletes neither referenced objects nor orphans. This avoids deletion races but allows storage usage to grow. An `inspect` result alone is not permission to delete an object, and an S3 lifecycle rule must not delete objects referenced by the current catalog. Durable catalog backups and an active query's in-memory snapshot serve different purposes.

Any future deletion implementation must account for query lifetime and all readers that may still hold old references. That implementation is deferred; do not approximate it with an arbitrary lifecycle expiration.

Follow the [compatibility upgrade procedure](compatibility.md) for codec, RPC, and reference-version checks. Packs carry revision checks and are not automatically migrated by replacing the binary.
