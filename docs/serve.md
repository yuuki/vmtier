# Run the SQLite service

For an external catalog with renewable archiver ownership, see the
[PostgreSQL service guide](postgres-catalog.md). This guide remains specific to
the SQLite single-owner deployment.

`vmtier serve` runs the archiver, SQLite catalog, and Cold read gateway in one
process. They share one blob-store client and its S3 request budget. The catalog
is independent of S3: no conditional PUT, ETag CAS, or LIST-based discovery is used.

The service includes bounded pack rotation, incremental oracle verification,
capacity preflight, LRU caching, adjacent-range reads, and query work admission.
It remains under development: the [first-release design](design.md) tracks workload,
provider, retention, ingress, recovery, and release qualification. The
[stage 2 validation](serve-validation.md) is historical evidence for its recorded
revision; new implementation and focused tests do not replace that provenance.
GC remains unimplemented. This SQLite deployment has no automatic failover;
[PostgreSQL HA](ha-deployment.md) is the separate multi-instance option.

## Configuration and startup

Build using [getting started](getting-started.md). Copy [the Sakura configuration](../examples/serve-sakura.json)
and set the real bucket, fixed Hot-only source URLs, tenants, archive policy, and
window-aligned `coverage_start_ms`. The example deliberately has an invalid coverage
start and `lateness_enforced=false`; do not enable that assertion until all ingestion
paths actually enforce the lateness limit. Source data must still be queryable in Hot.
Run [capacity preflight](preflight.md) against representative busy windows before
initialization fixes their width in the catalog contract.

```sh
cp examples/serve-sakura.json serve.json
# Edit serve.json for your deployment before initializing the catalog.
```

Create the S3 bucket beforehand. Supply credentials through `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY`, plus `AWS_SESSION_TOKEN` when needed. The S3 client
reads these environment variables directly; it does not load AWS profiles or use
instance-role discovery. Never put credentials in configuration files or source URLs.

The service settings are:

```json
{
  "catalog": {
    "type": "sqlite",
    "path": ".state/catalog-sqlite",
    "id": "sakura-serve-001"
  },
  "service": {
    "archive_interval": "30s",
    "archive_timeout": "2m"
  }
}
```

This fragment belongs in the full configuration, not in a standalone file. Create
the durable parent directory, initialize the authority explicitly, then start the service:

```sh
mkdir -p .state
.bin/vmtier catalog-init -backend sqlite -config serve.json \
  -path .state/catalog-sqlite -id sakura-serve-001
.bin/vmtier serve -config serve.json \
  -listen 127.0.0.1:8489 -http 127.0.0.1:8490
```

The initialization path/ID must match the service configuration. Startup refuses
missing state, another owner, wrong identity, or a changed contract/blob locator.
Startup does not silently create a catalog or contact S3 before those checks.
Relative catalog and local blob paths resolve against the process working directory,
not the configuration file's directory. Use stable absolute paths for a supervised
service so startup and offline maintenance refer to the same state.

Keep the state directory on persistent local storage. The lock covers this directory
on this host, not another host or another directory targeting the same bucket.
Run only one owner for the bucket. No automatic failover is provided.

## Configuration reference

Configuration is one JSON object; unknown fields and trailing data are rejected.
Durations use Go syntax such as `30s`, `6h`, or `720h` (not `30d`) and must be whole
milliseconds. Time intervals are half-open, `[start,end)`, in UTC epoch milliseconds.
The table covers SQLite; PostgreSQL adds the catalog connection, lease, and replica
settings in its [configuration guide](postgres-catalog.md#configuration-and-lifecycle).

| Fields | Meaning and constraints |
| --- | --- |
| `catalog.type`, `catalog.path`, `catalog.id` | Use `sqlite`, a persistent local directory, and an explicit ID. IDs contain 1–128 ASCII letters, digits, `.`, `_`, or `-`; `.` and `..` are invalid. The initialized ID must match. |
| `store.type` | `s3` or `local`. Local blobs require `store.path`, distinct from the catalog directory, and no S3 fields or request limits. |
| `store.endpoint`, `store.region`, `store.bucket`, `store.prefix` | S3 location. Bucket is required. Omitted endpoint uses the AWS endpoint; omitted region is `us-east-1`; omitted prefix uses the bucket root. Custom endpoints are HTTP(S) origins without credentials, query, or path. The client uses path-style addressing. |
| `tenants` | 1–100 unique account/project IDs, for example `"1:2"`. |
| `sources` | 1–100 unique `{ "id": "hot", "url": "http://127.0.0.1:8481" }` entries. Each URL is a Hot-only vmselect base URL covering all shards for that source. IDs use the catalog ID character rules. |
| `archive_age`, `lateness`, `hot_retention`, `safety_margin`, `window` | Required timing policy: `H > L >= 0`, positive safety margin and window, and `R > H + safety_margin + window`. Set retention to match the actual Hot deployment. |
| `coverage_start_ms` | Required positive epoch milliseconds, divisible by the window width in milliseconds. Choose the earliest complete window still available from Hot; history before it is outside the coverage guarantee. |
| `lateness_enforced` | Must be `true` only after every ingestion path enforces `lateness`. This is an operator assertion, not an ingestion control. |
| `cache_bytes` | Immutable raw index/block LRU cache allowance in bytes, including key/bookkeeping charges; omitted or zero disables retention. Maximum 256 MiB. Parsed indexes and shared-read buffers have separate accounting. |
| `max_query_bytes` | Per-query read/decoded-work allowance in bytes; omitted or zero uses 128 MiB. Maximum 1 GiB, subject to the shared allowance below. This is not a process RSS limit. |
| `query_memory_bytes` | Shared query work reservations; omitted or zero uses 256 MiB. Must cover one effective `max_query_bytes` reservation, at most 8 GiB. See [query work admission](#query-work-admission). |
| `max_pack_blocks` | Optional per-pack block limit, 1–100,000; omitted or zero uses 100,000. Changes packing, not window capacity or the immutable contract. See [archive packing](archive-packs.md). |
| `service.archive_interval`, `service.archive_timeout` | Default `30s` and `2m`; each accepts 10 ms through 24 hours. See [scheduling](#scheduling-and-publication). |
| `store.requests_per_second`, `store.max_concurrent_requests`, `store.max_queued_requests` | Shared S3 limits; see [shared S3 budget](#shared-s3-budget). |

Initialization pins the coverage start, window, archive age, lateness, tenant set,
source IDs/URL fingerprints, 1 ms deduplication, and normalized blob location.
Changing pinned values requires rebuilding under a new catalog and object prefix;
backend import commands preserve the archive contract and object location.
Restarting with different values fails. Shard topology behind a source URL must
also stay fixed, although the gateway cannot detect a topology change.

Cache/query budgets, `max_pack_blocks`, scheduling intervals, and S3 limits can
change on restart. `hot_retention` and `safety_margin` are validated runtime
settings rather than persisted contract fields; changes must still reflect the
real Hot policy and leave enough time to archive and recover.

## Connect stock vmselect

Configure the [stock vmselect Hot/Cold frontend](getting-started.md#connect-the-query-frontend) with
`-replicationFactor=1 -globalReplicationFactor=1 -search.skipSlowReplicas=false
-search.denyPartialResponse=true -search.disableCache=true -dedup.minScrapeInterval=1ms`.
The gateway RPC address is a storage node alongside Hot; the archiver's source must
be a separate Hot-only vmselect. Do not export from the combined frontend.

Label/series metadata uses the same validated block search as sample queries.
[LRU caching, shared misses, and adjacent-range reads](reader-validation.md) reduce
repeated object I/O while retaining payload validation and actual sample-time
filtering. The earlier uncached S3 fixture exceeded stock vmselect's default
five-second labels API deadline. The [before/after measurements](metadata-validation.md)
record passing that deadline on their JSON and SQLite fixtures after the reader
changes; their recorded revision does not validate a later VM upgrade. The service E2E harness retains its configurable 30-second default; no
30-second override was needed for these measured final runs.
No metadata-specific index-only shortcut is implemented.

Size the deadline for the deployment's workload and S3 budget; a longer deadline
does not improve throughput. The gateway caps individual requests at 30 seconds,
so increasing the frontend setting further cannot remove that limit. Larger time
ranges or cardinality can still time out. Focused GET-count improvements do not
establish an arbitrary-workload metadata latency guarantee.

## Scheduling and publication

The first archive pass starts immediately. Each pass attempts at most one oldest
eligible window per tenant/source, ordered by coverage end. Successful progress
triggers another pass immediately, allowing catch-up without one fixed sleep per
historical window. Once no progress is made, the worker waits `archive_interval`.
Each failed stream also receives that retry cooldown while healthy streams advance.

`archive_timeout` bounds the complete attempt for one stream/window, including
source export, exact oracle comparison, upload/read-back, and publication. Its default
is two minutes. A timeout fails that stream instead of canceling the rest of the pass.
Both durations accept whole milliseconds from 10 ms through 24 hours. Increase a
timeout only while retaining sufficient Hot retention headroom.

The process publishes only after independent raw-data comparison and object
verification. Publication and coverage update are one SQLite transaction per
tenant/source/window. An empty verified window publishes zero pack references.
Nonempty windows rotate into bounded packs between complete native records, while
retaining one logical window. See [archive packing](archive-packs.md) for limits,
cross-pack verification, and the `max_pack_blocks` setting. Restart resumes from
committed coverage; uploaded but uncommitted objects can remain as orphans.

The time contract is unchanged: `H > L >= 0` and
`R > H + safety_margin + window`. H, L, window, coverage start, tenants, sources,
and blob location are fixed. Retention is checked before work and before commit.
vmtier does not enforce ingestion lateness or shorten Hot retention. Expired source
history, late writes into published windows, backfill, and deletion are not repaired
automatically. Missing required Cold coverage makes the relevant query fail.

## Query work admission

Both `serve` and the retained JSON v2 `gateway` reserve the entire effective
`max_query_bytes` allowance before starting a Cold backend search. The top-level
`query_memory_bytes` setting caps concurrent reservations. Defaults are 128 MiB
per query and 256 MiB shared, permitting two simultaneous searches. An explicit
shared allowance must cover one query and cannot exceed 8 GiB. Increasing
`max_query_bytes` beyond the shared allowance requires an explicit matching change
to `query_memory_bytes`. These are provisional work limits, not a qualified
production memory envelope.

Excess work is refused immediately with a `Cold query memory admission limit
exceeded` error. Admission adds no queue, and the upstream RPC concurrency gate
also has no deliberate queue wait (its existing eight-request ceiling remains).
The S3 queue and rate limits below are unchanged. Reservations cover full
validation and metadata conversion. Sample-query results retain their reservation
until the iterator closes or its deadline expires; cancellation keeps the
reservation until the reader's search returns. Shared cache-miss downloads can
outlive the last waiting query while canceled I/O unwinds; their finite flight
limit is separate from query admission. Client disconnect cannot
immediately cancel every upstream RPC, so the existing 30-second deadline remains
the work bound. All resource refusals fail the query rather than returning partial
results.

`query_memory_bytes` is an **accounted work limit, not a heap or RSS ceiling**:

| Allocation | Accounting and lifetime |
| --- | --- |
| Downloaded index/payload bytes and physical decoded timestamp/value pairs | Charged to the reader's per-query allowance, including cache hits and rows before clipping |
| Parsed index and validation scratch | A structural estimate for the current parsed index is charged to query work, alongside pack/index limits (16 MiB encoded index, 10,000 series, 100,000 blocks by default); decoder scratch and allocator overhead still require measured headroom |
| Materialized sample results | Physical rows, copied native names and block bookkeeping are charged before payload reads; the reservation remains through iterator close/deadline and the gateway also enforces response, block, and series limits |
| Metadata results | Reservation covers conversion; the pinned upstream API returns slices without a serialization-completion callback, so serialization occurs after release under the RPC concurrency limit |
| Immutable cache | Independent `cache_bytes` allowance, up to 256 MiB; shared between admitted queries |
| Shared cache-miss flights | Independently capped at 16 flights without a wait queue; include canceled downloads still unwinding and their transient buffers when measuring combined memory |
| Archive verifier and pack buffers | Independent bounded single-window pipeline; not charged to query admission |
| Runtime/HTTP/RPC/SQLite buffers and allocator retention | Additional process memory; observe RSS alongside logical counters |

The metrics `vmtier_query_memory_limit_bytes`, `vmtier_query_work_bytes`, and
`vmtier_query_memory_reserved_bytes` expose this accounting. Resource refusals use
`vmtier_query_limit_refusals_total{reason="memory_admission"}`. Counters and
reservations reset on restart. A smaller shared allowance reduces admitted query
work without changing stored data. See [memory accounting](query-memory.md) for
component formulas, lifetimes, Go heap and archive metrics, and exclusions from
the logical allowance. The small differential fixtures alone do not establish a
combined process-memory envelope.

## Shared S3 budget

SQLite S3 configurations default to:

```json
{
  "requests_per_second": 80,
  "max_concurrent_requests": 8,
  "max_queued_requests": 64
}
```

These fields belong inside `store`. If overriding them, supply all three positive
values. The limiter paces requests without bursts and uses a bounded FIFO queue.
Every HTTP attempt, including SDK retries, waits on the same budget. An in-flight
permit remains held until the response body closes. Queue overflow returns an error;
waiting respects the operation's deadline and shutdown cancellation.

The defaults leave headroom under the documented Sakura standard-plan limits, but
have not been qualified against the real service. An archive-plan candidate is
eight requests/s, one concurrent request, and a queue of 64; its performance is
unverified. Check current provider limits before deployment. A dedicated prefix
does not isolate bucket-wide quotas. Other clients or instances sharing the bucket
invalidate the claim that this process controls all requests.

Legacy JSON v2 commands keep their original behavior when all limit fields are zero.
They do not share another process's budget. A SQLite service must not run a separate
archive process or independent maintenance client against its bucket.

## Health, metrics, and shutdown

See [operations](operations.md#monitoring) for shared gateway metrics, catalog
capacity accounting, and failure handling.

`/health` reports process liveness. `/ready` returns 503 until every configured stream
has required coverage and catalog checks pass. It does not scrub all S3 payloads or
prove that external ingestion/retention settings match the contract. A stream
missing required coverage makes global readiness fail; another tenant's fully
covered queries can still succeed. A stopped service can fail Hot-only queries as well because stock
vmselect fans out to both storage nodes and partial responses are disabled.

`/metrics` retains the gateway, object-I/O, and tenant coverage metrics and adds:

| Metric | Meaning |
| --- | --- |
| `vmtier_archive_passes_total`, `vmtier_archive_failed_passes_total` | Scheduler attempts and passes containing errors |
| `vmtier_archive_committed_windows_total`, `vmtier_archive_samples_total`, `vmtier_archive_bytes_total` | Successful publication work in this process |
| `vmtier_archive_last_commit_timestamp_seconds` | Last commit observed by this process; zero before one occurs |
| `vmtier_stream_coverage_end_ms`, `vmtier_stream_archive_lag_seconds`, `vmtier_stream_retention_headroom_seconds` | Coverage and remaining time per tenant/source |
| `vmtier_catalog_generation` | Current persisted generation |
| `vmtier_ready` | Catalog/required coverage readiness, 1 or 0 |
| `vmtier_s3_attempts_total`, `vmtier_s3_waited_requests_total`, `vmtier_s3_wait_seconds_total` | Budget admissions and queue/pacing waits |
| `vmtier_s3_rejected_requests_total`, `vmtier_s3_canceled_requests_total` | Queue refusals and canceled waits |
| `vmtier_s3_in_flight_requests`, `vmtier_s3_queued_requests` | Current budget occupancy |
| `vmtier_build_info{version,commit,go_version,vm_version}` | Constant 1; identifies the running build, as `vmtier version` does |

Counters reset on process restart; persisted coverage does not. Keep these HTTP and
RPC endpoints on a trusted network with suitable access controls/TLS termination.
There is no online mutation or management API.

SIGINT/SIGTERM cancels archive and read work, drains listeners/handlers, and then
releases catalog ownership. The catalog remains locked while any owned work drains.
Published windows survive restart; an interrupted attempt does not advance coverage.

### Troubleshooting

| Symptom | Check |
| --- | --- |
| Startup rejects missing state or an unexpected ID | Confirm the working directory, catalog path, and ID. Initialize only a new deployment; restore missing existing state from backup. |
| State-directory lock error | Stop the current owner before maintenance. Use `/ready` and `/metrics` to observe a running service. |
| Contract or blob locator mismatch | Compare the configured fixed fields with offline `catalog-inspect`; do not initialize over existing state. |
| `/health` succeeds but `/ready` returns 503 | Read the response and archive error logs; inspect per-stream lag/headroom metrics. Initial catch-up and an unavailable source can both leave coverage incomplete. |
| Metadata or broad queries time out | Check query range/cardinality, S3 budget waits, and the frontend deadline. Larger deadlines do not increase throughput or remove the gateway's 30-second cap. |
| A window exceeds a pack or verifier limit | Use [preflight](preflight.md) on representative data before initialization. Rotation does not remove whole-window or single-record bounds; a bound catalog's window width cannot be edited in place. |
| Queries fail with `memory_admission` | Check `vmtier_query_memory_reserved_bytes` and refusal metrics. The shared budget permits only a bounded number of full per-query reservations; changing it requires workload/memory qualification. |

## Maintenance and JSON v2 migration

Stop `serve` before running `inspect -config serve.json`, `verify -config serve.json`,
or the catalog backup/restore commands. They acquire the same directory lock and
refuse a live owner. Offline verification uses the same S3 budget and validates
every referenced index, payload, and native block. Plan enough Hot headroom for the
downtime. `archive` and `gateway` with a SQLite configuration explicitly direct the
operator to `serve`; separate workers would defeat the ownership/request-budget model.

Follow the [catalog management guide](sqlite-catalog.md) to import a stopped JSON v2
catalog. The source config must retain the old catalog ID and matching source/blob
settings. Then prepare a service config pointing to the imported SQLite directory
and its new ID. Start `serve`, wait for readiness, and compare Cold-only/Hot/boundary
queries before switching the query frontend. Keep old catalog state and objects
until validation completes; do not run both archivers concurrently.

After restoring an older SQLite backup under a new ID, `serve` attempts the missing
suffix from Hot. This succeeds only while that source history is still queryable
outside the safety margin. Source expiry requires an external complete recovery
copy. Restoring metadata alone is not lossless disaster recovery. No real-provider
or natural-retention recovery claim follows from the local tests.
