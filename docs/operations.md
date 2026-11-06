# Deployment, operation, and recovery

This document describes the **current MVP**, which is intended for reproducible evaluation. The [first-release design](design.md) describes planned changes, not commands available today. See the [README](../README.md) for setup, the [MVP design](mvp-design.md) for implemented behavior, and [compatibility](compatibility.md) for protocol coverage.

## Initial deployment

1. Pin stock VM to the supported version and inventory every tenant, export source, shard, and replica. Use a Hot-only export endpoint, never the combined query frontend.
2. Confirm Hot retention and verify that the oldest unarchived window can still be queried and exported. Do not shorten retention during deployment or commit a lost interval as empty.
3. Enforce the lateness bound L on every ingestion, import, and backfill path. Restrict direct access to vminsert and VM. Include clock error, queues, and retries in the bound; without this contract, age alone cannot establish that a window is immutable.
4. Require `H > L` and `R > H + safety_margin + window`, where H is archive age and R is Hot retention. Recovery must complete within the available headroom. Run one writer. `lateness_enforced` acknowledges this external contract; it does not implement enforcement. Use Go duration syntax, such as `720h`, not `30d`.
5. Give the archive a dedicated S3 prefix and least-privilege access. The adapter uses ordinary PUT with Content-MD5, GET, and Range GET; it does not require conditional PUT, ETags, or LIST. Read-back SHA-256 verification precedes publication. Use a storage class that permits immediate reads.
6. Explicitly initialize the catalog on persistent storage, start its service, and connect all clients to the same catalog ID. Run `vmtier archive -config FILE`, verify packs and indexes, and inspect tenants, sources, and coverage. The catalog must not advance until every source is complete. Schedule subsequent runs with a single-writer external scheduler.
7. Register Hot and the Cold gateway with the query vmselect. Set `replicationFactor=1`, `globalReplicationFactor=1`, `skipSlowReplicas=false`, `denyPartialResponse=true`, `disableCache=true`, and `dedup.minScrapeInterval=1ms` using the flags shown in the README.
8. In an isolated environment, compare Cold-only and boundary queries with a complete reference VM and confirm that a missing Cold object produces an error before deciding to use the deployment.

Set `coverage_start_ms` to the start of the guaranteed history, in epoch milliseconds, aligned to a window boundary. Do not move it forward merely to pass readiness. Earlier history is outside Cold's completeness guarantee.

Without explicit bounds, `archive -config FILE` selects the minimum contiguous coverage across all configured tenants and sources, initially `coverage_start_ms`, through `floor((now-H)/window)*window`. It is a no-op when all currently required windows are committed. Because the catalog is the checkpoint, the same command can resume after process restart. Explicit `-start <ms> -end <ms>` bounds remain subject to alignment, age, and retention checks.

For example, a dedicated host can use cron with a fixed working directory and writer lock. Choose the interval to fit the window size and safety margin.

```cron
*/5 * * * * cd /srv/vmtier && flock -n /srv/vmtier/archive.lock .bin/vmtier archive -config /etc/vmtier.json >> /var/log/vmtier-archive.log 2>&1
```

vmtier does not install this schedule. Monitor exit codes and structured logs, distinguishing a skipped lock acquisition from an archive failure.

Supply S3 credentials through `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`; do not put them in configuration files, logs, or Git. General IAM role and credential-provider-chain support is unverified. Keep TLS verification enabled. The local backend is for development and does not provide S3 durability guarantees.

## Catalog initialization and placement

```sh
.bin/vmtier catalog-init -path /srv/vmtier/catalog -id cluster-a-catalog-001
.bin/vmtier catalog -path /srv/vmtier/catalog -id cluster-a-catalog-001 -http 127.0.0.1:8491
```

`catalog-init` only creates new state and refuses an existing catalog. Normal startup rejects missing state and ID mismatches; it does not recover by creating an empty catalog. The ID is not a secret, but must be stable and unique to distinguish authorities and recovery instances.

The configuration uses `type`, `url`, and `id` for HTTP access, or `type`, `path`, and `id` for local development. The first commit fixes the contract: coverage origin, window size, deduplication at 1 ms, archive age H, allowed lateness L, tenant set, and source IDs/URL hashes. Subsequent changes are rejected, including moving the coverage origin forward to hide a gap.

Changing H or L can reopen an archived window for writes. For example, changing `H=3h,L=30m` to `H=6h,L=5h` permits writes into previously sealed history. Archive, query, and readiness reject this configuration mismatch. Such a change needs a separate migration that preserves old windows; the MVP does not provide it. Contract validation does not prove that ingestion actually enforces L.

The catalog API is intended for a trusted network. Set the same optional `VMTIER_CATALOG_TOKEN` on server and clients for bearer authentication. Use a TLS proxy and network access controls outside loopback. Do not embed the token in configuration, URLs, or logs. Commit authorization does not replace trust in the writer's export-completeness checks.

Use persistent local storage that correctly supports file and directory fsync, atomic rename, and flock. Do not store authority state in an ephemeral container layer. NFS and other shared filesystems are unverified. There is no HA or automatic failover, and an empty replacement authority on another host is not a recovery mechanism. Queries fail while the catalog is unavailable.

For deployment, initialize immediately below a pre-existing, durable parent directory. Initialization syncs the catalog directory and its immediate parent, not every newly created ancestor in a nested path. Initialization and commit behavior under actual power loss have not been tested.

## Sakura Object Storage

The deployment requirement assumes conditional PUT is unavailable on Sakura Object Storage; this is not a claim that the public API documentation specifies every conditional-header behavior. vmtier sends no conditional PUT and does not use ETags for correctness.

The adapter sends Content-MD5 in line with the checksum scope in the [official API documentation](https://manual.sakura.ad.jp/cloud/objectstorage/api.html). It independently verifies stored content with read-back GET and SHA-256. The [April 9, 2026 announcement](https://cloud.sakura.ad.jp/news/2026/04/09/objectstorage-checksum/) reports removal of earlier SDK checksum compatibility restrictions; an older AWS SDK is not a deployment requirement.

The documented bucket limits are 100 requests/second and 100 concurrent requests for the [standard plan](https://manual.sakura.ad.jp/cloud/objectstorage/about.html), and 10 requests/second with one concurrent request for the [archive plan](https://manual.sakura.ad.jp/cloud/objectstorage/archiveplan.html). The MVP's local concurrency controls do not enforce a bucket-wide request budget across gateways, metadata, archival, and retries. Operation within the archive-plan limits has not been validated. A deployment that depends on those limits needs aggregate rate and concurrency control followed by testing.

No actual Sakura bucket or credentials have been used to validate connectivity, SigV4, visibility after PUT, Range GET, quotas, recovery, or performance. The local Moto backend and proxy that rejects conditional requests and removes ETags approximate these API constraints; they are not service certification. See the [configuration example](../examples/sakura.json).

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

An external scheduler collects the one-shot archiver's structured JSON logs, exit code, and standard output. There is no resident archive metrics exporter. The gateway's metrics endpoint also consults the catalog. Monitor catalog health, persistent-disk capacity and I/O errors, and backup status separately.

Retention headroom is the finite time until the oldest unarchived sample can no longer be queried or exported. An alert does not extend it. VM can exclude samples through its retention filter even while their physical parts remain on disk, so physical file existence is not extra recovery time.

## Failure and restart behavior

| Failure | Response |
| --- | --- |
| Export timeout, missing source, or interrupted native stream | Do not advance the catalog. Restore the source and retry while Hot can still export the data. |
| Native export truncated at a record boundary, or a missing conflicting-timestamp block | EOF and counts alone do not establish completeness. The independent raw JSON matrix comparison checks every series, timestamp, and value and rejects mismatches. |
| Upload or read-back verification failure | Uncommitted objects are invisible to queries. Retry the archive. |
| Catalog generation conflict | Stop the duplicate writer, inspect identity, generation, and coverage, then retry. Do not replace CAS with an unconditional catalog overwrite. |
| Process stops just after commit | Reload the authoritative catalog. Do not infer coverage from the presence or absence of a success log. |
| Missing or corrupt object, or GET failure | Fail the query. Restore the original bytes, verify them, and retest; do not replace the failure with an empty result. |
| Hot retention deadline exceeded | Do not commit an empty window as success. Missing data needs another complete recovery copy. |

Published object keys are never reused. A new attempt uses a fresh nonce and keys, so a lost PUT response or generation conflict can leave orphan objects. Queries read only catalog references. After a lost commit response, reload the authority before deciding what remains to publish. The MVP supports bounded, single-PUT objects; multipart abort and cleanup are out of scope.

## Catalog backup, restore, and older formats

The catalog is the authority for committed coverage, not a disposable index reconstructible from an S3 listing. Stop the writer and quiesce commits before copying its state. Record the catalog ID, generation, contract, and referenced objects. Do not race a raw file copy with a commit. The MVP has no online backup or automatic restore command.

A live HTTP client rejects decreasing generations and changed content within one generation. After all clients restart, however, they have no previous observation. Do not resume from an older backup under the same ID. Isolate the old writer, use a new catalog ID, audit missing commits and references against independent records or a complete source, and explicitly switch every client. If Hot no longer has the data and the required metadata cannot be recovered, vmtier cannot establish completeness on its own.

The current implementation does not automatically import the former S3 `catalog.json`; an old object at that path is never used as the new authority. A manual migration requires stopping the old writer, preserving its catalog and all objects, auditing the contract, coverage, sources, and codec, and running differential checks with a new ID before switching. There is no conversion command, and hand-editing JSON is not a validated migration. Re-archiving into a new prefix from a source that still holds the full history is the reproducible MVP path.

## Late writes, backfill, and deletion

The `H > L` contract assumes accepted writes arrive before a window is archived. If an older sample is written to Hot afterward, vmtier does not guarantee that it will survive in Cold once Hot retention removes it. A read gateway cannot detect or reject violations on a direct write path.

There are no deltas, overlap re-exports, backfill updates, or tombstones in the MVP. Treat committed windows as immutable. If exceptional backfill is necessary, rebuild the complete archive under a **new prefix** from an isolated source holding the corrected full history, compare it with the reference, and switch to a new gateway with VM result caching disabled. This is a manual migration approach, not a tested automatic backfill command.

Although the gateway rejects delete, write, and administration RPCs, a delete sent through the top-level vmselect can modify Hot before Cold returns an error. Block the read frontend's HTTP mutation APIs at the access-control layer too. Deleting a series only from Hot can make it reappear from Cold. Environments requiring deletion need tombstones or a safe archive rebuild; the MVP does not provide deletion management.

## Retention changes and rollback

vmtier does not shorten Hot retention automatically. Before changing retention, check contiguous coverage for all tenants and sources, query the affected range through Cold alone, exercise failure responses, verify the lateness contract and recovery copies, and assess worst-case archive lag. These checks do not create protection against an unbounded outage.

If Hot still holds the entire history, removing the Cold node restores a Hot-only frontend. If older history is absent from Hot, removing the gateway is not a complete rollback: restore and validate the data in an isolated VM with sufficient retention before switching. Do not restore into an active vmstorage data directory.

## GC, Cold retention, and upgrades

The MVP performs no GC or Cold TTL deletion. Its append-only catalog retains existing entries, and it deletes neither referenced objects nor orphans. This avoids deletion races but allows storage usage to grow. An `inspect` result alone is not permission to delete an object, and an S3 lifecycle rule must not delete objects referenced by the current catalog. Durable catalog backups and an active query's in-memory snapshot serve different purposes.

Any future deletion implementation must account for query lifetime and all readers that may still hold old references. That implementation is deferred; do not approximate it with an arbitrary lifecycle expiration.

Follow the [compatibility upgrade procedure](compatibility.md) for codec, RPC, and reference-version checks. Packs carry revision checks and are not automatically migrated by replacing the binary.
