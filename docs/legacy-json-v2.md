# Legacy JSON v2 deployment

The separate-process JSON v2 runtime is retained for regression testing and
migration. New deployments should start with the [SQLite service](serve.md) or
[PostgreSQL service](postgres-catalog.md).
For offline migration, follow [catalog import](sqlite-catalog.md#import-json-v2).

## Commands


This section describes the retained separate-process MVP. Use the [service guide](serve.md) for SQLite deployment; its in-process catalog has no catalog HTTP listener.

```sh
.bin/vmtier version
.bin/vmtier catalog-init -path .state/catalog -id dev-catalog-001
.bin/vmtier catalog -path .state/catalog -id dev-catalog-001 -http 127.0.0.1:8491
# Keep the catalog running and use another shell for these commands.
.bin/vmtier archive -config config.json  # Select safe, incomplete windows automatically.
.bin/vmtier archive -config config.json -start 1791244800000 -end 1791266400000
.bin/vmtier gateway -config config.json -listen 127.0.0.1:8489 -http 127.0.0.1:8490
.bin/vmtier inspect -config config.json
.bin/vmtier verify -config config.json
```

`archive` is a one-shot command intended for an external scheduler. Without `-start` and `-end`, it selects complete windows from the catalog's contiguous coverage up to the current time minus the archive age, H. Explicit bounds use UTC epoch **milliseconds** and the half-open interval `[start,end)`. The catalog is the restart checkpoint; committed windows are not exported again. Windows that are too recent or lack retention headroom are rejected. The timestamps above illustrate the syntax and are not deployment-specific values.

Set `coverage_start_ms` in the [example configuration](../examples/config.json) to the earliest time whose completeness must be guaranteed, aligned to a window boundary. Set `lateness_enforced` to `true` **only when every write path actually enforces the lateness bound**. The examples intentionally refuse to start until these deployment decisions have been made. Supply S3 credentials through `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`, not through the configuration file. Create the bucket beforehand. A [Sakura configuration example](../examples/sakura.json) is also provided.

Configure the catalog explicitly. An HTTP service uses `{"type":"http","url":"http://127.0.0.1:8491","id":"dev-catalog-001"}`. Same-host development can use `{"type":"local","path":".state/catalog","id":"dev-catalog-001"}`. `catalog-init` creates new state and refuses to overwrite an existing catalog. Normal `catalog` startup fails if state is missing; it never silently creates an empty catalog. Client and server IDs must match.

Deploy the catalog on a trusted network. An optional shared bearer token is supplied through `VMTIER_CATALOG_TOKEN`. Deployments outside loopback need TLS termination and network access controls. The catalog is a single authority with persistent storage, not an HA service: losing its disk cannot be repaired from the S3 object listing alone.

Each export source must be a Hot-only stock vmselect connected to all of that source's Hot shards. Do not archive through the combined Hot/Cold query frontend. Source IDs, URLs, and the tenant set are fixed after the first commit. Shard topology inside a source must also remain stable; the gateway cannot detect its changes.

Required query-frontend flags, with deduplication configured consistently across VM components:

```sh
.bin/vmselect -httpListenAddr=127.0.0.1:8482 \
  -storageNode=127.0.0.1:8401,127.0.0.1:8489 \
  -replicationFactor=1 -globalReplicationFactor=1 \
  -search.skipSlowReplicas=false -search.denyPartialResponse=true \
  -search.disableCache -dedup.minScrapeInterval=1ms
```

Keep the RPC listener on a trusted network. `/health` reports process liveness; `/ready` validates the catalog and required coverage. `/metrics` exposes archive lag, retention headroom, object reads, cache hits, and query errors. Use `verify` for periodic payload verification. Cold metadata is also available through the gateway.


## Initial deployment

1. Pin stock VM to the supported version and inventory every tenant, export source, shard, and replica. Use a Hot-only export endpoint, never the combined query frontend.
2. Confirm Hot retention and verify that the oldest unarchived window can still be queried and exported. Do not shorten retention during deployment or commit a lost interval as empty.
3. Enforce the lateness bound L on every ingestion, import, and backfill path. Restrict direct access to vminsert and VM. Include clock error, queues, and retries in the bound; without this contract, age alone cannot establish that a window is immutable.
4. Require `H > L` and `R > H + safety_margin + window`, where H is archive age and R is Hot retention. Recovery must complete within the available headroom. Run one writer. `lateness_enforced` acknowledges this external contract; it does not implement enforcement. Use Go duration syntax, such as `720h`, not `30d`.
5. Give the archive a dedicated S3 prefix and least-privilege access. The adapter uses ordinary PUT with Content-MD5, GET, and Range GET; it does not require conditional PUT, ETags, or LIST. Read-back SHA-256 verification precedes publication. Use a storage class that permits immediate reads.
6. Explicitly initialize the catalog on persistent storage, start its service, and connect all clients to the same catalog ID. Run `vmtier archive -config FILE`, verify packs and indexes, and inspect tenants, sources, and coverage. The catalog must not advance until every source is complete. Schedule subsequent runs with a single-writer external scheduler.
7. Register Hot and the Cold gateway with the query vmselect. Set `replicationFactor=1`, `globalReplicationFactor=1`, `skipSlowReplicas=false`, `denyPartialResponse=true`, `disableCache=true`, and `dedup.minScrapeInterval=1ms` using the flags in [Commands](#commands).
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

## Catalog backup, restore, and older formats

For SQLite, use the implemented offline `catalog-backup`, `catalog-restore`, and
JSON v2 `catalog-import` commands described in [catalog maintenance](sqlite-catalog.md).
Stop `serve` first; its lifetime state lock rejects concurrent maintenance. After
restore to a new path and ID, `serve` can catch up from Hot while retention permits.
The manual authority handling below applies to legacy JSON v2 state.

The catalog is the authority for committed coverage, not a disposable index reconstructible from an S3 listing. Stop the writer and quiesce commits before copying its state. Record the catalog ID, generation, contract, and referenced objects. Do not race a raw file copy with a commit. The MVP has no online backup or automatic restore command.

A live HTTP client rejects decreasing generations and changed content within one generation. After all clients restart, however, they have no previous observation. Do not resume from an older backup under the same ID. Isolate the old writer, use a new catalog ID, audit missing commits and references against independent records or a complete source, and explicitly switch every client. If Hot no longer has the data and the required metadata cannot be recovered, vmtier cannot establish completeness on its own.

The current implementation does not automatically import the former S3 `catalog.json`; an old object at that path is never used as the new authority. A manual migration requires stopping the old writer, preserving its catalog and all objects, auditing the contract, coverage, sources, and codec, and running differential checks with a new ID before switching. There is no conversion command, and hand-editing JSON is not a validated migration. Re-archiving into a new prefix from a source that still holds the full history is the reproducible MVP path.


For shared retention, failure handling, monitoring, and immutable-data constraints,
see [operations](operations.md) and [limits](limits.md).
