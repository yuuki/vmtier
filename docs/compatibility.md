# VM compatibility and protocol boundaries

The SQLite and [PostgreSQL](postgres-catalog.md) `serve` backends and legacy JSON v2 MVP target **VictoriaMetrics OSS v1.155.0-cluster**, commit **`cab3d5fbeeedd5d0374c1dabde93939af4c2499c`**. A different release is not supported merely because some of its wire or native data happens to decode. vmtier is not an official VictoriaMetrics product. The [first-release design](design.md) distinguishes implemented stages from remaining work.

## Pinned primary sources

| Concern | Symbols and source |
| --- | --- |
| Native export framing and omitted tenant | [`prometheus.ExportNativeHandler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/app/vmselect/prometheus/prometheus.go): a 16-byte time range, followed by uint32 length + metric name and uint32 length + portable block. Uses `MarshalNoAccountIDProjectID`. |
| Independent raw oracle | [`prometheus.QueryHandler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/app/vmselect/prometheus/prometheus.go): the `promql.IsMetricSelectorWithRollup` special case calls `exportHandler` with `promapi` and `reduceMemUsage=false`, returning a raw matrix and a success footer. |
| Portable codec versus wire data | [`storage.Block.MarshalPortable`, `UnmarshalPortable`, `MarshalData`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/storage/block.go): portable data is neither an on-disk part nor MetricBlock wire bytes. `UnmarshalPortable` performs decoding. |
| Read interface | [`vmselectapi.API`, `BlockIterator`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/vmselectapi/api.go). |
| Framing, errors, deadlines, and queuing | [`vmselectapi.NewServer`, `processRPC`, `processSearch`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/vmselectapi/server.go): sends the search error string first; on success, sends MetricBlocks and an end marker. |
| Handshake and compression | [`handshake.VMSelectServer`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/handshake/handshake.go): `vmselect.01`, `ok`, and compression negotiation. vmtier reuses this implementation. |
| Replicas and partial responses | [`netstorage.startStorageNodesRequest`, `storageNodesRequest`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/app/vmselect/netstorage/netstorage.go) and [`httputil.GetDenyPartialResponse`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/httputil/bool.go). |
| Query cache | [`promql` and `search.disableCache`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/app/vmselect/promql/eval.go). |
| Retention filtering | [`storage.Search.Init`, `NextMetricBlock`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/storage/search.go): excludes blocks older than `retentionDeadline`. |
| No arbitrary time-range deletion | [`prometheus.DeleteHandler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/app/vmselect/prometheus/prometheus.go): rejects non-default start/end parameters. |

## RPC and HTTP support

Stock vmselect serves the metric query HTTP API. vmtier implements storage read
RPC and its own health/readiness/metrics HTTP endpoints; it does not expose a
Prometheus query endpoint.

| RPC | Gateway behavior | Use |
| --- | --- | --- |
| `search_v7` / `InitSearch` | Supported | Raw series for `/api/v1/query` and `/api/v1/query_range`; VM evaluates expressions. |
| `searchMetricNames_v3` | Supported | `/api/v1/series`, with tenant, time, and filter matching. |
| `labelNames_v5` | Supported | `/api/v1/labels`, including Cold label names. |
| `labelValues_v5` | Supported | `/api/v1/label/{name}/values`. |
| `tagValueSuffixes_v4` | Explicit error | Graphite suffix discovery is unsupported; `__graphite__` matchers are also rejected. |
| `seriesCount_v4`, `tsdbStatus_v6` | Explicit error | Global series counts and status statistics are unsupported. |
| `deleteSeries_v5`, `registerMetricNames_v3` | Explicit error | The gateway is read-only and does not return a false success. |
| `tenants_v1` | Explicit error | Automatic tenant discovery and the multitenant frontend are outside scope; tenants are configured explicitly. |
| `metricNamesUsageStats_v1`, `resetMetricNamesStats_v1` | Explicit error | Usage-statistics administration is unsupported. |
| `searchMetadata_v1` | Explicit error | Metric type/help/unit metadata is unsupported; label and series metadata remain supported. |
| Unknown RPC version | Upstream server error | No silent version fallback. |

A tenant is an accountID/projectID pair. Native exported metric names omit it, so it is restored from the archive context. Each request is restricted to one tenant; requests mixing tenants are rejected. Tenant identity is not encoded as an extra series label. The RPC listener is not an authentication boundary: protect it on a trusted internal network and authorize tenants at the HTTP entry point to stock vmselect.

## Correctness and errors

The gateway uses the storage query's actual time range. VM evaluates `rate`, `increase`, rollups, offsets, subqueries, and `@`. Before sending results, vmtier validates query limits, tenant, codec, and checksums of all selected blocks. Missing, corrupt, or timed-out Cold data is an error rather than an empty result.

The top-level vmselect requires `-replicationFactor=1 -globalReplicationFactor=1 -search.skipSlowReplicas=false -search.denyPartialResponse=true -search.disableCache=true -dedup.minScrapeInterval=1ms`. Cold is not a replica of Hot. Consistency with the frontend result cache enabled is outside the current contract; immutable S3 block caching is independent and remains available.

Cold metadata is calculated from actual Cold data ranges. Upstream Hot metadata can return broader ranges because its index uses daily granularity; parity is evaluated per API in E2E tests. Support does not extend to Graphite, cross-tenant discovery, or administrative statistics.

Metadata uses full validated block reads, bounded caching, and adjacent-range
coalescing. The current service and JSON v2 regression fixtures use stock
vmselect's five-second `-search.maxLabelsAPIDuration` default; see
[service query configuration](serve.md). Historical before/after measurements are
recorded in [metadata validation](metadata-validation.md), with broader workload
and recovery evidence in their separate reports. These fixture results do not
establish a production latency guarantee or qualify every dependency version.

The public RPC API passes a context with a deadline to the gateway. The gateway propagates it to Cold reads and applies its own maximum request duration. The upstream server constructs this context from a second-level RPC deadline and cancels it when the storage RPC connection closes. Immediate propagation of an HTTP client disconnect through stock vmselect has not been validated. Limits on concurrency, queueing, response bytes, and series/blocks produce explicit errors when exceeded.

Both service backends and the retained JSON v2 gateway reserve logical query work
before backend search, without a deliberate admission queue. Configured S3 request
budgets use a separate bounded queue. [Memory accounting](query-memory.md) explains the allowances and
why they are not a process RSS limit.

## Codec and upgrades

Pack v1 indexes record the VM release and revision; the reader rejects mismatches. These fields identify the expected codec, not an authenticated source binary. Portable streams do not carry a signed source version. Operators must pin all source vmselect and vmstorage binaries to the supported revision. `status/buildinfo` returns Prometheus-compatible fixed values and is not used for revision detection.

Preserving opaque payloads at rest does not imply decode-free RPC forwarding. The decoder checks frame, row, byte, and expansion limits before calling VM's public integer codec, clips to logical coverage, and reconstructs wire blocks. Infinities and stale markers use VM's scale/integer sentinel representations without additional floating-point precision reduction. [`decimal.FromFloat` and `AppendDecimalToFloat`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/lib/decimal/decimal.go) do not guarantee ordinary NaN payload preservation. Arbitrary NaN payload round trips are unsupported, and the archive verifier rejects non-stale NaNs. Label names and values must be valid UTF-8: pack indexes store labels as JSON, so the archiver refuses a window containing other bytes instead of publishing an index that cannot be read back.

For an upgrade:

1. Review upstream LICENSE, NOTICE, and module dependencies.
2. Compare export, header, and RPC symbols.
3. Run old and new portable fixtures and malformed-input tests.
4. Run Cold-only, Hot-only, boundary, metadata, tenant, and failure E2E tests with aligned frontend, Hot, and oracle versions.
5. Version codec changes explicitly and provide an existing-pack migration procedure.

Do not relabel old packs as a new codec. Single-node VM lacks the same `storageNode` interface and is outside this integration's scope.

### Upgrading from v1.128.0-cluster

This build pins v1.155.0-cluster for new archives and stock VM processes. Existing
catalogs and pack indexes pinned to v1.128.0-cluster remain rejected. An old
portable fixture decoding successfully does not authorize changing its provenance
or bypassing catalog compatibility checks. There is no in-place catalog or pack
migration in this update.

Keep the previous binaries, catalog backups, and immutable objects available.
While the complete history is still available from the source VM, align the new
source and query components to the pinned revision, initialize a new catalog ID,
and archive into a separate state directory and object prefix. Verify the complete
required coverage and compare queries before switching readers. If the source no
longer contains that history, retain the old deployment to serve its old archives;
do not switch it to this build or discard its objects. The JSON-to-SQLite import,
SQLite-to-PostgreSQL import, and PostgreSQL schema upgrade commands do not translate VM codec pins. A PostgreSQL schema-1-to-2
upgrade applies only to a catalog already compatible with the running binary;
it cannot make a v1.128.0 archive readable by this v1.155.0 build.

The v1.155.0 API uses context-aware methods and serialized wire-block iterators.
The gateway adapts to those interfaces without modifying upstream VM sources.
The new metric type/help/unit metadata RPC is explicitly unsupported.

Upstream deduplication also changed: when a normal sample and a stale marker share
a timestamp, v1.155.0 keeps the greatest non-stale value regardless of input order;
a collision containing only stale markers remains stale. vmtier uses upstream
`storage.DeduplicateSamples`, matching the aligned stock VM. This differs from the
old release's stale-marker precedence and is another reason to rearchive and
compare queries during an upgrade.

## S3 and catalog backends

| Area | Implemented requirement |
| --- | --- |
| PUT | Ordinary PutObject with Content-MD5; never reuse a published key. |
| Reads | GetObject and single-range GET with correct lengths/Content-Range; verify content with SHA-256. |
| Conditional PUT / ETag | Not required, requested, sent, or used for checksums/CAS. |
| LIST / multipart / DELETE | Not used in the normal path. Object discovery and GC are not provided. |
| Commit / CAS | SQLite or PostgreSQL transaction per tenant/source/window in `serve`; PostgreSQL also checks archiver fencing. Legacy JSON v2 uses generation CAS in its HTTP service or local adapter. |
| Catalog identity | Stable ID, fixed contract, append-only complete windows. Both service backends pin the blob locator. SQLite locks its state directory; PostgreSQL uses DB-time archiver leases. |
| S3 request budget | A process shares admission across archive/query traffic and retries. SQLite assumes a sole client; PostgreSQL divides explicit aggregate limits by the operator-enforced maximum client count. |
| Old S3 catalog | No automatic compatibility or migration; initialize the independent authority explicitly. |

The service supports SQLite schema 1 or PostgreSQL schema 2; the retained JSON
catalog format is v2. All use pack/native format v1 with the pinned VM identity.
Offline v2 import checks referenced indexes and copies metadata without rewriting
objects; see [SQLite maintenance](sqlite-catalog.md). The
[PostgreSQL importer](postgres-migration.md) copies a stopped SQLite catalog under
a new ID without rewriting objects. SQLite `inspect` and `verify` are offline;
PostgreSQL supports online inspection and verification with allocated S3 budget.
Standalone `archive` and `gateway` reject both service catalog types.

Both service writers rotate bounded packs within one logical window and publish
zero packs for a verified empty window; the retained JSON v2 writer emits one pack
per window. [Capacity preflight](preflight.md) sizes the shared service pipeline
without opening a catalog or object store.

The following HTTP API belongs only to the legacy JSON v2 catalog. `serve` has no
catalog HTTP listener. Catalog HTTP is an internal API for trusted networks, not a
public VM API.

| HTTP API | Behavior and errors |
| --- | --- |
| `GET /v1/catalog` | Authoritative snapshot. Missing/corrupt state or I/O failure: 503; token mismatch: 401. Clients validate ID, generation, and contract. |
| `POST /v1/catalog/commit` | Validates JSON `expected_generation` and `manifest` for generation CAS. Conflict: 409; invalid publication: 400; durability/I/O failure: 503. |
| Other paths or methods | 404/405. No fallback to a conditional S3 API. |

The service sets `Cache-Control: no-store`; clients request no caching and reject redirects. A shared token provides coarse read/write access, not public multitenant administration or RBAC. The local adapter is for development and same-host testing. It is not a persistent rollback-detection substitute for the remote client's observed-generation checks.

The target Sakura deployment assumes conditional PUT is unavailable; exhaustive conditional-header behavior has not been demonstrated from public documentation. The [API documentation](https://manual.sakura.ad.jp/cloud/objectstorage/api.html) describes MD5 validation. Following the [2026-04-09 announcement](https://cloud.sakura.ad.jp/news/2026/04/09/objectstorage-checksum/), an old SDK is not recommended as a workaround for superseded restrictions. vmtier independently uses SHA-256 for content verification.

The catalog HTTP adapter remembers observed generations within its process and
rejects generation rollback or changed content at the same generation. Rollback
detection across complete restarts and consistency among independently restored
authorities are not guaranteed. SQLite and JSON v2 do not provide automatic HA.
PostgreSQL HA relies on the [configured replication, promotion, and fencing
policy](ha-deployment.md); neither database replicas nor independent catalogs
should be treated as interchangeable stale authorities. SQLite restore requires
a new catalog ID. See [legacy operations](legacy-json-v2.md) for HTTP bearer
authentication and TLS proxy deployment.

## Scope of executed validation

The native pack-to-gateway-to-stock-vmselect spike and exact comparisons after rebuilding Hot with recent-only data succeeded in the original architecture. Historical all-S3 catalog results remain separate. The [MVP validation report](validation.md) records the legacy independent catalog and restricted S3 proxy results. The [SQLite foundation report](sqlite-catalog-validation.md) covers the original repository and offline tools; [service validation](serve-validation.md) records runtime checks separately. Earlier results do not establish that later implementations passed the same scenarios.

Differential tests normalize label/series ordering, compare timestamps and values without tolerance, disable result caches, and use 1ms deduplication on both sides. They provide evidence for the fixture, not every MetricsQL function, cardinality, or VM release. Local S3 test doubles and the restriction proxy do not establish real Sakura/AWS IAM, SigV4 authentication, consistency SLAs, durability, or performance.

GitHub Actions workflows exercise build, repository, and differential checks; see
[testing](testing.md) for commands and evidence locations.
Evaluate hosted results against their exact commit; local runs and historical
reports do not substitute for successful CI on a new dependency set. The
[PostgreSQL HA report](postgres-ha-validation.md) retains earlier gateway,
migration, and database-fault evidence with its original VM pin.
