# VM compatibility and protocol boundaries

[Documentation index](README.md) · [Service guide](serve.md) ·
[Catalog maintenance](sqlite-catalog.md)

The SQLite [`serve` runtime](serve.md) and legacy JSON v2 MVP both target **VictoriaMetrics OSS v1.128.0-cluster**, commit **`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`**. A different release is not supported merely because some of its wire or native data happens to decode. vmtier is not an official VictoriaMetrics product. The [first-release design](design.md) distinguishes implemented stages from remaining work.

## Compatibility at a glance

| Component | Current boundary |
| --- | --- |
| VictoriaMetrics | Pinned OSS cluster revision only; single-node VM integration is unsupported. |
| Query evaluation | Stock vmselect evaluates MetricsQL; vmtier returns validated raw blocks over read RPC. |
| Current runtime | `serve` with SQLite schema 1, one local state owner, pack/index format v1. |
| Legacy runtime | Separate `archive`, `gateway`, and local/HTTP JSON v2 catalog; retained for regression and offline migration. |
| Object storage | Ordinary PUT, GET, and single-range GET; no conditional PUT or ETag dependency. Real-provider qualification is pending. |
| Mutations | No late updates to published windows, backfill, deletion, compaction, GC, or Cold TTL. |
| Deduplication | 1 ms, configured consistently across VM components. Frontend result caching is disabled. |

## Pinned primary sources

| Concern | Symbols and source |
| --- | --- |
| Native export framing and omitted tenant | [`prometheus.ExportNativeHandler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/app/vmselect/prometheus/prometheus.go): a 16-byte time range, followed by uint32 length + metric name and uint32 length + portable block. Uses `MarshalNoAccountIDProjectID`. |
| Independent raw oracle | [`prometheus.QueryHandler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/app/vmselect/prometheus/prometheus.go): the `promql.IsMetricSelectorWithRollup` special case calls `exportHandler` with `promapi` and `reduceMemUsage=false`, returning a raw matrix and a success footer. |
| Portable codec versus wire data | [`storage.Block.MarshalPortable`, `UnmarshalPortable`, `MarshalData`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/storage/block.go): portable data is neither an on-disk part nor MetricBlock wire bytes. `UnmarshalPortable` performs decoding. |
| Read interface | [`vmselectapi.API`, `BlockIterator`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/vmselectapi/api.go). |
| Framing, errors, deadlines, and queuing | [`vmselectapi.NewServer`, `processRPC`, `processSearch`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/vmselectapi/server.go): sends the search error string first; on success, sends MetricBlocks and an end marker. |
| Handshake and compression | [`handshake.VMSelectServer`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/handshake/handshake.go): `vmselect.01`, `ok`, and compression negotiation. vmtier reuses this implementation. |
| Replicas and partial responses | [`netstorage.startStorageNodesRequest`, `storageNodesRequest`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/app/vmselect/netstorage/netstorage.go) and [`httputil.GetDenyPartialResponse`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/httputil/bool.go). |
| Query cache | [`promql` and `search.disableCache`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/app/vmselect/promql/eval.go). |
| Retention filtering | [`storage.Search.Init`, `NextMetricBlock`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/storage/search.go): excludes blocks older than `retentionDeadline`. |
| No arbitrary time-range deletion | [`prometheus.DeleteHandler`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/app/vmselect/prometheus/prometheus.go): rejects non-default start/end parameters. |

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
| Unknown RPC version | Upstream server error | No silent version fallback. |

A tenant is an accountID/projectID pair. Native exported metric names omit it, so it is restored from the archive context. Each request is restricted to one tenant; requests mixing tenants are rejected. Tenant identity is not encoded as an extra series label. The RPC listener is not an authentication boundary: protect it on a trusted internal network and authorize tenants at the HTTP entry point to stock vmselect.

## Correctness and errors

The gateway uses the storage query's actual time range. VM evaluates `rate`, `increase`, rollups, offsets, subqueries, and `@`. Before sending results, vmtier validates query limits, tenant, codec, and checksums of all selected blocks. Missing, corrupt, or timed-out Cold data is an error rather than an empty result.

The top-level vmselect requires `-replicationFactor=1 -globalReplicationFactor=1 -search.skipSlowReplicas=false -search.denyPartialResponse=true -search.disableCache=true -dedup.minScrapeInterval=1ms`. Cold is not a replica of Hot. Consistency with the frontend result cache enabled is outside the current contract; immutable S3 block caching is independent and remains available. See the [frontend command](serve.md#connect-stock-vmselect).

Cold metadata is calculated from actual Cold data ranges. Upstream Hot metadata can return broader ranges because its index uses daily granularity; parity is evaluated per API in E2E tests. Support does not extend to Graphite, cross-tenant discovery, or administrative statistics.

Metadata uses full validated block reads with LRU caching, shared cache misses,
and adjacent-range coalescing. The [metadata measurements](metadata-validation.md)
passed stock vmselect's five-second `-search.maxLabelsAPIDuration` default on the
recorded JSON and SQLite S3 fixtures after the reader changes. The harness retains
a configurable 30-second default, and larger ranges/cardinality can still time
out. These results and the [combined workload measurements](workload-validation.md)
are scoped observations, not a general metadata latency guarantee. See
[service query configuration](serve.md#connect-stock-vmselect).

The public RPC deadline has second-level precision and is converted into an S3
context deadline. Immediate cancellation on client disconnect is not guaranteed
because the public API does not expose the RPC request context. Both runtimes
reserve logical query work before backend search, with 128 MiB per-query and
256 MiB shared defaults. The eight-request RPC ceiling and memory admission have
no deliberate queue wait. S3 has its separate bounded queue. Limits on read/decoded
work, catalog plans, response bytes, series, blocks, and shared reads produce
explicit errors. [Memory accounting](query-memory.md) explains reservation lifetimes
and why these budgets are not a process RSS limit.

## Codec and upgrades

Pack v1 indexes record the VM release and revision; the reader rejects mismatches. These fields identify the expected codec, not an authenticated source binary. Portable streams do not carry a signed source version. Operators must pin all source vmselect and vmstorage binaries to the supported revision. `status/buildinfo` returns Prometheus-compatible fixed values and is not used for revision detection.

Preserving opaque payloads at rest does not imply decode-free RPC forwarding. The decoder checks frame, row, byte, and expansion limits before calling VM's public integer codec, clips to logical coverage, and reconstructs wire blocks. Infinities and stale markers use VM's scale/integer sentinel representations without additional floating-point precision reduction. [`decimal.FromFloat` and `AppendDecimalToFloat`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/decimal/decimal.go) do not guarantee ordinary NaN payload preservation. Arbitrary NaN payload round trips are unsupported, and the archive verifier rejects non-stale NaNs.

For an upgrade:

1. Review upstream LICENSE, NOTICE, and module dependencies.
2. Compare export, header, and RPC symbols.
3. Run old and new portable fixtures and malformed-input tests.
4. Run Cold-only, Hot-only, boundary, metadata, tenant, and failure E2E tests with aligned frontend, Hot, and oracle versions.
5. Version codec changes explicitly and provide an existing-pack migration procedure.

Do not relabel old packs as a new codec. Single-node VM lacks the same `storageNode` interface and is outside this integration's scope.

## S3 and catalog backends

| Area | Implemented requirement |
| --- | --- |
| PUT | Ordinary PutObject with Content-MD5; never reuse a published key. |
| Reads | GetObject and single-range GET with correct lengths/Content-Range; verify content with SHA-256. |
| Conditional PUT / ETag | Not required, requested, sent, or used for checksums/CAS. |
| LIST / multipart / DELETE | Not used in the normal path. Object discovery and GC are not provided. |
| Commit / CAS | SQLite transaction per tenant/source/window in `serve`; generation CAS in the legacy JSON v2 HTTP service or local adapter. |
| Catalog identity | Stable ID, fixed contract, append-only complete windows. SQLite additionally pins the blob locator and locks its state directory for the owner's lifetime. |
| S3 request budget | SQLite service uses a shared rate, in-flight, and queue budget across archive/query traffic and retries; it covers the bucket only when this process is its sole client. |
| Old S3 catalog | No automatic compatibility or migration; initialize the independent authority explicitly. |

SQLite schema 1 is used by `serve`; the retained JSON catalog format is v2. Both use
pack/native format v1. Offline v2 import checks referenced indexes and copies
metadata without rewriting objects; see [migration and recovery](sqlite-catalog.md).
SQLite `inspect` and `verify` are offline and cannot open a live service's state.
The standalone `archive` and `gateway` commands reject SQLite configurations.
The SQLite writer can rotate several packs within one logical window and publishes
zero packs for a verified empty window; the retained JSON v2 writer still emits
one pack per window. [Capacity preflight](preflight.md) sizes the SQLite pipeline
without opening the catalog or object store.

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
detection across complete restarts, consistency among multiple authorities, and HA
are not guaranteed by either deployment. SQLite restore requires a new catalog ID.
See [legacy operations](operations.md) for optional HTTP bearer authentication and
TLS proxy deployment, and [service operation](serve.md) for shared S3 request limits.

## Scope of executed validation

The native pack-to-gateway-to-stock-vmselect spike and exact comparisons after rebuilding Hot with recent-only data succeeded in the original architecture. Historical all-S3 catalog results remain separate. The [MVP validation report](validation.md) records the legacy independent catalog and restricted S3 proxy results. The [SQLite foundation report](sqlite-catalog-validation.md) covers the original repository and offline tools; [service validation](serve-validation.md) records runtime checks separately. Earlier results do not establish that later implementations passed the same scenarios.

Later [implementation validation](implementation-validation.md) records 1,440
exact query comparisons across both runtimes and local/Moto backends. Separate
reports cover [archive bounds](archive-validation.md),
[reader behavior](reader-validation.md), [metadata latency](metadata-validation.md),
[catalog scale](catalog-capacity-validation.md), [combined workload and memory](workload-validation.md),
and [offline recovery](recovery-validation.md). Each report retains its own
revision, fixture, and limitations; none establishes real-provider support.

Differential tests normalize label/series ordering, compare timestamps and values without tolerance, disable result caches, and use 1ms deduplication on both sides. They provide evidence for the fixture, not every MetricsQL function, cardinality, or VM release. Local S3 test doubles and the restriction proxy do not establish real Sakura/AWS IAM, SigV4 authentication, consistency SLAs, durability, or performance.

The [GitHub Actions workflow](../.github/workflows/ci.yml) defines automated checks.
See [Testing](testing.md) for the commands and evidence layout. A recorded local
run is distinct from a hosted CI result; consult the relevant run and its artifacts
before making a CI claim.
