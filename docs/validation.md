# Validation results (2026-11-06 UTC, current v2 MVP)

The implementation with an independent, persistent HTTP catalog authority and a
blob store using ordinary PUT/GET/Range requests was tested on executor
**`05fa190d414f`**. The local backend with one replica and Moto S3 with two replicas
each passed **360 exact comparisons, for a total of 720**.
Raw results are in [local.json](results/local.json) and
[s3-replicated.json](results/s3-replicated.json); see [testing.md](testing.md) for
reproduction instructions. Evidence for the previous all-S3 catalog architecture
is kept under [legacy-v1](results/legacy-v1/README.md). Its original JSON is
preserved byte-for-byte; its accompanying prose has been translated into English.
Those historical results are not included in the counts or measurements below.

The environment was Debian 13 / Linux amd64, Go 1.27.3, Python 3.12.14, five
available CPUs, and approximately 33.3 GiB RAM. S3 tests used Moto 5.1.15 and a
loopback compatibility proxy. VM binaries were unmodified OSS
**v1.128.0-cluster**, commit `9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`.
The suites ran sequentially: the local run started at
`2026-11-06T08:03:29.718965+00:00`, and the S3 run finished at
`2026-11-06T08:06:34.081484+00:00`.

## Passed checks

- `make build test race vet` passed for the final MVP implementation on this
  executor. This included all Go packages and the fixed archive-age/lateness
  contract tests.
- Native export → portable native pack → independent gateway → stock vmselect
  worked for `query` and `query_range`. Cold object Range GET counters increased.
- Each backend ran 30 expressions × 2 tenants × 3 periods (Cold, Hot, and the
  boundary) × 2 APIs (instant and range), yielding 360 comparisons. Timestamps,
  labels, and VM's numeric strings matched exactly after order normalization;
  there was no floating-point tolerance. Metadata comparisons were additional.
- Fixtures covered two tenants, 16 series per tenant, 17,654 input samples, five
  hours, and a 30-second scrape interval. Cases included counter resets, gaps,
  irregular scrapes, label churn, stale markers, NaN inputs, ±Inf, duplicate and
  conflicting timestamps, boundary ±1 ms, empty/negative/regexp matchers,
  aggregation, vector matching, offsets, subqueries, and `@`.
- Hot was recreated in a new data directory containing recent samples only. Raw
  export for the old period was empty for both tenants. Cold raw export returned
  no samples at or after the exact boundary millisecond. This did not wait for
  actual Hot retention expiry.
- Cold label names/values, series discovery, and tenant-specific values matched
  the reference VM. Cold deletion returned an explicit unsupported error.
- Killing the archiver during native export left coverage unpublished; retrying
  against the same source succeeded. Repeating the same windows and an automatic
  archive no-op left catalog manifest bytes unchanged. Unenforced lateness and
  unreachable-source configurations were rejected.
- Changes to the committed coverage origin, archive age, and allowed lateness
  were rejected without changing authority state. The blob backend contained no
  legacy `catalog.json`.
- After warming the gateway's 32 MiB cache, stopping the authority made queries
  and `/ready` fail. Restarting it from the same persistent state restored the
  exact manifest and query results. Starting with the state directory absent
  failed without silently creating an empty state.
- Missing, truncated, and checksum-invalid objects made both queries and `verify`
  fail; restoring the original bytes restored queries. Stopping the backend also
  caused the integrated vmselect to fail instead of returning empty success.

The S3 compatibility proxy rejects `If-*` and `x-amz-copy-source-if-*` headers and
removes response ETags. The suite recorded **zero rejected conditional requests,
2,480 removed ETag headers, valid Content-MD5 on all 17 PUTs, and 2,032 Range
requests**. PUT counts include fault injection and repair. Bucket initialization
went directly to Moto; subsequent blob operations went through the proxy.
A separate loopback fixture check confirmed that nine conditional header variants
returned 501 without reaching the upstream, ordinary PUT and Range GET passed,
and response ETags were removed.

## Small-scale measurements

These observations came from one machine running the reference VM, Hot, gateway,
catalog, and Moto together. The S3 measurements include an extra HTTP hop through
the compatibility proxy. They are not measurements of Sakura or comparisons with
other storage approaches.

| Measurement | Local / replica 1 | Moto + proxy / replica 2 |
|---|---:|---:|
| Archived logical samples | 9,038 | 9,038 |
| Archive wall / CPU time | 0.163 / 0.093 s | 0.811 / 0.162 s |
| Archive throughput | About 55,380 samples/s | About 11,143 samples/s |
| Upper bound from completed child-process peak RSS | 58,104 KiB | 58,108 KiB |
| Pack / index / authority manifest | 41,087 / 35,260 / 3,526 bytes | 80,485 / 48,326 / 3,526 bytes |
| Hot-only median, reference → tiered | 1.20 → 2.50 ms | 1.37 → 2.57 ms |
| Cold-only median, reference → tiered | 1.62 → 5.94 ms | 1.80 → 559.23 ms |
| Boundary rate median, reference → tiered | 1.59 → 3.71 ms | 2.09 → 296.78 ms |

Archive CPU is the before/after difference in `getrusage(RUSAGE_CHILDREN)`.
RSS is that API's cumulative high-water mark, including completed child processes
such as provenance probes before archive execution. It is an upper bound, not an
isolated archiver peak. Long-running VM, catalog, Python/Moto, and proxy CPU/RSS
are not attributed to the archiver.
Latency is the median of five executions of each fixed query, with both VM result
cache and gateway block cache disabled. Process and OS caches were warm. These
small fixtures and short runs establish neither a performance target nor an
advantage over other approaches.

A separate cache test enabled 32 MiB of cache and queried `cold_only[30m]` twice.
With Moto + proxy, the first query took 328.25 ms, performed six Range GETs, and
downloaded 16,932 blob bytes. The second took 3.30 ms, with zero Range GETs and zero
blob bytes. Values matched exactly; the VM result cache remained disabled.
Using 60 returned samples × 16 bytes as the denominator, blob read amplification,
including the index, was 17.64×. Catalog traffic uses a separate HTTP service and
is excluded from blob counters; the authority is still checked with a warm cache.

## Evidence provenance and untested scope

Both JSON files record binary, harness, and Go source-tree SHA-256 values, executor,
and timestamps. Both runs used vmtier binary SHA-256
`96e70de0a45cf8820394b8edfde1c4ae53acf6accd6e7af7d41cd9d5304ed3c4`
and Go source-tree SHA-256
`4c53aa85609a86ee29de72e34a892f535c43aa9a25b70bd3f7621f0775964720`.
Logs, fixtures, and isolated data were left in
`.cache/v2-local-20261106-final` and `.cache/v2-s3-20261106-final`.
Both suites passed on their first execution with this implementation. No GitHub
push, publication, or write to an external service was performed during validation.

Authorization, TLS, durability, consistency, and performance on actual Sakura,
AWS S3, or MinIO services remain untested. Other untested areas include sustained
or high-cardinality load, deletion by actual retention expiry, late-write rejection
at every ingress, resharding, catalog updates with frontend result cache enabled,
other VM versions, single-node VM, and measured comparisons with Thanos or Parquet.
Moto's SigV4 path does not replace authentication testing against a real service.

The catalog is a persistent authority on one host. It provides neither distributed
HA nor snapshot rollback detection across a restart of all client processes.
Actual power-loss durability testing has not been performed. See
[operations.md](operations.md) for external lateness enforcement, retention
headroom, backup/restore, and other prerequisites. Short fuzz runs and codec
benchmarks from v1 are historical evidence, not measurements rerun for v2.
