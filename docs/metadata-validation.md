# Cold metadata validation after reader changes

On 2026-11-06, both JSON v2 and SQLite S3 fixtures passed with stock vmselect's
**five-second labels API deadline** after the LRU/cache-miss and adjacent-range
changes. Each final run passed 360 exact sample-query comparisons and 24 exact
metadata comparisons. The measured fixture needs no additional metadata-specific
optimization or 30-second deadline override. The harness retains its configurable
30-second default; this observation is not a general latency guarantee.

## Compared runs

The original runtime was rerun with the same final instrumented Python harness.
“Before” and “after” refer to runtime implementations, not chronological run order.
The before checkout is `4a33e5b` (foundation fixes only): its Go runtime matches
the stage 2 implementation. Only `scripts/e2e.py` and `scripts/e2e_serve.py` were
copied from the integration checkout, and the original foundation binaries were
copied without rebuilding them against changed Go sources. The
[setup record](results/metadata/baseline-setup.json) records that distinction.

Unchanged raw reports:

| Runtime | Before: 30-second deadline | After: five-second deadline |
| --- | --- | --- |
| JSON v2 | [11:49:49–11:52:58 UTC](results/metadata/before-json-s3.json) | [11:48:48–11:49:58 UTC](results/metadata/after-json-s3.json) |
| SQLite | [11:53:13–11:56:00 UTC](results/metadata/before-sqlite-s3.json) | [11:50:05–11:51:13 UTC](results/metadata/after-sqlite-s3.json) |

All four runs passed. Each uses two tenants, two Hot replicas, 16 series per
tenant, 17,654 input samples, five hours of 30-second samples, and three archived
one-hour windows per tenant. The sample/metadata ranges are identical across runs:
`start_ms=1793941200000`, `boundary_ms=1793952000000`, `end_ms=1793959200000`.
Both service runs use the default pack block limit, not the forced multi-pack
test. Physical native blocks and object sizes vary across independent stock-VM
ingestions; these are the same logical fixture, not byte-identical stored packs.

The host was Debian 13, Linux amd64/kernel 6.18.57, five available CPUs, Go 1.27.3,
Python 3.12.14, and Moto 5.1.15. All workload children received `GOMAXPROCS=3`.
VM memory flags were 128 MiB and result caches were disabled. SQLite S3 admission
was 80 requests/second, eight concurrent requests, and 64 queued requests; the
legacy JSON fixture has no shared S3 rate limiter. Traffic stayed on loopback
through the restrictive S3 proxy. Other validation overlapped on this host, so
latencies are descriptive observations, not an isolated CPU benchmark.

## Per-call observations

Each row summarizes eight calls: label names, `cold_label` values, metric-name
values, and series discovery for both tenants. GET/Range/byte columns describe
the minimum–maximum per call, not a total. Latency is the median with the observed
minimum–maximum in parentheses. Full GETs and Range GETs are separate counters.

| Runtime / phase | Full GETs | Range GETs | Downloaded bytes | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| JSON before, uncached | 3 | 134 | 74,053–74,059 | 6,699 (6,659–6,879) |
| JSON after, uncached | 3 | 3 | 67,113–70,358 | 297 (251–303) |
| SQLite before, uncached | 3 | 110–128 | 67,911–73,128 | 5,992 (5,523–6,539) |
| SQLite after, uncached | 3 | 3 | 66,337–72,258 | 305 (270–314) |
| JSON before, warm | 0 | 0 | 0 | 10.1 (7.7–16.6) |
| JSON after, warm | 0 | 0 | 0 | 7.3 (6.3–9.0) |
| SQLite before, warm | 0 | 0 | 0 | 7.7 (5.2–17.9) |
| SQLite after, warm | 0 | 0 | 0 | 9.3 (6.3–15.1) |

The uncached phase sets immutable-cache capacity to zero. The warm phase repeats
a complete metadata pass after priming a 32 MiB cache; raw reports also retain the
prime calls. All calls compare full results to stock reference VM exactly, with
actual sample-time filtering and tenant isolation. Fewer Range GETs are consistent
with reading adjacent selected blocks in each of the three packs together. Do
not attribute the downloaded-byte differences to coalescing: native pack contents
and index sizes differed between runs, and coalescing does not overfetch gaps.
Both implementations fit this small warm fixture; these measurements alone do
not demonstrate the LRU's behavior under eviction pressure.

## Provenance and reproduction

The before vmtier SHA-256 is
`44bf5973c9e3eb4e2083b4e2ccaf05c5de5c2c3e8ba14869a67decc7addd64a7`;
the after SHA-256 is
`63396250a4c99cd0325b5d6b266c3c87e86a92565f36c2148e1f81f1a9f772b4`.
Before/after Go source-tree hashes are respectively
`e43295863e6fcc702bebe2a3b67702799cb9c515f31e6b8a5a4a757c74a99cdf` and
`e2a8a391dbd6e321a8653a346c51b2196d8d0f9ac7c55ec5605489f55b21d069`.
The reports define the hash scope and retain full per-process commands/settings.
All stock VM binary hashes match across runs. Each harness hash also matches
between its before/after pair; the runtime binaries and source hashes differ.

Follow [metadata measurement commands](testing.md#metadata-measurement), using
`--gomaxprocs 3 --measure-warm-metadata` and fresh work directories. Use
`--labels-api-duration 30s` for the old runtime and `5s` for the current runtime;
do not silently retry failed deadline runs or reuse their evidence directories.
The baseline's old runtime was not rerun with the five-second setting in this
comparison. Historical five-second failures remain in
[stage 2 validation](serve-validation.md#metadata-deadline-found-during-validation).

This verifies the retained small fixture after the reader improvements. Larger
catalogs/cardinality, provider latency/quotas, concurrent archival, and sustained
pressure remain separate qualification work. Full payload/native-block validation,
coverage authority, corruption rejection, and query budgets remain active; no
index-only metadata shortcut was introduced. See [reader validation](reader-validation.md)
for the focused cache, range, and failure tests.
