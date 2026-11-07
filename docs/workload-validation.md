# Local workload and combined memory qualification

The local measured envelope is **ten tenants, 5,000 series per tenant, 30-second
samples, one-hour windows, two dense hours, and two Hot replicas**. One series per
tenant also carries 24 hours of history. This lowers the original 10,000-series
target: its broad query exceeded the unchanged 128 MiB work allowance. The
[failed target](results/workload/target-10000-failed/results.json) is retained,
along with a separate [post-run diagnosis](results/workload/target-10000-failed/diagnostic.json).

This is a conditional local envelope, not a series-only production capacity
promise. Physical native block size, replica overlap, label sizes, compressibility,
query lookbehind and concurrency affect capacity. A longer-lived Hot instance can
export blocks spanning more than these two dense hours at the same active-series
count. Run [preflight](preflight.md) on representative busy windows and measure
the intended query workload before deployment. The full cohort does not have
24 hours of dense data in these runs. There is no real-provider, natural-retention,
ingress-enforcement or universal latency claim.

## Fixture, settings and independent checks

[`scripts/qualify_workload.py`](../scripts/qualify_workload.py) creates separate
stock reference and Hot clusters. The same bounded batches are ingested into
both. A Cold-only stock vmselect reads the archived data, so successful Cold
queries cannot be satisfied by retained Hot data. Every successful workload
query is compared exactly with the reference, including timestamps, labels and
numeric strings. Result caching is disabled and deduplication is `1ms`.

The metric has `job`, `instance` and ten `group` values; sample values follow a
deterministic integer pattern. Input contains 12,026,400 logical samples at the
lowered envelope. The archive counter must equal that input count, and metadata
must return exactly 5,000 series for every tenant. A complete-hour
`sum(count_over_time(qualify_load[1h]))` also checks the independent expected count
of 600,000 per tenant. These checks supplement reference equality.

The fixture runs on Linux amd64 in a shared development container with a four-CPU
cgroup quota and 16 GiB memory limit. Each workload child uses `GOMAXPROCS=2`.
The stock VM binaries are pinned to
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`; vmtier uses Go 1.27.3. The raw reports
record the CPU model, affinity, kernel, process commands and hashes. Other local
VM suites had stopped before the full workload runs. This is not a dedicated
benchmark host.

| Setting | Effective value |
| --- | ---: |
| Per-query logical work | 128 MiB |
| Shared query reservation allowance | 256 MiB, two full reservations |
| Raw immutable cache | 16 MiB |
| Shared cache-miss flights | 16 |
| RPC concurrency / maximum duration | 8 / 30 seconds |
| Per-stream archive attempt | 2 minutes |
| Archive concurrency | One window at a time |
| Stock process `memory.allowedBytes` | 128 MiB per process; not an RSS limit |
| Blob backend | Local filesystem |

No production limit was increased to make the target pass. The raw reports also
record the archive age, coverage horizon and conservative 240-hour configured
Hot recovery retention. Stock fixture storage actually retains 30 days. A fixed
real-wall-clock horizon keeps exactly 24 windows per tenant eligible for the run;
the harness fails if its 30-minute horizon expires.

## Why the original target failed

The 10,000-series target completed all 240 catalog windows, but its first broad
query failed with `query decoded/read budget exceeded`. The failure occurred
before successful query output; it is a query-capacity failure, not failed
archival or silently partial results.

The post-run inspection finds **4,805,280 physical rows** in each dense one-hour
pack for the first tenant, in 20,000 native blocks. Most series have two physical
blocks of 240 rows: the two-hour native block crosses the logical window boundary
and is present from both replicas. The long-history sentinel adds further rows.
The decoded charge alone is 153,768,960 bytes at 32 bytes per physical row, above
128 MiB before the 5,368,921-byte encoded index, parsed directory, payload or
copied-name charges. Counting only the 1.2 million logical samples in the hour
would miss that work. Clipping and deduplication do not eliminate the need to
validate the complete stored blocks.

The failed report was not rewritten. Its original harness stopped before recording
the final archive summary once it detected the background query failure; the
separate read-only diagnosis records generation 240 and complete coverage.
Later harness revisions capture completed archive evidence before propagating a
query failure.

## Archive, query and pressure measurements

The [initial 5,000-series run](results/workload/baseline-5000/results.json) passed
248 exact comparisons. Its 240 windows caught up in 43.277 seconds, including
12,026,400 logical samples and 49,341,604 physical rows across the committed
indexes. The full dense cohort represents two generated hours across all tenants;
its catch-up ratio was 166.37 generated hours per wall hour. That short-run ratio
is not a sustained production throughput guarantee.

That run issued one sequential broad/narrow query stream during catch-up. Its
vmtier sampled peak RSS was 175.32 MiB during archiving and queries. Cache eviction
and the admission burst were exercised afterward. A separate final combined run
below exercises those pressures while archival is still making progress.

The [final combined run](results/workload/concurrent-5000/results.json) passed on
2026-11-06. Four requests at a time rotate over tenants whose first dense window
is already committed. Expected reference results are prepared before the timed
phase. Evidence counts as concurrent pressure only while the persisted catalog
generation is below the final 240; observations after completion cannot satisfy
the check.

| Final combined observation | Measured value |
| --- | ---: |
| Complete exact comparisons | 250 |
| Archive catch-up after source outage | 51.806 seconds |
| Logical samples committed | 12,026,400 |
| Physical rows across committed indexes | 49,359,200 |
| Logical windows / packs | 240 / 240 |
| Catch-up logical samples per second | 232,144 |
| Generated dense hours per wall hour | 138.98 |
| Exact query successes during catch-up | 166 |
| Explicit admission refusals during catch-up | 166 |
| Commits after concurrent queries started | 18 (generation 222 to 240) |
| Cache evictions observed before completion | 1,394,394 |
| Simultaneous reservations observed before completion | 256 MiB |
| vmtier sampled peak RSS during archive/queries/pressure | 255.117 MiB |
| Synchronized test-topology child RSS peak in that phase | 765.219 MiB |
| Largest sampled Go heap-object total | 186.897 MiB |
| Largest single-query logical charge | 87.960 MiB |
| Largest parsed-directory estimate | 5.504 MiB |
| Archive verifier / upload component peaks | 21.705 / 11.104 MiB |

The source outage lasted 3.081 seconds, produced 290 failed source requests and
left generation zero. Catch-up then completed while query pressure was active.
The initial configured retention headroom after the safety margin was 763,497
seconds. Catch-up consumed only a small fraction in this fixture; this does not
establish recovery from a multi-day provider outage. The later six-request burst
returned two exact successes and four explicit admission refusals. A deliberate
1 KiB query allowance failed without publication changes, and restoring the
original settings restored exact reads. Reservations drained to zero.

The following distributions have ten observations each, one per tenant. They
describe sequential post-catch-up checks with the cache state shown. With ten
samples, this harness's p95 is the observed maximum. They are not a latency SLO.

| Operation | Median / p95 ms | Index GETs / Range GETs per call | Median downloaded bytes |
| --- | ---: | ---: | ---: |
| Narrow 1h, initially uncached tenant | 65.18 / 74.44 | 2 / 3 | 2,684,768.5 |
| Immediate narrow 1h repeat | 57.55 / 62.83 | 0 / 0 | 0 |
| Broad 1h range aggregation, index already cached | 248.45 / 265.52 | 0 / 3 | 530,246 |
| Complete-hour sample count, warm | 239.65 / 263.18 | 0 / 0 | 0 |
| Narrow 24h sentinel, earlier history uncached | 121.51 / 158.64 | 22 / 23 | 2,702,992 |
| Label names, warm | 222.44 / 252.62 | 0 / 0 | 0 |
| Instance label values, warm | 222.31 / 261.53 | 0 / 0 | 0 |
| Series metadata, warm | 226.00 / 248.61 | 0 / 0 | 0 |

After cycling all tenants, revisiting the first broad query required two index
GETs, two Range GETs and 3,203,923 downloaded bytes, confirming displacement from
the 16 MiB cache. The range aggregation evaluates from minute five through the
last 30-second sample of the hour; the separate complete-hour count covers all
120 samples per series. Native block formation can vary between identical input
runs, explaining differences in physical rows and object sizes between baseline
and final reports.

Both full runs use vmtier binary SHA-256
`783f9cf6072cf2248c9fecb119f2a807ce3f8c1274d52f798772ee153c978fba`
and Go source-tree SHA-256
`a21e9a75ab811085b1ef84ce528b564ed6cd582e619d213ec3c5aed591e03790`.
The final workload harness SHA-256 is
`d6805c0a00e1c78b197ceae94c70c6ff679d2b3e6ffd6c44ef35ee2e80227416`.
The Go tree includes tests; runtime changes are in commit `6cbf208`, with the
final concurrent-pressure harness in `d9be2a2`.

## Memory interpretation and default choice

The [accounting guide](query-memory.md) defines downloaded bytes, parsed index
estimates, physical decoded rows, copied names, cache, archive verifier and upload
buffers. The raw evidence includes component counters, Go heap/allocation metrics,
reservations and `/proc/PID/status` samples. Their values are complementary; the
logical 256 MiB admission allowance is not a process RSS ceiling.

RSS samples are nominally 100 ms apart, with actual observed median/maximum gaps
recorded. Metrics scraping has a one-second timeout and adds sampling overhead.
The final run's actual median and maximum sampling gaps were 106 ms and 257 ms.
Shorter peaks can be missed. Per-process `VmHWM` is reported separately and never
summed. The synchronized child-process sum includes both Hot and reference VM
clusters plus the Cold frontend and vmtier; it describes this test topology,
not a production deployment's physical-memory requirement. Shared pages are
counted by each process. The Python ingestion/proxy harness is reported separately,
and cgroup usage may include unrelated session activity.

Keep the 128 MiB/query and 256 MiB shared defaults for the qualified workload,
with the measured 16 MiB cache. The original target demonstrates that these
defaults refuse excess work rather than promising every active-series count fits.
Larger cache settings, larger labels, longer physical blocks, full-cohort 24-hour
queries and a different backend require new measurements. Choose the host limit
with margin for GC, SQLite, HTTP/RPC buffers and workload variation above observed
RSS; this short local fixture does not select a universal container memory limit.

## Catalog scale and regression checks

The [separate catalog qualification](catalog-capacity-validation.md) published
87,600 logical windows through the real SQLite API. It verifies bounded 1-hour
and 24-hour selection, explicit plan refusals and restart, while recording the
cost of full catalog-capacity metric scans. Those descriptors do not represent
87,600 windows of this payload workload.

`make build test race vet` passed. The retained JSON and SQLite suites passed
1,440 exact instant/range comparisons plus metadata, corruption, outage, restart,
and S3 admission checks with the new runtime. Their raw reports are retained as
[JSON local](results/query-memory/json-local.json),
[JSON S3](results/query-memory/json-s3.json),
[SQLite local](results/query-memory/sqlite-local.json) and
[SQLite S3](results/query-memory/sqlite-s3.json). SQLite S3 forced four-block packs.
These regressions ran before the capacity measurements, with some suites running
concurrently; their timing is not capacity evidence.

The first [small smoke failure](results/workload/smoke-failed-reconnect.json)
encountered an old stock-vmselect pooled connection after a service restart. The
harness now starts a fresh frontend for each incarnation rather than interpreting
an unrelated EOF as a resource refusal. The corrected
[smoke run](results/workload/smoke-1000.json) passed 46 exact comparisons. All
15 Python harness tests pass, including a check that pressure observed only
after archive completion cannot qualify as concurrent pressure.

## Reproduction and raw memory evidence

After the pinned build described in [testing](testing.md), use fresh directories:

```sh
python3 scripts/qualify_workload.py --workdir .cache/workload-target \
  --require-cache-pressure
python3 scripts/qualify_workload.py --series 5000 \
  --require-cache-pressure --require-concurrent-pressure \
  --workdir .cache/workload-qualified
python3 scripts/catalog_capacity.py --output .cache/catalog-results.json \
  --workdir .cache/catalog-qualification
```

The first command is expected to preserve a failed report for the original target
under this native-block layout. Hosted CI uses a smaller two-tenant/1,000-series
fixture to check the harness and runtime; CI success is not a rerun of the full
capacity target. All process logs and failure reports remain in each work directory.

Raw memory samples are stored beside the full-run reports as
`memory-samples.json.gz`. Decompression reproduces the bytes whose SHA-256 appears
in each report's `memory.raw_samples_sha256`; compression does not change the
measurements or their original filename. Binary/source/harness hashes identify
the tested implementation independently from later documentation-only commits.
