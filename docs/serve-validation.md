# SQLite service stage 2 validation

The SQLite-backed `serve` process was tested on 2026-11-06 with unmodified OSS
VictoriaMetrics `v1.128.0-cluster`, commit
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. This stage connects the scheduled
archiver, persistent catalog, read gateway, and shared S3 transport in one owner.
The earlier [offline SQLite evidence](sqlite-catalog-validation.md) and
[JSON v2 MVP evidence](validation.md) remain separate historical reports.

Both final SQLite suites passed **720 exact query comparisons in total**:

| Backend | Exact query comparisons | Scenario checks | Result |
| --- | ---: | ---: | --- |
| Local blobs, one Hot replica | 360 | 17 | Passed |
| Moto + restrictive S3 proxy, two Hot replicas | 360 | 19 | Passed |

The raw [local report](results/serve-stage2/local.json) and
[S3 replicated report](results/serve-stage2/s3-replicated.json) were copied
unchanged. Final runs were sequential: local ran from `10:14:08` to `10:14:17`
UTC and S3 from `10:14:17` to `10:16:06` UTC on 2026-11-06. The executor was
`05fa190d414f`, Debian 13 / Linux amd64, Go 1.27.3, Python 3.12.14, with five
available CPUs. The outer invocation for all four final suites set `GOMAXPROCS=3`.
The historical `Processes.start` implementation overrode this with `GOMAXPROCS=2`
for managed children, including stock VM, gateway, `serve`, and interrupted archive
processes. One-shot CLI commands launched by `command_stats` inherited the outer
value of `3`. This corrects runtime provenance; the unchanged raw results still
establish the exact comparisons below. The S3 fixture used Moto 5.1.15. All traffic
stayed on loopback.

## Executed checks

`make GO=/workspace/tools/go/bin/go build test race vet` passed before the measured
integration runs. Focused Go tests cover transactional publication, empty windows,
independent stream retry and timeout, retained coverage after restart, bounded
SQLite query plans, fail-closed authority reads, and S3 transport admission. The
transport tests include SDK retries, cancellation, queue rejection, and holding
the concurrency slot until a response body is closed. These are focused tests,
not provider load tests.

The new integration harness is [scripts/e2e_serve.py](../scripts/e2e_serve.py).
It reuses the MVP fixture and runs disposable stock VM processes, local state,
and, for the S3 backend, Moto behind the restrictive compatibility proxy.
Each backend compares 30 expressions across two tenants, three periods (Cold,
Hot, and the boundary), and two APIs (instant and range). Timestamps, labels,
and numeric strings must match exactly after order normalization. No numeric
tolerance is applied. Metadata checks are additional.

The fixture has 16 series per tenant and 17,654 input samples over five hours.
It includes counter reset, irregular gaps, stale markers, NaN and infinities,
duplicate/conflicting timestamps, empty and absent labels, and the boundary at
millisecond precision. Queries include rollups, aggregation, vector matching,
negative and regex matchers, offsets, subqueries, and `@`.

The service checks exercise:

- Missing state is rejected without implicitly creating a database. Initialization
  is explicit, and changes to the fixed archive policy, source URL, blob locator,
  or catalog identity are rejected without changing committed state.
- With the export source unavailable, `/health` remains available, `/ready`
  returns 503, coverage remains unpublished, and required Cold queries fail.
  During a tenant-specific source outage, the healthy tenant advances and remains
  queryable while the failed tenant remains uncovered and global readiness is 503.
- SIGTERM during a paused native export exits cleanly without publishing a
  window. Restart automatically archives six tenant/source windows, advances
  generation to six, and records 9,038 logical archived samples. Archive metrics
  are captured before a later restart resets process counters.
- A live owner prevents a second daemon and offline inspection, verification,
  and backup. After clean shutdown, offline backup and full verification succeed.
- Stock vmselect reads the native packs through Cold RPC and object Range GET.
  Cold raw export excludes samples at and after the boundary. Hot is then
  recreated with recent samples only; old-range raw export is empty for both
  tenants. Restart preserves all catalog rows and does not export those completed
  windows again.
- A warm 32 MiB immutable cache returns the same values without extra Range GETs
  or blob bytes. Stopping the service makes the previously warm query fail;
  restart restores it from persistent state. With the cache disabled, missing,
  truncated, and checksum-invalid payloads fail queries, and restoring the bytes
  restores exact reference results. Cold deletion remains explicitly unsupported.

The S3 proxy rejects conditional request headers and strips response ETags. The
final run recorded zero conditional requests, 2,041 removed ETags, 1,617 Range
requests, and valid Content-MD5 for all 17 PUTs. Counts include the harness's object
inspection, fault injection/repair, and post-shutdown verification. They are not
a measurement of the service's rate-limit ceiling. The shared transport exposed
admitted-attempt and wait metrics with no queue rejection; focused tests establish
retry pacing and queue bounds. Bucket initialization bypasses the proxy; subsequent
blob operations use it. No legacy `catalog.json` is written to the blob store.

The two final warm-cache checks performed zero additional Range GETs and read zero
blob bytes. After intentional service outage, the same frontend recovered in
0.207 seconds for local blobs and 0.464 seconds for S3, with two cached-dial
retries in each case. These are observed times from the recovery query's start,
after daemon readiness, through its first successful exact response. The raw
reports include query timing summaries and cache timings. They are small loopback
observations with stock VM result caching disabled, not capacity or provider
performance claims.

## Metadata deadline found during validation

The first S3 run completed all 360 differential query comparisons, then failed
the first broad label-name request. The stock vmselect default
`-search.maxLabelsAPIDuration=5s` expired; vmselect reported
`Cold search: context deadline exceeded` after 5.291 seconds. The unchanged
[failed report](results/serve-stage2/s3-default-metadata-timeout.json),
[fixture configuration](results/serve-stage2/s3-default-metadata-timeout.config.json),
and [timeout log excerpt](results/serve-stage2/s3-default-metadata-timeout.log)
are retained. The configuration contains disposable loopback endpoints only.

Stage 2 metadata still uses validated full-block searches. An uncached scan can
exceed five seconds when the shared S3 budget admits 80 attempts per second.
The final harness explicitly starts the tiered stock vmselect with
`-search.maxLabelsAPIDuration=30s`; it records this setting in its report. This is
timeout tuning for the tested fixture, not a metadata performance improvement or
a guarantee that arbitrary workloads fit within 30 seconds. The S3 request budget
remains 80 attempts per second, eight in flight, and 64 queued. More selective
metadata reads and range/cache improvements remain later work.

The unchanged JSON v2 S3 regression also completed all 360 comparisons and then
hit the default five-second deadline on a label-values request. That path has no
shared request budget, so the limitation is not exclusive to paced requests.
Its [failed report](results/serve-stage2/json-s3-default-metadata-timeout.json),
[configuration](results/serve-stage2/json-s3-default-metadata-timeout.config.json),
and [log excerpt](results/serve-stage2/json-s3-default-metadata-timeout.log) are
retained. Both final harnesses explicitly use the 30-second labels deadline on
their tiered frontend. This changes a stock vmselect flag and test reporting;
it does not patch VM or change the required query results.

A subsequent SQLite service S3 run passed all 360 comparisons and metadata checks,
then exposed a restart race in the harness. The daemon was ready, but the query
frontend still held the connection-refused error from the intentional outage. Stock vmselect
checks unavailable storage connections once per second. The harness now allows
up to five seconds for that specific cached connection-pool refusal to clear,
records retries and elapsed time, and compares the first successful response
exactly. It does not retry other errors or result mismatches. The
[failed report](results/serve-stage2/s3-frontend-reconnect-race.json),
[fixture configuration](results/serve-stage2/s3-frontend-reconnect-race.config.json),
and [frontend and restarted-service log excerpt](results/serve-stage2/s3-frontend-reconnect-race.log)
preserve the evidence. Daemon readiness does not imply immediate reconnection by
an already running frontend.

## JSON v2 regression

The existing JSON v2 runtime passed another **720 exact comparisons** after the
shared archive/reader changes. These are separate from the SQLite counts above.
Both final regressions use the explicit 30-second tiered frontend metadata
deadline described above; the fixture and result comparisons are unchanged.

| Backend | Exact query comparisons | Scenario checks | Result |
| --- | ---: | ---: | --- |
| Local blobs, one Hot replica | 360 | 19 | Passed |
| Moto + restrictive S3 proxy, two Hot replicas | 360 | 20 | Passed |

Raw reports are [JSON local](results/serve-stage2/json-local.json) and
[JSON S3 replicated](results/serve-stage2/json-s3-replicated.json). They ran
sequentially from `10:08:18` through `10:14:08` UTC on 2026-11-06, before the
two SQLite suites, with the same binary and Go source hashes. Full logs remain in
`.cache/serve-stage2-json-local-final4` and `.cache/serve-stage2-json-s3-final4`.

An earlier JSON S3 attempt crossed an hourly eligibility boundary while queries
were running. The legacy harness archives a fixed interval with fixed archive
age; it correctly received a required-Cold-coverage error when the eligible
interval advanced. Its [report](results/serve-stage2/json-s3-hour-boundary.json)
and [log excerpt](results/serve-stage2/json-s3-hour-boundary.log) are retained.
The historical final retry started in a fresh hour. This timing failure is distinct
from the metadata deadline failure above. Both current harnesses now choose a fixed
age at fixture creation that places the next eligible window 30 minutes away,
independent of the UTC-hour phase. They record that deadline and reject a run that
outlives it; no production clock or coverage behavior is changed. The retained
reports and logs above are historical evidence, not reruns of the updated harness.

## Reproduction and scope

All four final reports match the source tree and all binary/harness hashes verified
after the runs. The vmtier binary SHA-256 is
`30357c7049c53dbfc35e89b871aa129ec68d487d04ffa680a1f05cb604248dfd`;
the Go source-tree SHA-256 is
`e43295863e6fcc702bebe2a3b67702799cb9c515f31e6b8a5a4a757c74a99cdf`.
The report specifies the source-hash scope and records the three stock binary
hashes and each harness hash. Full logs and disposable data remain in
`.cache/serve-stage2-local-final4` and `.cache/serve-stage2-s3-final4`.

After building the stock binaries and installing `scripts/requirements-e2e.txt`,
run the suites sequentially with fresh directories. Current harnesses apply the
selected `GOMAXPROCS` to both managed children and one-shot workload commands and
record each launch in `results.json` (see [runtime provenance](testing.md#runtime-provenance)).
These commands therefore describe current reproduction settings, not the mixed
child settings in the historical reports:

```sh
export GOMAXPROCS=3
make build test race vet stock
python3 scripts/e2e_serve.py --backend local --workdir .cache/serve-local
python3 scripts/e2e_serve.py --backend s3 --replicas 2 --workdir .cache/serve-s3
```

CI now appends these two service suites after the existing JSON v2 suites and
uploads their work directories with the existing evidence artifact. Its job
timeout was increased from 25 to 35 minutes. The workflow was edited locally;
hosted CI was not executed.

The harness does not wait for natural Hot retention expiry: it recreates Hot with
recent data and proves that the old range is absent before comparing queries.
It sets `lateness_enforced=true` as a test assumption; it does not implement an
ingress policy or prove that production late writes are rejected. Production
completeness still requires enforced `H > L`, sufficient actual retention
`R > H + safety + window`, and successful publication before that retention
expires. Later writes into a published window are not added to Cold. History
before the configured coverage origin is outside the guarantee.

No real Sakura, AWS, or MinIO service was accessed. The proxy tests absence of
conditional-request and ETag dependencies, not provider authentication,
consistency, TLS, durability, or capacity. Natural retention transitions,
sustained/high-cardinality load, power loss, cross-host failover, other VM
versions, single-node VM, and external ingress enforcement remain untested.
Streaming multi-pack production, incremental oracle decoding, range coalescing,
and LRU replacement remain planned. The current bounded single-pack producer and
existing cache behavior are still in use. No hosted CI run, publication, or
deployment was performed as part of these local checks.
