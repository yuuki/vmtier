# Testing

Run commands from the repository root. The current SQLite service and the retained
JSON v2 runtime have separate E2E suites, using the same disposable stock-VM fixture.
This guide describes how to run the current tests; recorded runs are linked under
[validation reports](#validation-reports).

## Setup and fast checks

Follow [getting started](getting-started.md) to bootstrap dependencies and build
vmtier. The tested environment is Linux amd64; CI uses Ubuntu 24.04, Go **1.27.3**,
and Python **3.15.2**. The Go module requires at least 1.25.3. A C toolchain is needed
for CGO, including the bundled SQLite driver. E2E tests require local TCP listeners;
Docker and cloud credentials are not needed.

The initial [PR run](https://github.com/yuuki/vmtier/actions/runs/42209065311)
and [push run](https://github.com/yuuki/vmtier/actions/runs/42209044949) passed
their build, test, race, vet, and four E2E steps, but failed artifact upload because
the action rejected colons in nested fixture paths. Those runs were not successful
CI runs. Historical local result files remain unchanged.

## Pinned environment

The target is **VictoriaMetrics OSS v1.155.0-cluster**, commit
`cab3d5fbeeedd5d0374c1dabde93939af4c2499c`. No VM patches are applied.
`build-vm.sh` verifies the SHA-256 of a pinned source archive and builds
vmstorage, vminsert, and vmselect using upstream vendored dependencies.
The pinned toolchains are Go 1.27.3 and Python 3.15.2. Bootstrap verifies the
committed Go modules and regenerates vmtier's vendor tree independently of the
stock VM vendor tree. Historical result files retain their original toolchains
and VM revisions; they do not establish validation of an upgraded dependency set.
The retained JSON v2 suite tests an independent persistent HTTP catalog authority and
a blob store using ordinary PUT/GET/Range requests. The SQLite service has a separate
harness described below. Evidence from the previous
all-S3 catalog architecture is kept under [legacy-v1](results/legacy-v1/README.md)
and is not included in current results.

## SQLite service

After building vmtier and the pinned stock VM binaries and installing the Python
dependencies shown below, run:

```sh
make build test race vet
python3 -m unittest discover -s scripts -p 'test_*.py'
```

The Python tests cover subprocess runtime provenance, fixture timing, blob-outage
assertions, workload measurements, and evidence packaging. They use the standard
library and can run before building the binaries or installing Moto.

To prepare the E2E suites:

```sh
make stock
python3 -m venv .deps/e2e-venv
. .deps/e2e-venv/bin/activate
python -m pip install -r scripts/requirements-e2e.txt
```

`make stock` builds `.bin/vmstorage`, `.bin/vminsert`, and `.bin/vmselect` from the
bootstrapped, unmodified **VictoriaMetrics OSS v1.155.0-cluster**, commit
`cab3d5fbeeedd5d0374c1dabde93939af4c2499c`. Source downloads are SHA-256 pinned;
builds use upstream vendored dependencies. Use `make GO=/path/to/go ...` for another
Go binary. Set `GO_BIN=/path/to/go` for E2E commands if Go is outside `PATH`, so
the report can record its version.

The activated environment makes `moto_server` available to S3 tests. Alternatively,
set `MOTO_SERVER` to its executable path. The requirement pins Moto to **5.2.5**;
the local backend does not use it.

`GO_BIN` selects the Go binary. `VM_SOURCE_DIR` selects where the verified pinned
VM source is extracted; use a new directory or one already prepared by these
scripts. An arbitrary existing source tree is rejected. To validate only fixture ingestion into VM, use
`python3 scripts/e2e.py --vm-only`.

Each run creates an isolated directory and starts dedicated processes on available
loopback ports. Pass an **unused directory** to `--workdir PATH`; the suite does not
reuse existing data. At exit it stops only the processes it started and leaves
logs, configurations, `results.json`, and isolated VM data. It has no option to
connect to an external VM or a real S3 service. S3 tests use Moto 5.2.5 and public,
test-only credentials.

## Run the SQLite service suites

```sh
python scripts/e2e_serve.py --backend local --workdir .cache/serve-local-fresh
python scripts/e2e_serve.py --backend s3 --replicas 2 --pack-max-blocks 4 \
  --workdir .cache/serve-s3-fresh
```

`--pack-max-blocks 4` lowers the per-pack block limit so every fixture window must
produce multiple packs; the same logical coverage and exact query checks still
apply. Omitting it tests the default pack limits. See [archive packs](archive-packs.md)
for the bounded streaming pipeline and [archive validation](archive-validation.md)
for recorded multi-pack runs.

The harness starts one `serve` process with a SQLite catalog, scheduler, read RPC
gateway, and shared S3 request budget. In addition to the shared query and fault
checks below, it verifies:

- Explicit initialization; missing state, changed policy/source/blob locator/ID,
  and a second owner are rejected without changing committed state.
- A failed tenant remains uncovered while a healthy tenant progresses and can be
  queried; global readiness remains unsuccessful until all streams are covered.
- SIGTERM during native export publishes nothing. Restart archives automatically,
  then resumes from committed coverage without exporting completed windows again.
- A live owner excludes offline inspection, verification, and backup. Clean
  shutdown releases ownership for successful offline backup and verification.
- A stopped service causes queries to fail; restart restores reads from persisted
  state. S3 runs expose admitted-attempt and wait metrics.

Focused Go tests cover additional boundaries such as publication replay, verified
empty windows, streaming pack rotation and raw-oracle validation, bounded SQLite
selection, import/restore, LRU eviction, coalesced reads, shared query-memory
admission, and S3 retry pacing, cancellation, queue limits, and response-body
lifetime. E2E success alone does not establish the S3 limiter's maximum throughput.

## Run the legacy JSON v2 regression suites

```sh
python scripts/e2e.py --backend local --workdir .cache/v2-local-fresh
python scripts/e2e.py --backend s3 --replicas 2 --workdir .cache/v2-s3-fresh
```

These suites initialize a JSON catalog and start its persistent HTTP authority in
a separate process. Standalone `archive` and `gateway` commands use that authority;
the blob store contains only packs and indexes. JSON-specific checks include
SIGKILL during export, replay without manifest changes, fixed-contract rejection,
warm-cache queries and readiness failing on authority outage, durable authority
restart, and query/`verify` failures for corrupted objects.

`make e2e` and `make e2e-s3` are shortcuts for these **JSON v2** suites, not the
SQLite service suites. They build binaries first but do not install Python
dependencies. To test fixture ingestion into stock VM alone, use:

```sh
python scripts/e2e.py --vm-only --workdir .cache/vm-only-fresh
```

## Offline recovery and cutover

The dedicated recovery suite starts disposable stock VM clusters and a JSON v2
authority, then performs the documented offline import, backup, restore, and
SQLite service operations:

```sh
python3 scripts/e2e_recovery.py --workdir .cache/recovery-local
python3 scripts/e2e_recovery.py --replicas 2 --workdir .cache/recovery-replicated
```

The work directory must not already exist. `--gomaxprocs` uses the same precedence
as the other suites. `--rollover-delay` defaults to 60 seconds: the archive policy
is chosen so a real wall-clock eligibility boundary advances during the run,
without a production clock override or a changed archive contract. If import and
backup fail to finish before that deadline, the suite fails and asks for a larger
delay; it never silently skips the older-backup recovery check.

`report.json` retains provenance, each process command, exact comparison counts,
catalog IDs and coverage, unchanged import object hashes/stat identities, backup
age, downtime, retention headroom, and failure evidence. SIGKILL is injected after
native objects have been uploaded and before the independent raw oracle completes,
and again after a committed recovery. The expired-source negative case uses a
short configured recovery deadline and reconstructs an empty disposable Hot;
it does **not** establish natural VM retention expiry. See the
[measured recovery report](recovery-validation.md) for results and limitations.

Hosted CI runs the local recovery suite and includes its complete `ci-recovery-local`
work directory in the existing byte-preserving evidence archive, including on
failure. This is additional process recovery coverage, not a power-loss test.

## Workload and catalog qualification

After the same build setup, run the small local fixtures used by CI:

```sh
python scripts/qualify_workload.py --tenants 2 --series 1000 \
  --history-hours 4 --dense-hours 2 --replicas 2 \
  --workdir .cache/workload-smoke-fresh
python scripts/catalog_capacity.py --hours 24 --tenants 2 --repeats 1 \
  --output .cache/catalog-smoke-fresh/results.json \
  --workdir .cache/catalog-smoke-fresh/work
```

These exercise the qualification harnesses; their sizes do not establish the
full measured envelope. Use new work/output paths to preserve earlier evidence.
`catalog_capacity.py` builds its own dedicated test binary; select a Go executable
with `--go /path/to/go` (or `GO`), not `GO_BIN`. The expensive Go catalog fixture
skips ordinary `go test` unless invoked with its qualification environment.

The [workload report](workload-validation.md) gives full reproduction commands for
concurrent archival, exact Cold queries, cache eviction, admission refusal, and
sampled process memory. Its measured local envelope uses **5,000 series per tenant**;
the harness default of 10,000 is the original target whose broad query exceeded the
unchanged query allowance. Two dense hours plus one 24-hour sentinel series per
tenant do not qualify 24 hours of dense data for the whole cohort. Use the report's
explicit workload and pressure flags when reproducing its evidence.

The [catalog capacity report](catalog-capacity-validation.md) separately covers
87,600 logical windows. It publishes synthetic descriptors through the real SQLite
API and checks bounded selections, limit refusals, and restart; it performs no
blob I/O. Its publication rate is not archive throughput. [Memory accounting](query-memory.md)
explains why logical query reservations, Go heap metrics, cache limits, and sampled
RSS describe different parts of the workload.

## Work directories and shared assertions

Each run starts dedicated processes on available loopback ports. Use a new or empty
`--workdir`; nonempty directories are rejected to preserve earlier evidence. The
harness stops its own processes at exit and leaves logs, configurations,
`results.json`, and isolated VM data. If no directory is given, it creates a
temporary one and prints its path. Neither harness has an option to connect to an
existing VM or real S3 service. S3 fixtures use public, test-only credentials.

Both complete E2E suites:

1. Load the same full dataset into a reference VM and Hot. The fixture has tenants
   `1:2` and `7:9`, 16 series per tenant, and 17,654 input samples over five hours.
   It includes irregular gaps, counter resets, label churn, empty/absent labels,
   duplicate/conflicting timestamps, staleness, NaN, infinities, and millisecond
   archive boundaries. Tenant values differ.
2. Archive the oldest three hours in one-hour windows. Fixture creation chooses H
   so the next eligible window is 30 minutes away; Hot retention is 240h, allowed
   lateness is 30m, and the safety margin is 30m. No ingestion occurs after fixture
   setup except the controlled recent-only reconstruction below.
3. Confirm that stock vmselect reads Cold through the native gateway and Range
   GETs, and that Cold excludes young samples at and after the boundary.
4. Recreate Hot with a **new data directory** containing recent samples only and
   confirm old-range raw export is empty for both tenants. This tests Hot absence,
   not natural retention expiry.
5. Compare 30 expressions × two tenants × three periods (Cold, Hot, boundary) ×
   two APIs (instant, range): **360 exact comparisons per run**. Labels, timestamps,
   and VM's numeric strings must match after ordering, without floating-point
   tolerance. Expressions include rollups, aggregation, vector matching, offsets,
   subqueries, `@`, and regexp/negative matchers. Metadata checks compare labels,
   label values, and series discovery separately.
6. Establish baseline results with the Cold cache disabled, verify a warm 32 MiB
   immutable cache performs no additional Range GETs, reject missing/truncated/
   checksum-invalid payloads, restore exact queries after repairs, and reject Cold
   deletion explicitly.
7. Interrupt only blob access while the gateway/service stays running, require a
   query error identifying the blob failure and increased GET/error counters, then
   restore exact results without changing publication or coverage. Health and
   catalog/coverage readiness stay successful during this outage: readiness does
   not scrub blobs. Connection refusals or unrelated timeouts cannot pass this
   check. Reports retain the observations under `blob_outage`.

For S3, [the compatibility proxy](../scripts/s3_compat_proxy.py) sits in front of
Moto. It rejects conditional request headers with 501, removes response ETags,
and validates Content-MD5 on every PUT. The suites require zero conditional
requests, removed ETags, Range GETs, and valid MD5s. For blob outages the proxy
returns a distinct HTTP 503; the local backend instead moves object contents out
of the open root. These are protocol fixtures, not Sakura service tests.

All vmselect instances disable result caching and partial responses, set both
replication factors to one, and disable slow-replica skipping. Reference/Hot
storage and all vmselect instances use `-dedup.minScrapeInterval=1ms`.
`--replicas 2` creates two vmstorage instances per cluster and changes vminsert's
replication factor to two; Hot and Cold must still both participate in queries.

Combined frontends default to `-search.maxLabelsAPIDuration=30s`. Earlier uncached
S3 metadata reads exceeded the stock five-second deadline; [runs after the reader
changes](metadata-validation.md) passed this fixture with `--labels-api-duration 5s`.
Metadata still validates full payload blocks. See [metadata measurement](#metadata-measurement)
for deadline and repeat-measurement options; fixture success is not a general
metadata deadline guarantee.

Both fixtures record `created_at_ms`, `archive_age_ms`, `next_eligible_at_ms`, and
`finished_at_ms`. They must complete within their 30-minute eligibility horizon.
Crossing a UTC-hour boundary alone does not change required coverage; production
eligibility checks remain active without a runtime clock override.

## Metadata measurement

Both harnesses record `metadata_measurements` for label names, two label-value
selectors, and series discovery for both tenants. Each observation includes exact
reference comparison status, tiered latency, and deltas in object GET requests,
Range GET requests, and downloaded bytes. The initial `uncached` phase disables
the immutable cache; stock vmselect result caching stays disabled throughout.

To reevaluate the stock five-second labels deadline after reader changes and
collect repeat measurements on the same fixture, use fresh work directories:

```sh
python3 scripts/e2e.py --backend s3 --replicas 2 --labels-api-duration 5s \
  --measure-warm-metadata --workdir .cache/metadata-json-5s
python3 scripts/e2e_serve.py --backend s3 --replicas 2 --labels-api-duration 5s \
  --measure-warm-metadata --workdir .cache/metadata-sqlite-5s
```

`--labels-api-duration` accepts `5s` or the retained default `30s`; the effective
value appears in the report and the child vmselect command. A deadline failure
remains a failed suite and the partial metadata measurement is retained. Do not
interpret a successful query with fewer returned series as an improvement: all
phases keep the exact reference comparisons.

The optional `prime` and `warm` passes run after the existing 32 MiB cache test.
The prime pass may reuse entries from prior calls, so it is not a cold-cache
measurement. The subsequent warm pass is a repeated workload, not a guarantee
that every entry fits or that zero GETs occur. These per-call counters are process
observations; in this fixture archival has completed and does not upload during
the measurement. On S3, request counts include transport retries. Loopback latency
and this small fixture do not establish a general metadata deadline guarantee.

## CI and evidence

The [CI workflow](../.github/workflows/ci.yml) runs Python regression tests,
`make bootstrap build test race vet stock`, the workload and catalog smoke fixtures,
all four JSON/SQLite E2E suites, and the local recovery suite on pushes and pull
requests. SQLite S3 forces four-block packs. CI then packages available `.cache/ci-*`
work directories, including qualification and recovery output, and uploads
the `e2e-evidence` artifact. Packaging and upload use `if: always()`, so test failures
can still produce diagnostic evidence; artifact upload does not turn a failed test
into a success.

After downloading and unzipping the artifact, extract it in an empty directory:

```sh
tar -xzf e2e-evidence.tar.gz
```

The tar preserves tenant paths containing colons, hidden files, logs, and result
bytes. A failed suite may leave partial output; an empty tar means no suite work
directory was available. [CI evidence validation](ci-evidence-validation.md)
records an earlier successful artifact round trip and the upload issue it fixed.

## Runtime provenance

Both suites select workload-child `GOMAXPROCS` in this order: `--gomaxprocs N`,
the outer environment variable, then `2`. The value must be a positive integer and
applies to managed services and one-shot archive/maintenance commands alike:

```sh
GOMAXPROCS=7 python scripts/e2e_serve.py --gomaxprocs 3 \
  --backend local --workdir .cache/runtime-local
```

Here every workload child receives `3`. `results.json` records the outer value,
selection source, selected value, commands, allowlisted runtime environment, and
exit status when available, including failed commands. `--vm-only` writes this
process provenance to `runtime.json`. Credentials and unrelated environment
variables are not recorded. Executable-version probes are outside the workload.

Reports also record hostname, binary/harness/Go-source hashes, UTC timestamps,
OS/Python/Moto versions, range-query timing distributions, cache measurements,
and object bytes/counters. The JSON v2 report additionally measures one-shot
archive wall time, CPU, and child-process peak RSS. RSS uses Linux
`getrusage(RUSAGE_CHILDREN).ru_maxrss`, which can include earlier provenance
probes; it is not total service memory and does not attribute long-running
VM/catalog or Python/Moto resources to archive.

Expressions evaluated in the Hot period can still read Cold through lookback or
offsets, so `hot` timings do not mean Hot-only overhead. In the JSON v2 cache report,
read amplification means blob bytes including indexes divided by returned raw
samples × 16 bytes; it excludes catalog HTTP traffic and is not a ratio to
compressed pack size. VM result caching stays disabled throughout.

Loopback results do not qualify real Sakura/AWS/MinIO authentication, TLS,
durability, latency, sustained load, or high-cardinality capacity. Natural Hot
retention expiry, external lateness enforcement, resharding, multiple writers,
stock VM result-cache behavior, and other VM versions are outside these suites.

## Validation reports

Reports describe the named run and revision; later harness changes do not
retroactively alter their evidence. Keep original result JSON unchanged.

| Report | Scope |
| --- | --- |
| [Workload and memory](workload-validation.md) | Local measured workload, concurrent archive/query pressure, and retained failed target |
| [Catalog capacity](catalog-capacity-validation.md) | 87,600 synthetic window descriptors; catalog-only scope |
| [Recovery and cutover](recovery-validation.md) | Offline import, older-backup catch-up, and process crashes |
| [Metadata](metadata-validation.md) | Exact metadata results, request counts, and the five-second deadline fixture |
| [Archive packs](archive-validation.md) | Streaming multi-pack publication and bounded-window failures |
| [Reader](reader-validation.md) | LRU, shared reads, coalescing, and bounded query behavior |
| [Raw oracle](raw-oracle-validation.md) | Incremental raw verification and allocation evidence |
| [Implementation regression](implementation-validation.md) | Earlier archive/reader implementation checks |
| [Harness foundations](foundation-validation.md) | Runtime provenance, fixture timing, and stronger blob-outage assertions across all four suites |
| [SQLite service](serve-validation.md) | Service integration, scheduler, ownership, and shared S3 budget; includes preserved failures |
| [SQLite catalog](sqlite-catalog-validation.md) | Offline catalog implementation, import, and maintenance |
| [CI evidence](ci-evidence-validation.md) | Hosted artifact packaging and byte-preserving extraction |
| [JSON v2 MVP](validation.md) | Historical separate-process MVP results and measurements |
| [All-S3 v1 archive](results/legacy-v1/README.md) | Superseded architecture; excluded from current runtime evidence |
