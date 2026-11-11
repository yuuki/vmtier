# Reproducing validation

Run commands from the repository root. Choose a suite for the runtime and behavior
being changed; historical reports describe their recorded revisions, not the
current checkout.

| Validation | Procedure |
| --- | --- |
| Go and harness regressions | [Setup and quick checks](#setup-and-quick-checks) |
| SQLite service integration | [SQLite service](#sqlite-service) |
| PostgreSQL repository, redundant gateways, migration, and database faults | [PostgreSQL repository and HA suites](#postgresql-repository-and-ha-suites) |
| Retained JSON v2 runtime | [Legacy JSON v2 suite](#legacy-json-v2-suite) |
| Stopped JSON-to-SQLite cutover, backup, restore, and process recovery | [Offline recovery and cutover](#offline-recovery-and-cutover) |
| Workload and SQLite catalog smoke fixtures | [Workload and catalog qualification](#workload-and-catalog-qualification) |
| Recorded results and limitations | [Evidence index](evidence.md) |

## Pinned environment

The target is **VictoriaMetrics OSS v1.155.0-cluster**, commit
`cab3d5fbeeedd5d0374c1dabde93939af4c2499c`. No VM patches are applied.
`build-vm.sh` verifies the SHA-256 of a pinned source archive and builds
vmstorage, vminsert, and vmselect using upstream vendored dependencies.
The pinned toolchains are Go 1.27.4 and Python 3.15.2. Bootstrap verifies the
committed Go modules and regenerates vmtier's vendor tree independently of the
stock VM vendor tree. Historical result files retain their original toolchains
and VM revisions; they do not establish validation of an upgraded dependency set.
The retained JSON v2 suite tests an independent persistent HTTP catalog authority and
a blob store using ordinary PUT/GET/Range requests. SQLite and PostgreSQL service
harnesses are described below. Evidence from the previous
all-S3 catalog architecture is kept under [legacy-v1](results/legacy-v1/README.md)
and is not included in current results.

## Setup and quick checks

Follow [getting started](getting-started.md#build-from-source) for prerequisites.
From the repository root:

```sh
./scripts/bootstrap.sh
make check build vet stock
python3 -m venv .venv-e2e
. .venv-e2e/bin/activate
python -m pip install -r scripts/requirements-e2e.txt
```

`make check` runs golangci-lint, `go mod tidy -diff`, actionlint and zizmor,
the Go race tests, and the Python regression tests below. `make test-fuzz` runs
every Go fuzz target for `FUZZTIME` (30 seconds by default); CI fuzzes for 15
seconds per target and for three minutes in its weekly run. `make bench` writes
benchmark results to `bench.txt` for benchstat.

Use a fresh work directory for every suite. The commands below assume this setup,
including the active virtual environment for S3 tests. For documentation-only
changes, check links, CLI flags, and implementation claims; no benchmarks are needed.
The Python regression tests use only the standard library and can run before
building binaries or installing Moto. They cover fixture timing, subprocess
provenance, fault assertions, measurements, and evidence packaging.

## SQLite service

After building vmtier and the pinned stock VM binaries and installing the Python
dependencies in the setup above, run:

```sh
python3 scripts/e2e_serve.py --backend local --workdir .cache/serve-local-fresh
MOTO_SERVER="$PWD/.venv-e2e/bin/moto_server" python3 scripts/e2e_serve.py \
  --backend s3 --replicas 2 --pack-max-blocks 4 --workdir .cache/serve-s3-fresh
```

`--pack-max-blocks 4` forces multiple packs per nonempty fixture window while
retaining the same coverage and exact query checks. Omit it to test default pack
limits; see [archive packing](archive-packs.md) for the pipeline bounds.

This harness starts one SQLite `serve` process. It exercises automatic archive
progress, a failed tenant alongside a healthy tenant, interrupted export and restart,
exclusive ownership, configuration drift rejection, Hot absence, 360 exact query
comparisons, metadata, cache behavior, corrupt objects, and offline maintenance.
It uses the same strict S3 proxy and disposable VM fixture as the JSON v2 suite.
The archive-age boundary is held between complete windows by a fixture-specific H;
there is no runtime clock override or scheduler pause mode.

The combined stock vmselect defaults to `-search.maxLabelsAPIDuration=30s` in
the harness. The earlier uncached S3 metadata scan exceeded the stock five-second
deadline. After the reader changes, [measured JSON and SQLite runs](metadata-validation.md)
passed with `--labels-api-duration 5s`, while still validating full payload blocks.
See [service validation](serve-validation.md) for results, preserved failure evidence,
provenance, and unexecuted checks. Loopback timings do not qualify provider performance.

## PostgreSQL repository and HA suites

Use a disposable PostgreSQL 17 database and set `VMTIER_TEST_POSTGRES_DSN` before
`make test race`, or run `make test-integration`, which runs only the PostgreSQL
packages and fails when the variable is unset; integration tests otherwise skip
explicitly. Tests isolate
themselves in temporary schemas. See [PostgreSQL setup](postgres-catalog.md#build-and-integration-tests).

With HAProxy, `psql`, Moto, and the pinned stock VM binaries installed:

```sh
export VMTIER_TEST_POSTGRES_DSN='postgres://vmtier:vmtier@127.0.0.1:5432/vmtier_test?sslmode=disable'
make build test race vet stock
python3 scripts/e2e_postgres.py --replicas 2 --pack-max-blocks 4 --workdir .cache/postgres-gateways-fresh
python3 scripts/e2e_postgres.py --replicas 2 --pack-max-blocks 4 --migrate-sqlite --workdir .cache/postgres-migration-fresh
python3 scripts/postgres_ha.py --postgres-bin /usr/lib/postgresql/17/bin \
  --race --workdir .cache/postgres-faults-fresh
```

The gateway harness exercises two candidates, archiver takeover, gateway/proxy
loss, static S3 budget allocation, and exact stock-VM comparisons. The commands
above force multiple packs per nonempty window. Migration mode
first archives into SQLite and verifies a stopped-writer metadata cutover without
object PUTs. The database fault harness initializes its own primary and synchronous
standby and exercises commit-response loss, stale writers, transport failures,
and explicitly fenced promotion. It runs as an unprivileged user and needs no
existing database server. All work directories must be fresh.

The [HA report](postgres-ha-validation.md) separates retained v1.128.0-cluster stage
evidence from v1.155.0-cluster integration runs. Reproduction commands use the
current checkout and pin; preserve new reports separately from earlier artifacts. These local
suites do not qualify production HA controllers, multi-AZ partitions, the outer
L4 endpoint, or actual S3 providers.

## Legacy JSON v2 suite

The current regression harness also sets `-search.maxLabelsAPIDuration=30s` on
the combined frontend. Its uncached S3 metadata reads share the full-block cost
described above, even without the SQLite service's request limiter. This setting
is recorded in new reports; the historical MVP reports retain their original
configuration. See [stage 2 validation](serve-validation.md) for the default-deadline
failure and subsequent regression results.

```sh
python3 scripts/e2e.py --backend local --workdir .cache/v2-local-fresh
MOTO_SERVER="$PWD/.venv-e2e/bin/moto_server" python3 scripts/e2e.py \
  --backend s3 --replicas 2 --workdir .cache/v2-s3-fresh
```

`make e2e` and `make e2e-s3` are shortcuts for these JSON v2 suites. They build
binaries first but do not install Python dependencies or run the service suites.

`GO_BIN` selects the Go binary. `VM_SOURCE_DIR` selects where the verified pinned
VM source is extracted; use a new directory or one already prepared by these
scripts. An arbitrary existing source tree is rejected. To validate only fixture ingestion into VM, use
`python3 scripts/e2e.py --vm-only`.

Each run creates an isolated directory and starts dedicated processes on available
loopback ports. Pass a **new or empty directory** to `--workdir PATH`; nonempty
directories are rejected. If omitted, a temporary directory is created. At exit
the harness stops only the processes it started and leaves
logs, configurations, `results.json`, and isolated VM data. It has no option to
connect to an external VM or a real S3 service. S3 tests use Moto 5.2.5 and public,
test-only credentials.

`scripts/s3_compat_proxy.py` sits in front of Moto. It rejects requests containing
`If-*` or `x-amz-copy-source-if-*` headers with 501 and strips every response ETag.
It validates Content-MD5 on PUTs. At completion the suite checks that there were no
conditional requests, that ETags were removed, that Range GETs occurred, and that
every PUT carried valid Content-MD5. This is not a test against Sakura Object Storage.

For both backends, the suite explicitly runs `catalog-init` in a directory separate
from blob storage, then starts an HTTP catalog service in another process.
Archive and gateway operations use that HTTP authority; S3 stores only packs and
indexes. Catalog generation publication uses the service's local persistent state,
without S3 conditional PUT, ETags, or LIST.

`results.json` records the executor hostname, SHA-256 of binaries, harness files,
and Go source tree, start/end UTC timestamps, and OS/Python/Moto versions.
Set `GO_BIN` or put Go on `PATH` to record its version.

## Legacy JSON v2 end-to-end checks

- Load the same fixture into a reference VM retaining the full period and into
  Hot before archive. Standard Python generates remote-write protobuf and literal
  Snappy while preserving the stale NaN bit pattern.
- Use tenants `1:2` and `7:9`, each with 16 series, and 17,654 input samples in
  total. Generate five hours at a 30-second interval, including irregular gaps,
  counter resets, label churn, empty/missing labels, duplicate/conflicting values
  at the same timestamp, staleness, NaN, ±Inf, and archive-boundary ±1 ms. Values
  differ between tenants.
- Archive the oldest three hours with one-hour windows. Choose H at fixture
  creation so the next eligible window remains 30 minutes away, regardless of
  the UTC-hour phase; record the exact H and eligibility deadline.
  The test contract uses Hot retention=240h, lateness=30m, and safety margin=30m.
  Fixture ingestion is the only write activity during the experiment.
- Send SIGKILL to the archiver during native export and confirm the explicitly
  initialized catalog remains at generation 0. Retry against the same source URL
  and confirm success. Replaying complete windows and automatic selection must
  leave the authority's catalog bytes unchanged.
- Reject changes to the committed coverage origin, archive age, and allowed
  lateness without changing authority state. Confirm no legacy `catalog.json`
  exists in blob storage.
- Query Cold using a stock vmselect connected only to the gateway. Exercise both
  `query` and `query_range`, and confirm the object Range GET counter increases.
- Stop Hot, restart it with a **new data directory**, and ingest recent samples
  only. Confirm raw export of all series over the old period is empty for both
  tenants. This proves Hot absence by reconstructing the fixture; it does not
  wait for actual retention expiry.
- Run 30 expressions × 2 tenants × 3 periods (Cold, Hot, and the boundary) × 2 APIs
  (instant and range), for 360 comparisons. Normalize order and require **exact
  equality** of timestamps, labels, and VM's numeric strings, without floating-point
  tolerance. Cover rollups, aggregation, vector matching, offsets, subqueries,
  `@`, and regexp/negative matchers.
- Compare label names, label values, and series discovery with the reference VM,
  including Cold data.
- Confirm healthy queries before fault injection. Missing, truncated, and
  checksum-invalid objects must make both query and `verify` fail. Restoring the
  original bytes must restore successful queries.
- Warm a 32 MiB cache, stop the catalog authority, and require queries and `/ready`
  to fail. Restarting the authority from the same persistent state must restore
  the exact manifest and query results. Removing state must make catalog startup
  fail without silently creating an empty state.
- Require zero conditional-request rejections, at least one removed ETag, and
  valid Content-MD5 on every PUT in the S3 proxy counters. Validate each MD5 against
  its request body.
- Call delete through the Cold-only vmselect and require an explicit unsupported
  error.
- In both suites, establish exact baseline results with the Cold cache disabled,
  then interrupt only blob access while the gateway/service stays running. The
  local fixture moves object contents out of the open storage root; the S3 proxy
  rejects every request with a fixture-specific HTTP 503 while retaining Moto's
  data for restoration. Require a frontend HTTP error containing that blob error,
  and increments in Cold query, query-error, and object-GET counters. S3 also
  records rejected GETs at the proxy. Connection refusals and unrelated timeouts
  cannot satisfy this check. `/health` and `/ready` must remain successful because
  readiness checks catalog/coverage, not blob availability. Restore blob access
  and require exact reference results and unchanged published catalog/coverage.
  New reports retain these observations under `blob_outage`.

All vmselect instances use `-search.disableCache`,
`-search.denyPartialResponse=true`, `-replicationFactor=1`,
`-globalReplicationFactor=1`, and `-search.skipSlowReplicas=false`.
Set `-dedup.minScrapeInterval=1ms` consistently on reference/Hot storage and all
vmselect instances. With `--replicas 2`, reference and Hot each use two vmstorage
instances; only vminsert's replication factor becomes 2. Top-level reads must not
skip Hot or Cold as if they were interchangeable replicas.

## Interpreting measurements

`results.json` records archive wall time, CPU, child-process peak RSS, sample count,
pack/index bytes, external authority manifest bytes, range-query latency
distributions during the 360 comparisons, and Range GET/download counters for the
cache test. RSS is Linux `getrusage(RUSAGE_CHILDREN).ru_maxrss`: an upper bound that
also includes the peaks of children such as provenance probes completed before
archive. Python/Moto and long-running VM/catalog CPU and RSS are not attributed to
archive.

The expressions in each time period include more than raw selectors. An expression
evaluated in the Hot period can still need Cold through offsets or lookback.
Do not interpret the `hot` distribution as strictly Hot-only overhead. These are
small measurements against Moto on the same machine, not a general S3/network
performance test.

Only the separate cache test restarts the gateway with a 32 MiB cache and runs
`cold_only[30m]` twice. VM result cache remains disabled. The second query must
perform zero Range GETs and return the same values. Read amplification is defined
as **blob download bytes, including indexes / (returned raw samples × 16 bytes)**.
This compares transfer volume with logical timestamp/float64 size, excludes catalog
HTTP traffic, and is not a ratio to compressed pack size.

Real Sakura/AWS S3/MinIO IAM, TLS, consistency, sustained load, high cardinality,
catalog updates with stock VM result cache enabled, actual retention expiry,
external ingress rejection of late writes, resharding, multiple-writer operation,
and compatibility with every VM version are outside this suite's scope.

Separate Go regression tests reject H/L contract changes at the authority,
archiver, and gateway. The archiver rejects them before new PUTs or exports; the
gateway fails queries and readiness even with a warm cache. Coverage-gap tests use
fixtures whose initial contract already has a shorter H, rather than substituting
a later configuration mismatch for an actual missing-coverage test.

Both fixtures use a shared archive-age calculation that places the next eligible
window exactly 30 minutes after fixture creation. A run may begin at any point in
the UTC hour; crossing the next UTC hour does not itself change required coverage.
The report records `created_at_ms`, `archive_age_ms`, `next_eligible_at_ms`, and
`finished_at_ms`. Each suite must finish its assertions within that 30-minute
horizon and reports failure if it does not. Actual production eligibility and
coverage checks remain active. The [historical hour-boundary failure](serve-validation.md)
is preserved separately.


## Runtime provenance

Both suites select workload-child `GOMAXPROCS` in this order: `--gomaxprocs N`,
the outer `GOMAXPROCS` environment variable, then `2`. The selected value must be
a positive integer and applies equally to stock VM, gateway/serve, catalog,
interrupted commands, and synchronous archive/maintenance CLI commands. For example:

```sh
GOMAXPROCS=7 python3 scripts/e2e.py --gomaxprocs 3 --backend local --workdir .cache/runtime-local
```

Here the wrapper has `GOMAXPROCS=7`, while every workload child receives `3`.
`results.json` records the outer value, selection source, chosen value, and each
managed or one-shot process's command, allowlisted runtime environment, and exit
status when available. Records are appended before launch and retained on expected
or unexpected failures. Credentials and unrelated environment variables are not
recorded. `--vm-only` writes the same process provenance to `runtime.json`.
The recorded value is the environment actually passed to the process, not a
measurement of Go's scheduler or a claim about peak process memory. Executable
version probes are outside the measured workload.

Before this correction, managed children always received `GOMAXPROCS=2` while
one-shot commands inherited the wrapper environment. Historical result JSON remains
unchanged; the [stage 2 narrative](serve-validation.md) distinguishes those settings.
Run the focused harness regressions with:

```sh
python3 -m unittest discover -s scripts -p 'test_e2e_harness.py' -v
```

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
python3 scripts/qualify_workload.py --tenants 2 --series 1000 \
  --history-hours 4 --dense-hours 2 --replicas 2 \
  --workdir .cache/workload-smoke-fresh
python3 scripts/catalog_capacity.py --hours 24 --tenants 2 --repeats 1 \
  --output .cache/catalog-smoke-fresh/results.json \
  --workdir .cache/catalog-smoke-fresh/work
```

These exercise the qualification harnesses; their sizes do not establish the
full measured envelope. Use new work/output paths to preserve earlier evidence.
`catalog_capacity.py` builds its own dedicated test binary; select a Go executable
with `--go /path/to/go` (or `GO`), not `GO_BIN`. The expensive Go catalog fixture
skips ordinary `go test` unless invoked with its qualification environment.

The [workload report](workload-validation.md) gives full commands and the recorded
envelope of 5,000 series per tenant. The harness default of 10,000 is the original
target whose broad query exceeded the unchanged query allowance. Two dense hours
plus one 24-hour sentinel series per tenant do not qualify 24 hours of dense data
for the whole cohort. Use the report's explicit workload and pressure flags.

The [catalog capacity report](catalog-capacity-validation.md) separately covers
87,600 logical windows through the real SQLite API, with no blob I/O. Its
publication rate is not archive throughput or PostgreSQL capacity evidence.
Both reports retain their original revisions and dependency pins.

## CI evidence

The workflow always packages available `.cache/ci-*` work directories into
`e2e-evidence.tar.gz`, then uploads that file as the `e2e-evidence` artifact.
After downloading and unzipping the artifact, run `tar -xzf e2e-evidence.tar.gz`.
The archive preserves original tenant paths (including colons), hidden files,
logs, and result bytes. Failed suites may leave partial directories; an empty
archive means setup failed before any work directory was created. Packaging and
upload also run after test failure and do not turn a failed test into success.

The initial [PR run](https://github.com/yuuki/vmtier/actions/runs/42209065311)
and [push run](https://github.com/yuuki/vmtier/actions/runs/42209044949) passed
their build, test, race, vet, and four E2E steps, but failed artifact upload because
the action rejected colons in nested fixture paths. Those runs were not successful
CI runs. Historical local result files remain unchanged.


See the [workflow](../.github/workflows/ci.yml) for the current suite matrix and
[evidence index](evidence.md) for recorded results.

## Release and supply-chain checks

The `test` workflow's `release-check` job builds two release snapshots of the
pushed commit in the pinned builder. The first also runs `go test` with release
tags and static musl linking. Each build checks the linked modules, their license
digests, and the embedded C library versions against `release/licenses.json`. The
job requires identical file hashes and `SHA256SUMS`, then runs the archive smoke
test. That test checks the exact asset set, static linking, the presence of every
recorded notice, `vmtier version`, and SQLite catalog initialization in the static
binary. The job uploads the snapshot as `release-snapshot`. The `security`
workflow runs `make vulncheck` (govulncheck) and CodeQL for Go and Python on
every push and weekly. Its `workflows` job runs `make lint-actions`: actionlint
and zizmor over `.github/`, including zizmor's online audits. The `test`
workflow's `lint` job runs golangci-lint and `go mod tidy -diff`, and its `fuzz`
job runs `make test-fuzz`.
`python3 -m unittest discover -s scripts -p 'test_*.py'` covers the packaging
logic without Docker. A tag runs the full [release gates](releasing.md#maintainer-procedure).
This includes the suites above against the packaged binary; their evidence is
uploaded as `release-e2e-evidence`.
