# Historical v1 validation procedure

This document records the procedure for the former all-S3 catalog architecture.
Its prose has been translated into English. The current checkout provides SQLite
`serve` and retained JSON v2 commands, so the commands below do not reproduce the
v1 results with current sources. See the
[current testing guide](../../testing.md) for the supported procedure.

The target was **VictoriaMetrics OSS v1.128.0-cluster**, commit
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. No VM patches were applied.
`build-vm.sh` verifies the SHA-256 of the pinned source archive and builds
vmstorage, vminsert, and vmselect using upstream vendored dependencies.
The procedure used Go 1.25.3 or newer and Python 3.10 or newer.

```sh
./scripts/build-vm.sh
# Build vmtier using the bootstrap/build instructions in the README.
python3 -m venv .venv-e2e
.venv-e2e/bin/pip install -r scripts/requirements-e2e.txt
python3 scripts/e2e.py --backend local
MOTO_SERVER="$PWD/.venv-e2e/bin/moto_server" python3 scripts/e2e.py --backend s3
MOTO_SERVER="$PWD/.venv-e2e/bin/moto_server" python3 scripts/e2e.py --backend s3 --replicas 2
```

`GO_BIN` selects the Go binary, and `VM_SOURCE_DIR` selects an existing unmodified
VM source tree. `python3 scripts/e2e.py --vm-only` validates only fixture ingestion
into VM.

Each run creates an isolated directory and starts dedicated processes on available
loopback ports. Pass an **unused directory** to `--workdir PATH`; the suite does not
reuse existing data. At exit it stops only the processes it started and leaves
logs, configuration, `results.json`, and isolated VM data. There is no option to
connect to an external VM or a real S3 service. S3 tests use Moto 5.1.15 and public,
test-only credentials.

## End-to-end checks

- Load the same fixture into a reference VM retaining the entire period and Hot
  before archive. Standard Python generates remote-write protobuf and literal
  Snappy while preserving the stale NaN bit pattern.
- Use tenants `1:2` and `7:9`, each with 16 series, and 17,654 input samples in
  total. Generate five hours at a 30-second interval, including irregular gaps,
  counter resets, label churn, empty/missing labels, duplicate/conflicting values
  at one timestamp, staleness, NaN, ±Inf, and archive-boundary ±1 ms. Values differ
  between tenants.
- Archive the oldest three hours with sample age H=3h and one-hour windows. The
  test contract uses Hot retention=240h, lateness=30m, and safety margin=30m.
  Fixture ingestion is the only write activity during the experiment.
- Send SIGKILL to the archiver during native export and confirm the catalog
  remains unpublished. Retry against the same source URL and confirm success.
  Replaying complete windows and automatic selection must leave catalog bytes
  unchanged.
- Query Cold using stock vmselect connected only to the gateway. Exercise both
  `query` and `query_range`, and confirm the object Range GET counter increases.
- Stop Hot, restart it with a **new data directory**, and ingest recent samples
  only. Confirm raw export of all series over the old period is empty for both
  tenants. This proves Hot absence by reconstructing the fixture; it does not
  wait for actual retention expiry.
- Run 30 expressions × 2 tenants × 3 periods (Cold, Hot, and the boundary) × 2 APIs
  (instant and range), for 360 comparisons. Normalize order and require **exact
  equality** of timestamps, labels, and VM's numeric strings, without
  floating-point tolerance. Cover rollups, aggregation, vector matching, offsets,
  subqueries, `@`, and regexp/negative matchers.
- Compare label names, label values, and series discovery with the reference VM,
  including Cold data.
- Confirm healthy queries before fault injection. Missing, truncated, and
  checksum-invalid objects must make both query and `verify` fail. Restoring
  original bytes must restore successful queries.
- Call delete through the Cold-only vmselect and require an explicit unsupported
  error. Stopping the backend must make integrated vmselect return an HTTP error.

All vmselect instances use `-search.disableCache`,
`-search.denyPartialResponse=true`, `-replicationFactor=1`,
`-globalReplicationFactor=1`, and `-search.skipSlowReplicas=false`.
Set `-dedup.minScrapeInterval=1ms` consistently on reference/Hot storage and all
vmselect instances. With `--replicas 2`, reference and Hot each use two vmstorage
instances; only vminsert's replication factor becomes 2. Top-level reads must not
skip Hot or Cold as if they were interchangeable replicas.

## Interpreting measurements

`results.json` records archive wall time, CPU, child-process peak RSS, sample count,
pack/index/catalog bytes, range-query latency distributions during the 360
comparisons, and Range GET/download counters for the cache test.
RSS is Linux `getrusage(RUSAGE_CHILDREN).ru_maxrss`, an upper bound that includes
peaks from previously completed child processes. Python/Moto and long-running
VM CPU and RSS are not attributed to archive.

The expressions in each period include more than raw selectors. An expression
evaluated in the Hot period can need Cold through offsets or lookback. Do not
interpret the `hot` distribution as strictly Hot-only overhead. These are small
measurements against Moto on one machine, not a general S3/network performance
test.

Only the separate cache test restarts the gateway with a 32 MiB cache and runs
`cold_only[30m]` twice. VM result cache remains disabled. The second query must
perform zero Range GETs and return the same values. Read amplification is defined
as **download bytes, including catalog and indexes / (returned raw samples ×
16 bytes)**. This compares transfer volume with logical timestamp/float64 size,
not compressed pack size.

Real AWS S3/MinIO IAM, TLS, consistency, sustained load, high cardinality, catalog
updates with stock VM result cache enabled, actual retention expiry, external
ingress rejection of late writes, resharding, multiple-writer operation, and
compatibility with every VM version are outside this historical suite's scope.
