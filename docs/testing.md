# Reproducing MVP validation

The target is **VictoriaMetrics OSS v1.128.0-cluster**, commit
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. No VM patches are applied.
`build-vm.sh` verifies the SHA-256 of a pinned source archive and builds
vmstorage, vminsert, and vmselect using upstream vendored dependencies.
Use Go 1.25.3 or newer and Python 3.10 or newer.
The current v2 suite tests an independent persistent HTTP catalog authority and
a blob store using ordinary PUT/GET/Range requests. Evidence from the previous
all-S3 catalog architecture is kept under [legacy-v1](results/legacy-v1/README.md)
and is not included in current results.

```sh
./scripts/build-vm.sh
# Build vmtier using the bootstrap/build instructions in the README.
python3 -m venv .venv-e2e
.venv-e2e/bin/pip install -r scripts/requirements-e2e.txt
python3 scripts/e2e.py --backend local --workdir .cache/v2-local-fresh
MOTO_SERVER="$PWD/.venv-e2e/bin/moto_server" python3 scripts/e2e.py \
  --backend s3 --replicas 2 --workdir .cache/v2-s3-fresh
```

`GO_BIN` selects the Go binary, and `VM_SOURCE_DIR` selects an existing unmodified
VM source tree. To validate only fixture ingestion into VM, use
`python3 scripts/e2e.py --vm-only`.

Each run creates an isolated directory and starts dedicated processes on available
loopback ports. Pass an **unused directory** to `--workdir PATH`; the suite does not
reuse existing data. At exit it stops only the processes it started and leaves
logs, configurations, `results.json`, and isolated VM data. It has no option to
connect to an external VM or a real S3 service. S3 tests use Moto 5.1.15 and public,
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

## End-to-end checks

- Load the same fixture into a reference VM retaining the full period and into
  Hot before archive. Standard Python generates remote-write protobuf and literal
  Snappy while preserving the stale NaN bit pattern.
- Use tenants `1:2` and `7:9`, each with 16 series, and 17,654 input samples in
  total. Generate five hours at a 30-second interval, including irregular gaps,
  counter resets, label churn, empty/missing labels, duplicate/conflicting values
  at the same timestamp, staleness, NaN, ±Inf, and archive-boundary ±1 ms. Values
  differ between tenants.
- Archive the oldest three hours with sample age H=3h and one-hour windows.
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
  error. Stopping the blob backend must make the integrated vmselect return an
  HTTP error.

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

The automatic archive no-op check assumes a normal short run that finishes in the
same UTC hour as fixture generation. After an interruption crossing the next UTC
hour, another window becomes age-eligible; retry the suite with a new workdir.
