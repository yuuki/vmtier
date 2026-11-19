# VM v1.156.0 upgrade evidence (2026-11-19 UTC)

This directory supports the
[upgrade from v1.155.0-cluster](../../compatibility.md#upgrading-from-v11550-cluster).
All runs used Linux amd64, Go 1.27.4, Python 3.13.18, Moto 5.2.5, PostgreSQL
16.15, HAProxy 2.8.30, and GOMAXPROCS=2 for workload children. Local paths in the
JSON files are replaced by `<repo>`, `<work>` and `<vm-v1.156.0>`.

## Stock v1.156.0 with the previous pin

vmtier commit `034d185` (v1.155.0 pin) ran against the official v1.156.0
linux-amd64 cluster binaries. `downloads.json` records the archive URL and the
upstream checksums, which were verified before use. The drivers are those of the
[earlier investigation](../vm-compatibility-2026-11-10/README.md).

| File | Driver and options | Result |
| --- | --- | --- |
| `pin155-stock156-json.json` | `run_version.py --collision` | Passed, 360 exact comparisons |
| `pin155-stock156-sqlite.json` | `run_version.py --suite sqlite --collision` | Passed, 360 exact comparisons, at most 4 blocks per pack |
| `pin155-stock156-replica.json` | `probe_collision.py` | Archive succeeded; Cold raw result equals the source |

## The v1.156.0 pin

`pin156-e2e-summary.json` summarizes every CI E2E suite on commit `6c3fdbd`
with stock VM built by `make stock` from the pinned source. All seven suites
passed: six with 360 exact comparisons each, and recovery with 480. The working
tree also held an uncommitted documentation change, so the binaries report
`+dirty`; no Go source or harness differed from the commit. `make check` and
`make test-integration` (PostgreSQL) also passed on that commit; their output is
not stored.

## Native format and matchers

`stock_checks.py` starts a one-node stock cluster, imports one series, requests
its native export, and evaluates the regular expressions of
`TestMatcherAssertionsFollowStockVM`. `stock-v1.156.0.json` is the run that
produced `internal/pack/testdata/native-v1.156.0.hex`. `stock-v1.155.0.json`
repeats it with stock v1.155.0. Both versions return the same matcher results,
which equal the test's expectations.

Repeating the v1.155.0 fixture import on stock v1.156.0 returns a body
byte-identical to `internal/pack/testdata/native-v1.155.0.hex`:

```sh
python3 stock_checks.py --vm-bin /path/to/v1.156.0 --workdir /path/to/new \
  --metric fixture_155 --values '[2,3.5]' --day-start 1793923200
```

## Query cancellation

`probe_cancel.py` puts a TCP proxy between a stock vmselect and a vmtier gateway.
The proxy delays the gateway's response by 1.5 seconds, and the HTTP client
disconnects after 0.5 seconds, so vmselect cancels the query while it still reads
MetricBlocks. The probe then runs the next query after 1 second and after
7 seconds; the second wait exceeds the gateway's 5-second deadline for reading
request arguments.

| File | Gateway | vmselect | Markers sent | New connections | Gateway log |
| --- | --- | --- | --- | --- | --- |
| `cancel-pin155-vmselect155.json` | v1.155.0 pin | v1.155.0 | 0 | 2 | Empty |
| `cancel-pin155-vmselect156.json` | v1.155.0 pin | v1.156.0 | 2 | 2 | `unsupported rpcName: "cancelRequestMarker"`; read timeout |
| `cancel-pin156-vmselect155.json` | v1.156.0 pin | v1.155.0 | 0 | 2 | Empty |
| `cancel-pin156-vmselect156.json` | v1.156.0 pin | v1.156.0 | 2 | 0 | Empty |

Every query after a cancellation succeeded and returned the warm-up result. The
fixture response is small, so the gateway had finished sending before the marker
arrived. Cancellation of a Cold read that is still in progress was not exercised.

## Source acquisition

`codeload.github.com` was not reachable from the executor. The pinned source
archives were recreated from the tag commits with
`git -c tar.tar.gz.command="gzip -cn" archive --format=tar.gz --prefix=VictoriaMetrics-<commit>/ <commit>`.
Their SHA-256 values equal those that `scripts/vm-source.sh` pins for v1.155.0
and v1.156.0, and `make bootstrap` verified them.
