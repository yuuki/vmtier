# Issue foundations validation

The harness and CI changes for issues #2–#5 passed **11 focused Python tests**
and four local stock-VM suites with **1,440 exact query comparisons**, plus metadata
and failure checks, on 2026-11-06. These changes do not modify Go runtime code or
storage formats. The tested harness includes the HTTP 422 correction in
[`4a33e5b85ad04fa69f80d2c1c74debb6b8c9e155`](https://github.com/yuuki/vmtier/commit/4a33e5b85ad04fa69f80d2c1c74debb6b8c9e155).

The following reports were copied byte-for-byte from their original work
directories; timestamps are UTC. Each report records 360 exact comparisons.

| Runtime/backend | Run interval | Report | Outage HTTP / blob GET attempts |
| --- | --- | --- | --- |
| JSON v2 / local | 11:45:28–11:45:36 | [JSON local](results/issue-foundations/json-local.json) | 422 / 1 |
| JSON v2 / Moto, two replicas | 11:46:52–11:49:27 | [JSON S3](results/issue-foundations/json-s3.json) | 422 / 3 |
| SQLite / local | 11:47:00–11:47:08 | [SQLite local](results/issue-foundations/sqlite-local.json) | 422 / 1 |
| SQLite / Moto, two replicas | 11:48:24–11:51:17 | [SQLite S3](results/issue-foundations/sqlite-s3.json) | 422 / 3 |

All four runs used pinned stock VM `v1.128.0-cluster` at revision
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`, Go 1.27.3, Python 3.12.14, and
Debian 13/Linux amd64 on executor `36765b6c3f29`; S3 used Moto 5.1.15. The Go
source-tree hash was `e43295863e6fcc702bebe2a3b67702799cb9c515f31e6b8a5a4a757c74a99cdf`
and vmtier binary hash was `44bf5973c9e3eb4e2083b4e2ccaf05c5de5c2c3e8ba14869a67decc7addd64a7`.
Runs overlapped in the same container; their timings are not capacity measurements.

`--gomaxprocs 3` selected `GOMAXPROCS=3` for every recorded managed and one-shot
workload child, with the outer variable unset. Reports retain commands, effective
environment, process type and exit status. The focused subprocess regression reads
the child's environment directly and verifies overrides and failed-command records.
The [historical stage 2 narrative](serve-validation.md) is corrected separately;
its original raw JSON remains unchanged.

The hour-boundary regression covers fixture creation at the start of an hour,
one millisecond into it, halfway through, one minute before its end, and one
millisecond before its end. Required coverage remains fixed for 30 minutes, then
advances at the recorded deadline. All four real suites finished inside their
recorded horizon. This uses no production clock override or relaxed coverage check.

Each blob-outage check first obtains exact reference results with the Cold cache
disabled and keeps the same gateway/service running. Local object contents are
moved out of the open root; the S3 proxy rejects all blob access with a distinct
503 error. Stock vmselect reports the resulting query error as **HTTP 422**.
The assertions require that error's marker and increases in Cold query, error,
and blob-GET counters; both S3 runs also observed exactly three rejected GETs.
Health and catalog/coverage readiness remain successful. Restoring blob access
restores exact results without changing publication or coverage. Connection
refusals, unrelated timeouts, and errors without a blob attempt cannot pass.

The initial [JSON local failure](results/issue-foundations/json-local-failed-422.json)
and [JSON S3 failure](results/issue-foundations/json-s3-failed-422.json) are preserved
unchanged. Both passed their 360 comparisons before the new harness incorrectly
required a 5xx frontend response. The correction accepts 422 or 5xx only alongside
the existing error-marker/counter proof; a focused 422 regression was added.
`python3 -m unittest discover -s scripts -p 'test_*.py'` now passes all 11 tests.

Hosted [run 42212598216](https://github.com/yuuki/vmtier/actions/runs/42212598216)
for the same `4a33e5b` code completed successfully at 11:53:03 UTC. The subsequent
documentation-only `f23d197` [run 42213377632](https://github.com/yuuki/vmtier/actions/runs/42213377632)
was still in progress when checked for this report; no success is claimed for it.
The earlier successful upload and downloaded-artifact round trip are recorded
separately in [CI evidence validation](ci-evidence-validation.md).

These are loopback correctness and evidence checks. They do not qualify real
Sakura/AWS/MinIO, natural Hot retention expiry, production lateness enforcement,
high-cardinality capacity, or later archive/reader implementation changes.
