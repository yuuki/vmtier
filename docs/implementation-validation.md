# Archive and reader implementation validation

Four fresh local integration runs passed on **2026-11-06**, with **1,440 exact
stock-vmselect instant/range query comparisons**, plus exact label names, label
values, and series-discovery comparisons. They exercise the retained JSON v2
runtime and the SQLite service after the archive, reader, and harness changes.
The evidence establishes correctness on these small fixtures; production
workload, provider, and combined-memory qualification remain separate work.

## Retained runs

The linked result files are byte-for-byte copies of the completed runs' original
`results.json` files. Historical reports, including failed stage-2 runs, remain
unchanged. Each new report retains timestamps, source/binary/harness hashes,
per-process commands and effective `GOMAXPROCS`, query measurements, and fault
evidence.

| Runtime and backend | Replicas | Packing | Exact instant/range comparisons | Result |
| --- | --- | --- | --- | --- |
| [JSON v2, local](results/implementation/json-local.json) | 1 | Retained single-pack writer | 360 | Passed |
| [JSON v2, Moto S3](results/implementation/json-s3.json) | 2 | Retained single-pack writer | 360 | Passed |
| [SQLite, local](results/implementation/sqlite-local.json) | 1 | Forced four-block limit: 33 packs across six stream windows | 360 | Passed |
| [SQLite, Moto S3](results/implementation/sqlite-s3.json) | 2 | Default 100,000-block limit: six packs across six stream windows | 360 | Passed |

All four runs explicitly selected the stock frontend's **five-second labels API
deadline**, and all recorded metadata comparisons passed. The largest observed
uncached metadata latency was about 314 ms in the SQLite S3 run. This is a result
for the retained fixture, not an arbitrary-workload latency promise. The harness
still offers its documented 30-second setting. See the separate
[metadata measurements](metadata-validation.md) for the comparison and tuning
scope.

Each fixture has two tenants, 16 series per tenant, 30-second samples, and
one-hour windows. Irregular gaps, counter resets, stale markers, infinities,
duplicate/conflicting timestamps, millisecond boundaries, and empty/absent labels
remain in the comparison set. VM result caching is disabled. The separate warm
immutable-cache checks use a 32 MiB allowance and add zero payload Range GETs;
warm metadata checks also record zero additional downloaded bytes.

The forced SQLite local run proves multiple packs retain exact results after Hot
is recreated with only recent samples. The replicated S3 run uses normal packing;
these four runs do not separately establish forced multi-pack behavior on a real
provider. Recreated Hot demonstrates absence of archived samples, not natural VM
retention expiry in an unchanged storage directory.

## Environment and provenance

Runs completed between 11:48 and 11:51 UTC in one Linux amd64 development
container: Debian 13, kernel 6.18.57, five available logical CPUs, Go 1.27.3,
Python 3.12.14, and Moto 5.1.15 for the S3 runs. The reported container memory
total was about 17.59 GiB; that is environment capacity, not measured workload RSS.

The outer `GOMAXPROCS` was unset. Each invocation explicitly chose
`--gomaxprocs 3`, which the reports record for both managed workload processes and
one-shot CLI commands. This differs from the historical stage-2 wrapper/child
setting ambiguity corrected in [service validation](serve-validation.md).

Compatibility remains pinned to unmodified VictoriaMetrics OSS
`v1.128.0-cluster`, revision
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. All four reports identify the same
vmtier binary and Go source snapshot:

```text
vmtier binary SHA-256:
63396250a4c99cd0325b5d6b266c3c87e86a92565f36c2148e1f81f1a9f772b4
Go source tree SHA-256:
e2a8a391dbd6e321a8653a346c51b2196d8d0f9ac7c55ec5605489f55b21d069
```

The source hash covers sorted `cmd/**/*.go`, `internal/**/*.go`, `go.mod`, and
`go.sum`, with path/NUL/content/NUL framing. Binary, stock-VM binary, and individual
harness hashes are preserved in each report's `provenance` object. These identify
the tested snapshot independently of later documentation commits.

## Behavioral coverage and remaining limits

The new evidence combines the following focused checks with the four integration
suites:

- The harness packages paths containing tenant colons into a portable evidence
  archive, records actual child runtime settings, and keeps a stable fixture
  horizon across UTC hour boundaries. Failure-path and missing-directory handling
  are covered by the Python harness checks. Hosted upload/download confirmation
  remains distinct from these local checks.
- The blob-outage checks establish a successful baseline against a running Cold
  backend, then require query and object-read counter increments with a specific
  backend error. All four runs observed HTTP 422 from stock vmselect, retained
  successful health/coverage readiness, and recovered exact results without
  changing coverage. The S3 proxy recorded three failed GET attempts per outage;
  frontend connection refusal or an unrelated timeout cannot satisfy the check.
- [Pack rotation](archive-packs.md) preserves complete records and one logical
  window while enforcing per-record, pack, physical-sample, input, and attempt
  limits. Focused tests cover cross-pack conflicting values and stale priority,
  oracle failure after staging, truncation, failed upload/restart, and atomic
  publication. [Preflight](preflight.md) reports a bounded source observation
  without opening or changing catalog state or publishing objects.
- [Incremental raw verification](raw-oracle-validation.md) retains the independent
  completeness oracle and full-envelope/EOF validation. Its fixed benchmark
  allocated about 77% fewer bytes per operation. This is allocation evidence,
  not an archive-process peak-RSS measurement.
- [Reader checks](reader-validation.md) cover LRU pressure, coalesced concurrent
  misses, cancellation and abandoned-read cleanup, adjacent-range bounds,
  individual block verification, exact metadata semantics, and authoritative
  catalog failure despite a warm cache.
- [Query admission](serve.md#query-work-admission) now caps logical reservations
  across sample and metadata work, with release on errors, deadlines, service
  cancellation, and iterator close. Capacity and failure metrics distinguish
  cache/catalog accounting and committed object bytes from actual bucket usage.
  Query-budget implementation is only part of issue #11: concurrent archival,
  queries, cache pressure, metadata serialization, and peak RSS still require
  measurement before choosing qualified production defaults.

The two S3 runs use a local compatibility proxy that rejects conditional requests,
strips ETags, and verifies Content-MD5 on PUT. Both report zero attempted
conditional requests, zero bad MD5 values, and MD5 on all 17 observed PUTs. This
qualifies the exercised request subset against Moto, not Sakura, AWS, or MinIO.
No real-provider credentials or disposable provider bucket were configured.

Issues #14–#19 remain qualification/delivery work: the ten-tenant/10,000-series
workload and 87,600-window catalog target, real Sakura behavior, natural retention
transition and outage headroom, externally enforced ingress lateness, complete
JSON-v2 cutover and older-backup catch-up, and reproducible release artifacts with
redistribution notices. Existing restart and offline-maintenance checks do not
complete that recovery qualification. These runs add no production ingress
enforcement, deployment, provider-support claim, or published release.

## Reproduction

With the pinned dependencies and stock binaries built as described in
[testing](testing.md), the repository build, unit tests, race tests, and vet passed
with `GOMAXPROCS=3`. All 11 focused Python harness tests also passed.

```sh
make build test race vet
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/e2e.py --backend local --gomaxprocs 3 \
  --labels-api-duration 5s --workdir .cache/reproduce-json-local
MOTO_SERVER=/path/to/moto_server python3 scripts/e2e.py \
  --backend s3 --replicas 2 --gomaxprocs 3 --labels-api-duration 5s \
  --workdir .cache/reproduce-json-s3
python3 scripts/e2e_serve.py --backend local --pack-max-blocks 4 \
  --gomaxprocs 3 --labels-api-duration 5s --workdir .cache/reproduce-sqlite-local
MOTO_SERVER=/path/to/moto_server python3 scripts/e2e_serve.py \
  --backend s3 --replicas 2 --gomaxprocs 3 --labels-api-duration 5s \
  --workdir .cache/reproduce-sqlite-s3
```

Use new work directories; the harness refuses to overwrite prior evidence. Some
recorded runs overlapped briefly in this container, but they did not constitute a
controlled simultaneous heavy workload or a combined RSS capacity experiment.
The measured envelope remains the small correctness fixture described above.
