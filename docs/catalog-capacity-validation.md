# Historical SQLite catalog capacity

This qualification exercises the catalog portion of [issue #14](https://github.com/yuuki/vmtier/issues/14):
ten tenants, one source, and 8,760 hourly windows per tenant, for 87,600 logical
windows. It does not qualify the corresponding archived payload, series count,
sample rate, object storage, or combined service workload.

## Fixture and assertions

The opt-in [Go fixture](../internal/catalog/sqlite_qualification_test.go) creates a
fresh catalog through `CreateSQLite` and publishes every window through
`PublishWindow`. Each publication uses the normal validation, one SQLite WAL
transaction with `synchronous=FULL`, generation conflict check, and coverage
advance. It does not bulk-load SQL or relax durability settings. The catalog has
one connection, matching the service.

Each window contains one synthetic pack descriptor. Its 1 MiB data length and
4 KiB index length are chosen only to check exact committed-byte totals; **no
blob objects are written or read**. Descriptor hashes are syntactically valid
fixture values. Consequently, neither the referenced byte totals nor the
publication rate represent archive throughput or verified payload capacity.

At both 8,760 and 87,600 rows, selection checks cover all ten tenants and the
oldest, middle, and newest positions. One-hour and 24-hour intervals each have
300 calls per checkpoint. Every returned descriptor, generation, coverage end,
tenant, source, and window boundary is compared with the generated expectation.
Selection caps are 24 windows, 24 packs, and 32 KiB of logical plan accounting.
Separate requests must refuse each smaller window, pack, and byte cap with the
typed `catalog_plan` reason.

The diagnostic query plan checks the same SQL used by `Select`: the window query
must use the `(tenant, source_id, start_ms)` index with both time bounds, and pack
lookup must use its `(window_id, ordinal)` primary key without a temporary sort.
The production `Select` method performs all timed selections. Diagnostic SQL is
not substituted for the query path.

The fixture also times 20 `Capacity` observations at each checkpoint, then runs
100 sequential 24-hour selections concurrently with 20 sequential observations.
This intentionally exposes contention on the single connection. `Capacity`
aggregates committed pack sizes with a full table scan; its cost is not bounded
by a query's selected interval. Finally, closing and reopening the real catalog
runs `quick_check` and verifies exact 24-hour tail selections for every tenant.

## Reproduction

After the normal dependency/bootstrap setup:

```sh
GOCACHE="$PWD/.cache/go-build" python3 scripts/catalog_capacity.py \
  --go go --gomaxprocs 2 --hours 8760 --tenants 10 --repeats 10 \
  --workdir /tmp/vmtier-catalog-capacity-new \
  --output /tmp/vmtier-catalog-capacity-new.json
```

Use new output and state paths on every run. The runner builds a dedicated test
binary, then measures that process separately from compilation. The output
records its SHA-256, source/harness hash, Git state, toolchain, SQLite version,
hardware, cgroup limits, selected runtime, latency distributions, apparent
database/WAL/SHM lengths, process CPU, and Linux process peak RSS. The local
catalog and log remain in the work directory for inspection. A quick functional
run can use `--hours 48 --tenants 2 --repeats 1`; it is not the scale qualification.
The expensive fixture skips ordinary `go test` unless its explicit runner
environment is supplied.

## Recorded result

The [raw report](results/catalog-capacity/87600-windows.json) passed on
2026-11-06, from 14:03:11 to 14:05:04 UTC. It records 87,600 successful
publications, 1,200 exact sequential selection checks, 100 exact selections
under observation contention, three typed limit refusals, and ten exact checks
after reopening. The measured test binary SHA-256 is
`9c262aee6e2743c98bc450ef9ca6f5053fd3aac9c75197eff90663b190c89f81`.
The report retains the source/harness digest and the dirty checkout provenance;
it does not imply that the starting Git commit alone contains the new fixture.

| Catalog rows | 1-hour selection p50 / p95 / p99 | 24-hour selection p50 / p95 / p99 | Capacity observation p50 / p95 / p99 |
| --- | --- | --- | --- |
| 8,760 | 0.157 / 0.305 / 0.379 ms | 1.163 / 1.912 / 5.082 ms | 2.037 / 3.060 / 3.553 ms |
| 87,600 | 0.270 / 0.361 / 0.486 ms | 0.809 / 1.439 / 1.680 ms | 14.945 / 18.661 / 23.942 ms |

The 87,600-row catalog used apparent file lengths of 45,223,936 bytes for the
database, 4,223,032 bytes for the WAL, and 32,768 bytes for shared memory at the
final checkpoint. Referenced data/index totals were exactly 91,855,257,600 and
358,809,600 bytes, respectively; these are synthetic descriptor sums, not stored
object bytes. Process peak RSS was 30,277,632 bytes (28.875 MiB).

The publication calls took 110.65 seconds in total (791.7 descriptor
publications/second); their p50/p95/p99 were 1.117/2.128/4.027 ms. The full fixture
took 112.36 seconds, excluding compilation. Reopening, including `quick_check`,
took 61.22 ms. These figures measure local catalog work, with no archive or S3
operations.

With the two selection/observation workers contending, 24-hour selection
p50/p95/p99 were 1.180/16.879/18.330 ms. The 20 capacity scans were deliberately
back-to-back, much more frequent than the service's five-second observation
interval. Their table scan explains the observed connection contention and the
growth in observation cost; indexed selection alone must not be used to claim
constant-time metrics at arbitrary catalog size.

## Measurement scope

Results are specific to this local overlay filesystem and shared managed
container. The hardware is an AMD EPYC 7763 host with a four-CPU cgroup quota,
16 GiB cgroup memory limit, and `GOMAXPROCS=2` for the measured process. Go 1.27.3,
CGO, GCC 14.2.0, and SQLite 3.50.4 were used. No stock VM or provider process is
part of this fixture. Linux `ru_maxrss` belongs to the dedicated catalog test
process; it is not the memory usage of an archive/query deployment.

The queries use a newly populated catalog and the operating system's page cache;
this is not a disk-cache-cleared benchmark. Latency percentiles describe the
recorded calls, not an SLO. Container/runtime activity and host scheduling can
affect them. A small local recovery correctness fixture using stock VM and
`vmtier` overlapped approximately the final 17 seconds of this run. The indexed
plan and exact bounded results establish catalog
selection behavior at this row count; first-release support still requires the
separate payload, recovery, and combined-memory workload evidence.
