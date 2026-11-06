# Historical v1 validation results (2026-11-06 JST)

This document describes the previous all-S3 catalog architecture. Its results do
not validate the current v2 implementation. The prose is an English translation;
the original result JSON files are unchanged.

The environment was Debian 13 / Linux amd64, Go 1.27.3, Xeon Platinum 8573C, five
available CPUs, and 33 GiB RAM. VM binaries were built from unmodified OSS
v1.128.0-cluster source at commit `9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`.
The historical procedure is in [testing.md](testing.md), and raw measurements are
in [local.json](local.json) and [s3-replicated.json](s3-replicated.json).

## Passed checks

- `make bootstrap build test race vet` ran locally. Tests for all Go packages,
  the race detector, and vet passed. The external S3 integration test, skipped by
  default, also passed in a separate run with a Moto endpoint and `-race`.
- Native export → portable native pack → independent gateway → stock vmselect
  worked for `query` and `query_range`. Cold Range GET counters increased.
- The local backend with one replica and S3-compatible Moto 5.1.15 with two replicas
  each passed **360 exact comparisons, 720 in total**, after normalization, with
  zero floating-point tolerance.
- Fixtures covered two tenants, 16 series each, 17,654 input samples, five hours,
  and 30-second scrapes. Cases included counter resets, gauges, gaps, irregular
  scrapes, label churn, stale markers, NaN inputs, ±Inf, duplicate/conflicting values
  at one timestamp, boundary ±1 ms, regex/negative/empty-label matchers,
  aggregation, vector matching, offsets, subqueries, `@`, rate/increase, and rollups.
- Hot was recreated with recent data only, and raw export over the old period was
  empty for both tenants. Cold-only and boundary queries were compared in that
  state. A separate check confirmed Cold did not return recent samples.
- Cold label names/values, series discovery, tenant isolation, and explicit errors
  for unsupported delete were checked.
- Faults covered SIGKILL during native export followed by a successful retry,
  replaying the same windows, automatic age-selection no-op, missing sources, S3
  outage, and missing/truncated/checksum-invalid objects followed by recovery.
- Unit and fault tests covered failures before/after publication, PUT/GET failures,
  CAS conflicts and lost responses, coverage gaps, catalog generation changes
  during queries, tenant/codec mismatches, unsupported RPCs, rejected lateness
  contracts, expired retention, resource limits, oversized zstd decompression,
  and malformed varints.
- A regression test covered native streams ending at a record boundary while
  omitting a block with a higher value at an existing timestamp. Sample counts
  can still match in that case. An independent raw JSON matrix comparison of all
  timestamps and values rejected the input and prevented catalog publication.
- Native parser fuzzing passed for 15 seconds and 283,033 inputs. This short run
  does not establish exhaustive coverage. Later parser changes were checked again
  with unit and race tests.

A GitHub Actions [workflow](../../../.github/workflows/ci.yml) was added. These
results came from the development environment; there was no external push or
GitHub-hosted workflow execution.

## Small-scale measurements

These observations were collected on the same machine over loopback. They are not
comparisons with production S3, Thanos, or Parquet. Reference VM, Hot, gateway, and
Moto shared the environment. Packs retain physical blocks, so replicas and blocks
spanning window boundaries can increase stored data.

| Measurement | Local / replica 1 | Moto S3 / replica 2 |
|---|---:|---:|
| Archived logical samples | 9,038 | 9,038 |
| Archive wall / CPU time | 0.118 / 0.073 s | 0.221 / 0.101 s |
| Archive throughput | About 76,493 samples/s | About 40,909 samples/s |
| Peak RSS upper bound, including completed child processes | 22,824 KiB | 22,820 KiB |
| Pack / index / catalog | 41,087 / 35,260 / 2,808 bytes | 80,989 / 49,007 / 2,808 bytes |
| Hot-only median, reference → tiered | 1.33 → 4.33 ms | 1.90 → 3.58 ms |
| Cold-only median, reference → tiered | 3.11 → 3.33 ms | 1.05 → 37.88 ms |
| Boundary rate median, reference → tiered | 1.57 → 3.71 ms | 1.09 → 28.44 ms |

Latency used five runs of each fixed query with VM result cache and gateway block
cache disabled. Process and OS caches were warm. Small fixtures, few repetitions,
and contention from colocated processes make these unsuitable as performance
targets.

A separate cache test enabled 32 MiB of gateway cache and queried
`cold_only[30m]` twice. With Moto, the first query took 31.63 ms, performed six
Range GETs, and downloaded 23,240 bytes. The second took 4.61 ms, with zero Range
GETs and 5,616 bytes for catalog reads. Values matched and the VM result cache was
disabled. Initial read amplification, including catalog and indexes, was 24.21×,
using 60 returned samples × 16 bytes as the denominator. Full catalog reads and
bounded index scans had substantial fixed cost; these results do not establish a
performance advantage for native storage.

Codec-only benchmarks used 1,000 series × 128 samples, a one-second interval, two
labels, and GOMAXPROCS=2: `BenchmarkBuildNativePack` measured 11.03 ms/op,
16.41 MB/s, and 6,567,880 B/op; `BenchmarkPortableToWire` measured 4.134 µs/op,
34.35 MB/s, and 5,154 B/op. They exclude S3 traffic and the archiver's independent
raw-data comparison.

```sh
go test -mod=vendor ./internal/pack -run '^$' -bench . -benchmem -benchtime=1s
```

## Untested scope and limits

Real AWS S3/MinIO authorization, durability, consistency, production performance,
sustained high-cardinality load, deletion at actual retention expiry, operational
late-write rejection upstream, resharding, catalog updates with frontend result
cache enabled, other VM versions, single-node VM, and measured Thanos/Parquet
comparisons were not tested. Moto exercises the SDK's SigV4 signing path but does
not replace real AWS credential validation.

The MVP established reproducible reads and queries, not production certification.
Hot retention does not follow an external watermark, and L must be enforced at
every ingress. It does not guarantee data preservation through indefinite outages
or unsupported backfill/deletion. The [current operations guide](../../operations.md)
describes the current architecture's operating contract; it has evolved since
this historical run.
