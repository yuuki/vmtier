# Archive bounds validation

The changes for [#6](https://github.com/yuuki/vmtier/issues/6),
[#7](https://github.com/yuuki/vmtier/issues/7), and
[#8](https://github.com/yuuki/vmtier/issues/8) passed the checks below on
2026-11-06. They add fixed-window pack rotation, incremental independent raw
verification, and read-only capacity preflight. The retained JSON v2 writer stays
single-pack. These runs use the original reader, before the separate cache,
range-coalescing, admission, and capacity-metrics changes.

## Source and runtime

The tested source matches archive commit `b56b045`, based on foundations commit
`4a33e5b` and oracle commit `2674518`. The fixture runs began before the archive
commit was recorded; their unchanged raw reports identify the actual files and
binaries by SHA-256. The recorded Go source hash was independently checked against
the files in `b56b045`:

```text
6e55ff5ec90a902b7bbf28e77e79eaac807d5d1af01b611cb96d07335738c1d6
```

The hash covers sorted `cmd/**/*.go`, `internal/**/*.go`, `go.mod`, and `go.sum`,
each encoded as path, NUL, contents, NUL. Both reports include harness and binary
hashes. The vmtier binary hash is
`1a8fcc38ed1d92eee4ea30974043cf36408715289fd694bb4edea253beb45c1e`.

Environment: Linux amd64, Debian 13, Go 1.27.3, Python 3.12.14, and Moto 5.1.15
for S3. The outer invocation and all recorded workload children used
`GOMAXPROCS=3`. Unmodified stock VictoriaMetrics is pinned to
`v1.128.0-cluster`, revision `9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`.
Both frontends used the recorded 30-second labels API deadline and disabled VM
result caching.

## Checks and retained results

`make build test race vet` passed, as did all 11 Python harness/packaging tests.
Go regressions cover complete-record rotation, physical/input/pack limits,
cross-pack conflicts and stale markers, truncation, failed staging/restart,
atomic publication, and zero-pack empty windows. Preflight checks prove that it
reports complete and over-limit observations without opening or modifying a
catalog or contacting the configured blob store. See [preflight](preflight.md)
and [pack bounds](archive-packs.md) for commands and accounting.

| SQLite fixture | UTC interval | Windows / packs | Exact query comparisons | Raw report |
| --- | --- | --- | --- | --- |
| Local blobs, one replica | 11:50:11–11:50:21 | 6 / 24 | 360 | [local.json](results/archive-bounds/local.json) |
| Moto S3, two replicas | 11:50:44–11:55:49 | 6 / 56 | 360 | [s3-replicated.json](results/archive-bounds/s3-replicated.json) |

Both runs forced `max_pack_blocks=4`, required multiple packs in every window,
and passed **720 exact instant/range comparisons** in total across Cold, Hot, and
their boundary. Label names, label values, and series discovery also matched the
reference exactly. Hot was recreated with recent samples only; old-range exports
were empty before the comparisons.

The service tests also passed interrupted-export restart, independent stream
progress, owner exclusion, configuration-drift rejection, corrupt/missing-object
failure, cache reuse, and offline verification/backup checks. During the separate
blob outage, the same running service remained healthy and ready, attempted the
intended blob read, returned a stock-vmselect HTTP 422 backend error, and recovered
exact reference results without changing coverage. S3 recorded three injected
failed GET attempts. The strict proxy verified Content-MD5 on all 117 PUTs,
rejected no conditional requests, removed 6,094 ETags, and observed 2,224 Range
requests. Full counters and process settings remain in the unchanged reports.

Reproduce with the commands in [pack bounds](archive-packs.md), using fresh work
directories. The independent [raw-oracle benchmark](raw-oracle-validation.md)
compares the same 200,000-sample fixture against baseline `12414db`: incremental
decoding allocated about 77% fewer bytes. That earlier allocation measurement is
separate from these stock-VM runs and is not a peak-RSS measurement.

These are small loopback fixtures. They do not qualify a real object-storage
provider, natural Hot retention expiry, production ingress enforcement, workload
capacity, combined archive/query peak RSS, or release support. Preflight does not
measure upload/read-back or sustained recovery headroom. Hosted CI evidence is
separate from the local results recorded here.
