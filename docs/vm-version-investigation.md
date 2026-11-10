# VictoriaMetrics version compatibility

The oldest OSS cluster release that passed this investigation's end-to-end and
conflicting-replica checks is **v1.134.0-cluster**. The preceding v1.133.0-cluster
fails when numeric samples and stale markers share timestamps across storage
nodes. **The supported deployment contract remains v1.155.0-cluster only.** These
results identify a candidate lower bound for future qualification; they do not
qualify every intervening release or change the codec/catalog pin.

Tests ran on 2026-11-10 UTC against vmtier commit
`b0cb82e12f7456369b0fd33aab186a007fba8ec4`, without changing production code,
dependencies, old fixtures, or VM sources. Evidence and reproduction drivers are
in [results/vm-compatibility-2026-11-10](results/vm-compatibility-2026-11-10/).

## Different compatibility boundaries

| Meaning | Observed boundary | Evidence and limitation |
| --- | --- | --- |
| Supported deployment | v1.155.0-cluster only | Existing [compatibility contract](compatibility.md); unchanged. |
| End-to-end compatibility candidate, including conflicting replicas | v1.134.0-cluster | 360 exact JSON-gateway comparisons, 360 SQLite-service comparisons, and the directed conflicting-replica test passed. v1.133.0 fails the directed test. |
| Native export block layout | v1.82.0-cluster | v1.81.2 is rejected by the current decoder; v1.82.0 archived and served Cold data, but its full differential suite failed on duplicate values. Layout compatibility is insufficient. |
| Cold query frontend RPC | v1.79.0-cluster | Five HTTP APIs through an old stock vmselect matched the current frontend over the same current Cold gateway. v1.78.1 fails data queries using `search_v6`. This does not cover old native exports or mixed-version Hot clusters. |
| Previously published catalogs/packs | Exact current pin only | Old VM identities remain rejected even if the enclosed portable bytes decode. No relabeling or in-place migration was performed. |

## Executed matrix

Each full differential run aligns source vmstorage, source vmselect, vminsert,
oracle and query frontend to the candidate release. vmtier always uses its
unchanged v1.155.0 codec and RPC implementation. All experiments use new,
disposable catalogs and object directories.

Release dates below reproduce the official release notes' `Released at` calendar
date in ISO 8601 format. Upstream does not specify a time zone for these dates.
They are distinct from GitHub's release-publication timestamp: for example,
v1.134.0 is dated 2026-01-16, while its GitHub release was published on 2026-01-19.
The [release-date evidence](results/vm-compatibility-2026-11-10/release-dates.json)
retains both the upstream date text and GitHub's `published_at` timestamp.

| OSS cluster version | Release date | Full-path result | Additional observation |
| --- | --- | --- | --- |
| [v1.78.1](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.78.1) | 2022-07-08 | Native decode fails: `invalid portable row count` | Frontend-only queries fail with HTTP 422; gateway logs `unsupported rpcName: "search_v6"`. Labels APIs alone pass. |
| [v1.79.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.79.0) | 2022-07-14 | Native decode fails with the same error | Frontend-only `query`, `query_range`, `series`, `labels`, and label values all pass. |
| [v1.81.2](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.81.2) | 2022-09-08 | Native decode fails with the same error | Confirms the last v1.81 patch remains on the old native layout. |
| [v1.82.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.82.0) | 2022-10-07 | Native archive and Cold-only queries pass; full suite fails | Exact comparison diverges for `duplicates` in the Hot-only range. This failure is not evidence of a Cold decoder defect; the old stock duplicate behavior itself differs between the independently ingested datasets. |
| [v1.128.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.128.0) | 2025-10-17 | Original 360 comparisons pass | Both the single-stream stale collision fixture and the conflicting-replica fixture fail archive verification. |
| [v1.133.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.133.0) | 2026-01-02 | 360 comparisons with single-stream collisions pass | Conflicting-replica fixture fails archive verification. One successful fixture is insufficient to establish compatibility. |
| [v1.134.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.134.0) | 2026-01-16 | Original and collision fixtures each pass 360 comparisons | SQLite `serve` with forced multi-pack windows also passes 360 comparisons; conflicting replicas archive successfully and Cold raw results exactly match the source. |
| [v1.155.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.155.0) | 2026-10-23 | Collision fixture passes 360 comparisons | Control run: conflicting replicas also archive and round-trip successfully. |

Full runs cover two tenants, Cold-only, Hot-only and boundary queries, instant
and range evaluation, label/series discovery, archive replay, restarts, cache
reuse and failure handling. Query results compare exact labels, timestamps and
numeric strings without tolerance. Result caches are disabled and deduplication
is 1 ms. The SQLite run forces at most four blocks per pack and uses the 5-second
label API deadline.

The v1.78.1, v1.79.0, v1.81.2 and v1.82.0 experiments omit four newer flags:
`inmemoryDataFlushInterval`, `globalReplicationFactor`,
`search.skipSlowReplicas`, and `search.maxLabelsAPIDuration`. Therefore these are
protocol/format probes, not demonstrations that the current deployment command
line works on those releases. Actual child commands are retained in each report.

## Why v1.133.0 is insufficient

The directed test puts samples `(t1, 1)` and `(t2, 2)` on one stock vmstorage and
stale markers at the same timestamps on another. One stock source vmselect reads
both nodes, with 1 ms deduplication throughout. This preserves the conflict across
separate native blocks instead of depending on whether a single storage node has
already merged its samples.

* v1.128.0 and v1.133.0 choose stale markers. The independent raw source returns
  `NaN`, while current vmtier's deduplication chooses numbers. Archiving refuses
  publication with `raw source/native stale marker mismatch`; the retained
  catalogs have generation 0 and no entries.
* v1.134.0 and v1.155.0 choose the numeric values. Archiving succeeds, and Cold
  raw queries return the same `1` and `2` at the same timestamps.

The upstream change is
[`d5170723ea4b0fd48b71974c7302f137b2ef47bf`](https://github.com/VictoriaMetrics/VictoriaMetrics/commit/d5170723ea4b0fd48b71974c7302f137b2ef47bf),
documented in the [v1.134.0 release](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.134.0).
It prefers numeric values over stale markers at an identical timestamp. The
[v1.133.0 implementation](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cfca33124ecd9c6362f704a87d7129a38c55f6e2/lib/storage/dedup.go)
still prefers stale markers.

The changelog also lists the fix in v1.110.28 and v1.122.13, both dated
2026-01-16 in their official release notes. Those
[v1.110.28](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.110.28)
and [v1.122.13](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.122.13)
release assets are Enterprise releases, without corresponding public OSS cluster
tags/assets in the inspected release metadata. They were not installed or tested
and are not counterexamples establishing an older **OSS** compatibility floor.

## Native format and RPC evidence

The [v1.82.0 release notes](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.82.0)
explicitly document an incompatible native export format change. The old block
header contains minimum timestamp, first value, row count, scale and encoding
types. The new header also includes maximum timestamp and precision bits. The
current bounded decoder in `internal/pack/pack.go` expects the newer layout.
Compare the
[v1.81.2 block encoder](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/e9a0b803a4c5bcf31587f26cd0b5bdb84cd032cb/lib/storage/block.go)
with the
[v1.82.0 block header](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/58ee1b9edb66ba8209e35f56d2698b5f1900df9f/lib/storage/block_header.go).
The v1.82.0 notes also warn about incorrect query/relabeling results; this report
does not recommend deploying that release.

For the RPC boundary, compare
[v1.78.1 netstorage](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/d27a375d347ab1136f1388835dc367942cc74d7c/app/vmselect/netstorage/netstorage.go)
with
[v1.79.0 netstorage](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/a87f143e9203cc20c3203f50bd9c79bb403e342e/app/vmselect/netstorage/netstorage.go).
The data-search RPC moves from `search_v6` to `search_v7`. The current gateway's
upstream server accepts `search_v7` without fallback. The isolated frontend probe
uses freshly created current-codec archives so native export compatibility cannot
confound that RPC observation.

## Existing archives and remaining scope

`pack.ValidateIndex` and `LoadBlock` require the exact `VMVersion` and
`VMRevision`. JSON, SQLite and PostgreSQL catalogs also check the VM pin.
`TestPreviousReleasePackRejected` explicitly rejects the v1.128.0 identity.
Pack, archive, gateway and catalog package tests passed in this investigation;
their output is retained as `unit-tests.log`.

Candidate-source experiments create **new** packs marked with the running
vmtier codec identity. That field is not authenticated proof of the source binary
version; candidate binaries and SHA-256 hashes are recorded separately. These
experiments do not make old v1.128.0 catalogs usable or authorize rewriting their
identity. Follow the existing upgrade procedure for deployed archives.

This is a bounded investigation, not an exhaustive support matrix. In particular:

* v1.135 through v1.154 were not each installed and run. Selected versions were
  inspected in the source scan; no blanket runtime guarantee is made.
* SQLite and legacy JSON paths used local object storage. PostgreSQL HA, real S3,
  natural retention expiry, all MetricsQL expressions, scale/performance and
  release qualification were not repeated for historical VM versions.
* Enterprise releases, single-node VictoriaMetrics, mixed-version Hot clusters,
  and VM versions newer than the current pin are outside this investigation.

Supporting v1.134.0 as a product contract would still require explicit version
qualification and source/codec identity rules. The current pin and published
archive acceptance rules remain unchanged.
