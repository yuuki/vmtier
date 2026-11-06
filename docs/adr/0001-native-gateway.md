# ADR 0001: Native packs and a stock vmselect read gateway

## Status and scope

Accepted for the MVP, targeting VictoriaMetrics OSS `v1.128.0-cluster`, commit `9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. Reproducible E2E tests and the validation report provide execution evidence; this decision record does not establish that unrun tests passed. The [first-release design](../design.md) describes subsequent work.

[ADR 0002](0002-independent-catalog.md) supersedes the original use of conditional S3 PUT for catalog publication. Native packs and the stock vmselect gateway remain the selected approach.

## Problem

Existing Hot metrics must be archived to S3 after reaching a sample age, while stock vmselect continues evaluating the same expressions after those metrics disappear from Hot. Mirroring all ingestion, requiring manual restoration for every query, or implementing a MetricsQL subset would not meet that goal. Storage format and read protocol are separate choices.

## Decision

- Store portable/native payloads and a bounded JSON label/time/block index in S3.
- Use the public [`vmselectapi.API` and `NewServer`](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/vmselectapi) from an independent binary. Do not patch stock VM binaries or upstream source.
- Let the top-level stock vmselect fan out directly to Hot vmstorage and the Cold gateway. The MVP needs neither a custom Hot adapter nor a downstream native listener.
- Preserve native bytes at rest and allow bounded, validated decoding/re-encoding during queries. [`Block.UnmarshalPortable`](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/lib/storage/block.go) itself calls `UnmarshalData`; use of a public API does not prove decode-free operation.
- Distinguish logical intervals from physical block extents. Preserve boundary clipping, tenant identity, and raw-data merge semantics.
- Scope the MVP to fixed tenants/topology, a single writer, immutable published objects, generation CAS in an independent catalog service, and externally enforced bounded lateness. S3 needs ordinary PUT/GET/Range GET, without conditional PUT or ETags.

## Alternatives

Pinned primary sources are recorded in [prior-art.md](../prior-art.md).

| Alternative | Reason not selected for the MVP | Reconsider when |
| --- | --- | --- |
| VM-to-TSDB conversion, Thanos, and a VM adapter | Adds VM/TSDB codec conversion, a StoreAPI adapter, and translation of external labels, tenants, overlap, and compaction. | Measurements show shared TSDB infrastructure outweighs conversion costs. |
| Parquet and a VM adapter | VM query compatibility still requires an index, series reconstruction, and an adapter. | Shared SQL/analytics becomes a primary requirement and the same oracle validates semantics. |
| Snapshot/partition restoration | Requires local capacity, startup, index, and retention management before ordinary queries. | A recovery path or a colder tier can tolerate restoration latency. |
| POSIX/S3 layer | Sample-age migration and VM I/O compatibility are unverified. | mmap, locking, fsync, and retention behavior are demonstrated and operational costs are compared. |
| Hot native fan-out inside the gateway | Expands dependence on upstream private clients. | Independent routing becomes necessary and partial-response, replica, and cancellation parity is verified. |
| Reuse of Prometheus postings indexes | Adds dependencies and format translation while packs remain small and bounded. | Measurements show JSON index CPU or memory is a bottleneck. |

## Consequences

Stock VM retains expression evaluation, without copying its private query implementation. The read RPC and native codec are not stable external protocols, so version pinning and upgrade differential tests remain necessary. Full index scans, per-block Range GETs, and bounded query materialization are not claimed to be optimal at large scale.

Hot and Cold cannot be skipped as interchangeable replicas. The frontend requires `replicationFactor=1`, `globalReplicationFactor=1`, `skipSlowReplicas=false`, and `denyPartialResponse=true`. Disable its result cache so an old empty result cannot hide newly published Cold data. Immutable block caching and VM result-cache consistency are separate concerns.

Stock VM retention is not coordinated with an external archive watermark. The contract requires a finite safety margin and external ingestion enforcement; it does not promise no loss through indefinite outages. No performance superiority over Thanos or Parquet has been established.
