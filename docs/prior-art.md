# Related implementations and approach comparison

Review date: 2026-11-06. This is a review of documentation, source, and licenses at pinned public repository revisions. It is not a comparative deployment benchmark or verification of production use. Throughput, compression, and S3 cost advantages over other products have not been measured. Earlier research informed the review, with verified observations distinguished from untested claims.

## Storage formats and query integration

| Format or platform | Additional work to serve stock vmselect | Assessment for the MVP |
| --- | --- | --- |
| VM portable/native packs | vmstorage read RPC, label/time index, portable-to-wire conversion | Reuses VM's representation. Public codecs may decode, so this does not establish decode-free operation, minimum CPU, or minimum storage. Selected after the compatibility spike and differential tests. |
| Prometheus TSDB blocks and Thanos Store Gateway | VM-to-TSDB conversion and a StoreAPI-to-VM read RPC adapter | Existing block indexes, caches, and compaction are useful, but external labels, tenants, duplicates, overlap, deletion markers, and bidirectional codec conversion add work. Stock vmselect does not implement StoreAPI. Not selected for the MVP. |
| Parquet and a reader | Label/time index, series reconstruction, VM read RPC adapter | Useful for external analytics, but does not automatically preserve VM range-vector, staleness, or duplicate semantics. No comparative read-amplification or compression measurement. Not selected for the MVP. |
| Snapshot/partition restoration | Download selected partitions, manage restored vmstorage, route queries | Reuses the existing reader but requires complete indexes, suitable retention, local capacity, and startup management. Retained as a recovery option, not the normal query path. |
| A POSIX layer such as JuiceFS | VM deployment, caching, and operational integration | Compatibility with VM mmap, locking, fsync, I/O, and sample-age migration is unverified. File inactivity is not equivalent to sample age. |

A read gateway is a separate design axis from storage format. Each format can delegate MetricsQL evaluation to VM if it supplies compatible Hot/Cold raw series. Replacing vmselect with Thanos Querier or a SQL engine does not satisfy the same integration requirement.

## Reviewed implementations

### vmcoldstorage

Reviewed revision: [`e22e2bc5cfff1418359e043bbd35c9afaa7ef0e3`](https://github.com/kamijin-fanta/vmcoldstorage/tree/e22e2bc5cfff1418359e043bbd35c9afaa7ef0e3), matching `main` at review time.

[architecture.md](https://github.com/kamijin-fanta/vmcoldstorage/blob/e22e2bc5cfff1418359e043bbd35c9afaa7ef0e3/docs/architecture.md) describes native export framing, opaque payloads, tenant catalogs, Range GET, a stable-age guard, and a custom MetricsQL subset. It explicitly states that `vmcoldselect` does not yet provide a Prometheus HTTP frontend and full VM expression engine. Stable age rejects unsuitable input blocks; it does not extend Hot retention.

The root `LICENSE` at that revision returned HTTP 404. The complete history was not audited, so this is not a conclusion that no permission exists anywhere. Reuse permission was not established, and no code was incorporated. Design comparison is separate from code reuse. vmtier's validation concerns stock vmselect integration, age-based offload from existing Hot data, and differential queries after confirming Hot data is absent, rather than a claim of novelty for native packs.

### Victoria Lakehouse

Reviewed revision: [`4c4c21e0d77b46e1cf281dae1705abea13cf6da7`](https://github.com/ReliablyObserve/victoria-lakehouse/tree/4c4c21e0d77b46e1cf281dae1705abea13cf6da7), with an [Apache-2.0 license](https://github.com/ReliablyObserve/victoria-lakehouse/blob/4c4c21e0d77b46e1cf281dae1705abea13cf6da7/LICENSE).

The [deployment architecture](https://github.com/ReliablyObserve/victoria-lakehouse/blob/4c4c21e0d77b46e1cf281dae1705abea13cf6da7/docs/deployment-architecture.md) and README describe a Parquet tier and ingestion mirroring for VictoriaLogs/VictoriaTraces. They do not implement metrics tiering or offload of existing VM metrics after reaching an age threshold.

[patches/README.md](https://github.com/ReliablyObserve/victoria-lakehouse/blob/4c4c21e0d77b46e1cf281dae1705abea13cf6da7/patches/README.md) lists exported wrappers and dispatch patches to the embedded upstream code. It is not evidence that upstream patches are unnecessary. Adapter boundaries and conformance-test ideas informed the comparison; no code was adopted.

### VictoriaLogs object storage PR

[PR #1155](https://github.com/VictoriaMetrics/VictoriaLogs/pull/1155) was open with `mergedBy: null` when reviewed. The reviewed head was [`807ea1488f74470c795c1b458ce4ab1e7cc11d6f`](https://github.com/VictoriaMetrics/VictoriaLogs/tree/807ea1488f74470c795c1b458ce4ab1e7cc11d6f), licensed under Apache-2.0.

The [Object Storage section at that head](https://github.com/VictoriaMetrics/VictoriaLogs/blob/807ea1488f74470c795c1b458ce4ab1e7cc11d6f/docs/victorialogs/README.md#object-storage) describes daily partitions, `-offloadPeriod`, read-only Cold partitions, and dropping/counting late logs targeting those partitions. This changes VictoriaLogs itself; it is not a ready-made external VM metrics gateway. Its explicit offload, retention, and late-arrival contract is useful precedent. Performance was not independently measured.

### Thanos and Thanos Parquet Gateway

Thanos reviewed revision: [`df1f3f3d5e72769dc639cb6006a7972b168672ce`](https://github.com/thanos-io/thanos/tree/df1f3f3d5e72769dc639cb6006a7972b168672ce), [Apache-2.0](https://github.com/thanos-io/thanos/blob/df1f3f3d5e72769dc639cb6006a7972b168672ce/LICENSE).

[sidecar.md](https://github.com/thanos-io/thanos/blob/df1f3f3d5e72769dc639cb6006a7972b168672ce/docs/components/sidecar.md) describes Prometheus TSDB block uploads, usually covering two hours, external labels, and compaction settings. [store.md](https://github.com/thanos-io/thanos/blob/df1f3f3d5e72769dc639cb6006a7972b168672ce/docs/components/store.md) describes StoreAPI, index-header/cache, overlapping series, deletion-marker delay, and the Parquet path. Mapping tenant context to VM account/project IDs requires separate design; external labels must not be casually added to series identity.

Parquet Gateway reviewed revision: [`7f86e0e4daafdc69b30d6278375ab88c12afb87a`](https://github.com/thanos-io/thanos-parquet-gateway/tree/7f86e0e4daafdc69b30d6278375ab88c12afb87a), [Apache-2.0](https://github.com/thanos-io/thanos-parquet-gateway/blob/7f86e0e4daafdc69b30d6278375ab88c12afb87a/LICENSE).

The [README](https://github.com/thanos-io/thanos-parquet-gateway/blob/7f86e0e4daafdc69b30d6278375ab88c12afb87a/README.md) labels it a POC and describes `convert`, `serve`, Thanos Info/Series/Query gRPC, and a subset of Prometheus HTTP APIs. This does not establish VM native codec/RPC compatibility or deletion coordinated with VM retention. Neither project's code was adopted.

### Cortex, parquet-common, and Mimir

- **Cortex:** [`c20496bf28c179541152bdc95d6e482973a6b006`](https://github.com/cortexproject/cortex/tree/c20496bf28c179541152bdc95d6e482973a6b006), [Apache-2.0](https://github.com/cortexproject/cortex/blob/c20496bf28c179541152bdc95d6e482973a6b006/LICENSE). [Parquet Mode](https://github.com/cortexproject/cortex/blob/c20496bf28c179541152bdc95d6e482973a6b006/docs/guides/parquet-mode.md) is marked experimental and documents coexistence with TSDB, TSDB fallback, and conversion markers. It is not treated as a substitute for VM tiering that permits deletion of the Hot originals.
- **parquet-common:** [`61b084ecc459c35ded9152bd1b5013fe1272e744`](https://github.com/prometheus-community/parquet-common/tree/61b084ecc459c35ded9152bd1b5013fe1272e744), [Apache-2.0](https://github.com/prometheus-community/parquet-common/blob/61b084ecc459c35ded9152bd1b5013fe1272e744/LICENSE). The [README](https://github.com/prometheus-community/parquet-common/blob/61b084ecc459c35ded9152bd1b5013fe1272e744/README.md) was reviewed. It is a candidate if Parquet is selected, not a verified compatible API for directly storing/querying opaque VM blocks. Not adopted.
- **Mimir:** [`7fa905593f896dc3d8a1848d0abfe500c7b4cd33`](https://github.com/grafana/mimir/tree/7fa905593f896dc3d8a1848d0abfe500c7b4cd33). The [LICENSE](https://github.com/grafana/mimir/blob/7fa905593f896dc3d8a1848d0abfe500c7b4cd33/LICENSE) contains GNU AGPL v3; the project requirement identifies it as AGPL-3.0-only. Its code is not treated as having Apache-2.0 terms. Comparison is limited to Store Gateway design; no code was incorporated or linked. The complete Store Gateway implementation was not audited.

### KOF Cold Storage Exporter

Reviewed revision: [`eaf69dd1d440bb797446b428220073ab97770f19`](https://github.com/k0rdent/kof/tree/eaf69dd1d440bb797446b428220073ab97770f19), [Apache-2.0](https://github.com/k0rdent/kof/blob/eaf69dd1d440bb797446b428220073ab97770f19/LICENSE).

Reviewed `Run`, `collectWindows`, and `exportMetricsWindow` in [exporter.go](https://github.com/k0rdent/kof/blob/eaf69dd1d440bb797446b428220073ab97770f19/kof-operator/internal/coldstorage/exporter.go), plus `ExportMetrics` and `ScanVMExport` in [vmclient.go](https://github.com/k0rdent/kof/blob/eaf69dd1d440bb797446b428220073ab97770f19/kof-operator/internal/coldstorage/vmclient.go). The implementation exports JSON time windows, converts them to Parquet, uploads through S3 multipart, and writes a final `_SUCCESS` marker. It is useful archiver precedent, but does not replace stock vmselect read RPC or Hot/Cold raw-series merging. No code was adopted.

### vmbackup-partition

Reviewed revision: [`c0068e2dd89201aea01bb23b85cceba4ab38f87f`](https://github.com/surenwuyuwuqiu/vmbackup-partition/tree/c0068e2dd89201aea01bb23b85cceba4ab38f87f), [Apache-2.0](https://github.com/surenwuyuwuqiu/vmbackup-partition/blob/c0068e2dd89201aea01bb23b85cceba4ab38f87f/LICENSE).

The [README](https://github.com/surenwuyuwuqiu/vmbackup-partition/blob/c0068e2dd89201aea01bb23b85cceba4ab38f87f/README.md) describes monthly snapshot backups, indexdb handling, vmrestore, and destination-retention considerations. Its measurements and full-compatibility claims were not reproduced. It is a recovery approach rather than direct object-storage reads during ordinary queries. Not adopted.

### SeaweedFS and JuiceFS

- **SeaweedFS:** [`0dd33f5f0fc53913b4059ad50edbb8cce4fe8b0e`](https://github.com/seaweedfs/seaweedfs/tree/0dd33f5f0fc53913b4059ad50edbb8cce4fe8b0e), [Apache-2.0](https://github.com/seaweedfs/seaweedfs/blob/0dd33f5f0fc53913b4059ad50edbb8cce4fe8b0e/LICENSE). README and LICENSE were reviewed. [Cloud Tier](https://github.com/seaweedfs/seaweedfs/wiki/Cloud-Tier) is comparison material; its wiki revision was not pinned and volume migration was not tested.
- **JuiceFS:** [`fd69a5715ff6d4a5b826b83aeee451139fcad106`](https://github.com/juicedata/juicefs/tree/fd69a5715ff6d4a5b826b83aeee451139fcad106), [Apache-2.0](https://github.com/juicedata/juicefs/blob/fd69a5715ff6d4a5b826b83aeee451139fcad106/LICENSE). README and LICENSE were reviewed. VM mmap/lock/fsync behavior, query latency, and sample-age offload were not demonstrated.

Neither is a code dependency. A POSIX or object-store API alone does not establish correct sample-age tiering for existing VM data.

## Review and licensing boundaries

The revisions above came from repository pages at review time, and the inspected sources were pinned to those revisions. PR status is a time-specific observation rather than a property of a commit. Before adopting a newer upstream release, recheck LICENSE, NOTICE, APIs, codecs, dependency licenses, and conformance tests.

This review does not comprehensively certify another project's licensing or performance. Actual VM modules and dependencies used by vmtier are recorded in [third-party notices](../THIRD_PARTY_NOTICES.md). The project does not copy comparison implementations or derive code from vmcoldstorage without verified permission.
