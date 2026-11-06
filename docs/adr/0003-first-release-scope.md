# ADR 0003: A single-host first release with a transactional catalog

## Status

Selected for the next implementation phase. This ADR does not describe features
already implemented in the v2 MVP. The specification and delivery gates are in
[the first-release design](../design.md).

## Context

The MVP establishes native pack and stock vmselect compatibility without S3
conditional writes. It leaves ordinary capacity and operational problems: a
10,000-entry whole-file catalog, one pack per window, repeated small object reads,
and separate clients that cannot share a provider request budget. The next release
should address these problems while keeping deployment and maintenance small.

## Decision

- Keep native pack/index v1 and pinned stock VM RPC integration from ADR 0001.
- Keep publication authority independent of S3 as decided in ADR 0002.
- Run archive scheduling, catalog access, and the read gateway in one `serve`
  process with a persistent state directory and shared S3 attempt limiter.
- Replace the bounded JSON catalog with SQLite and indexed tenant/time selection.
  Publish all packs for one tenant/source/window in one transaction; unrelated
  tenants can advance independently. Retain explicit empty-window coverage.
- Keep verification bounded in memory, stream the raw JSON oracle, and split packs
  independently of logical windows. Do not introduce a disk sample spool yet.
- Add offline catalog backup/restore and an explicit offline v2 import.
- Improve read costs with a bounded LRU and adjacent-range coalescing; require
  measurements before adopting another index or storage format.

SQLite replaces the MVP state-file implementation, not the separation between data
and publication authority. It addresses long-lived catalog growth and transactions,
not automatic HA. The existing HTTP catalog API is not a required production service
in this deployment.

## Alternatives

| Alternative | Reason not selected now |
| --- | --- |
| Keep JSON and only raise its limit | Full-history read/rewrite costs and an eventual hard stop remain |
| Segmented manifests and a custom journal | More persistence/recovery machinery to maintain than SQLite |
| PostgreSQL or a distributed catalog | Adds services and dependencies without a current multi-host requirement |
| Keep three separately deployed services | Adds coordination and prevents a simple shared bucket budget |
| Disk verification and new indexes immediately | Not yet shown necessary within a bounded initial workload |
| Weaken verification to native EOF or counts | Loses completeness checks already shown necessary by the MVP |

## Consequences

One host and local disk remain availability dependencies. Offline backups require
maintenance downtime and enough Hot overlap for recovery. A process-local limiter
covers the bucket only when vmtier is its sole client. Late-write enforcement stays
outside vmtier, and unlimited outages are not lossless.

There is no HA, automatic GC, Cold retention, backfill, dynamic topology, or universal
VM-version compatibility in the first release. Real Sakura support and capacity
claims require qualification; local MVP results do not provide that evidence.
