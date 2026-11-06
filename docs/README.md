# Documentation

New to vmtier? Start with the [project overview](../README.md) and
[Getting started](getting-started.md). The current runtime is **`vmtier serve`
with a SQLite catalog**. The separate-process JSON v2 MVP is retained for regression
testing and migration; the earlier all-S3 v1 catalog is historical only.

## Use and operate vmtier

| Document | What it covers |
| --- | --- |
| [Getting started](getting-started.md) | Prerequisites, source build, and disposable local/S3 demos |
| [Service guide](serve.md) | Configuration, startup, query frontend, scheduling, request budgets, and monitoring |
| [Archive capacity preflight](preflight.md) | Read-only sizing and completeness checks before catalog initialization |
| [Configuration examples](../examples/README.md) | Which example to use and which values must be supplied |
| [SQLite catalog maintenance](sqlite-catalog.md) | Offline inspection, backup, restore, and JSON v2 import |
| [Compatibility](compatibility.md) | Pinned VM revision, supported RPCs, native format, and S3 requirements |

## Understand and contribute

| Document | What it covers |
| --- | --- |
| [Current architecture](architecture.md) | Implemented data flow, publication, query path, and resource limits |
| [Archive packing](archive-packs.md) | Complete-record rotation, per-pack and whole-window bounds, and atomic publication |
| [Query and memory accounting](query-memory.md) | Reader admission, cache/archive components, metrics, and measured memory limits |
| [Contributing](../CONTRIBUTING.md) | Development workflow, issue reports, pull requests, and documentation conventions |
| [Testing](testing.md) | Unit checks, SQLite and JSON v2 differential suites, and CI artifacts |
| [First-release design and roadmap](design.md) | Implemented stages and remaining archive, reader, and qualification work |
| [Prior art](prior-art.md) | Related storage systems and design context |
| [Third-party notices](../THIRD_PARTY_NOTICES.md) | Dependencies, attribution, and binary redistribution |

Design decisions: [native gateway](adr/0001-native-gateway.md),
[independent catalog](adr/0002-independent-catalog.md), and
[first-release scope](adr/0003-first-release-scope.md). ADRs record the rationale at
the time of the decision; the architecture and service guides describe current behavior.

## Validation records

These are reports of specific runs, not setup guides or claims that all later
revisions have passed. Original result JSON and provenance are preserved. Use
[Testing](testing.md) to run checks for your checkout.

| Report | Runtime and evidence scope |
| --- | --- |
| [Archive and reader integration](implementation-validation.md) | Four SQLite/JSON v2 local and Moto suites after archive and reader changes; 1,440 exact comparisons |
| [Local workload and combined memory](workload-validation.md) | Conditional ten-tenant/5,000-series envelope, concurrent archive/query pressure, and the preserved failed 10,000-series target |
| [Catalog capacity](catalog-capacity-validation.md) | Indexed selection and metrics with 87,600 synthetic window descriptors; no payload qualification |
| [Offline recovery and cutover](recovery-validation.md) | JSON v2 import, older-backup catch-up, service crashes, and expired-source refusal on local stock-VM fixtures |
| [Metadata before/after](metadata-validation.md) | Five-second labels deadline on the retained fixture after reader improvements |
| [Reader bounds](reader-validation.md) | LRU, shared reads, adjacent ranges, and query admission |
| [Archive rotation](archive-validation.md) | Forced multi-pack windows and exact stock-VM comparisons |
| [Incremental raw oracle](raw-oracle-validation.md) | Bounded parsing, malformed response handling, and parser memory measurements |
| [Harness and failure regressions](foundation-validation.md) | Earlier runtime settings, hour-boundary fixtures, and live blob-outage proof across four suites |
| [Hosted CI artifact validation](ci-evidence-validation.md) | Successful hosted run and byte-preserving artifact download at the named revision |
| [SQLite service integration](serve-validation.md) | Stage 2 scheduler, gateway, request budget, restart, and recovery runs |
| [SQLite catalog foundation](sqlite-catalog-validation.md) | Stage 1 catalog and offline maintenance checks |
| [JSON v2 MVP validation](validation.md) | Separate-process independent-catalog baseline |
| [All-S3 v1 archive](results/legacy-v1/README.md) | Superseded conditional-write catalog and its original results |

The local workload and catalog reports establish only their named fixture limits.
Local/Moto tests do not qualify real Sakura/AWS behavior, arbitrary production
capacity, natural Hot retention expiry, or external lateness enforcement. Limits and pending
qualification are described in the [roadmap](design.md).

## Legacy JSON v2

- [Deployment and operations](operations.md): separate `catalog`, `archive`, and
  `gateway` processes, their configuration, and recovery constraints.
- [MVP design](mvp-design.md): the earlier JSON v2 architecture and its limitations.
- [Migration to SQLite](sqlite-catalog.md): offline import without rewriting pack objects.

Existing document paths remain available so links from issues, pull requests,
and validation reports continue to resolve.
