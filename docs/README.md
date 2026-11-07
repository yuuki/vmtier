# Documentation

vmtier is under development. The current runtime is the single-owner SQLite
service; retained JSON v2 commands support regression tests and offline migration.
Use the guides for current behavior and the evidence reports for the exact scope
of recorded tests.

## Use and operate vmtier

| Task | Read |
| --- | --- |
| Build, try a disposable demo, or connect an existing cluster | [Getting started](getting-started.md) |
| Understand scheduling, ownership, request limits, and health | [SQLite service](serve.md) |
| Monitor, handle failures, and plan recovery | [Operations](operations.md) |
| Initialize, inspect, import, back up, or restore state | [SQLite catalog](sqlite-catalog.md) |
| Size windows before initializing a catalog | [Capacity preflight](preflight.md) |
| Check deployment constraints | [Guarantees and limits](limits.md) |

## Understand and develop vmtier

| Topic | Read |
| --- | --- |
| Data flow and code ownership | [Architecture and source map](architecture.md) |
| Supported commands and backends | [Command reference](commands.md) |
| VM codecs, RPCs, and upgrades | [Compatibility](compatibility.md) |
| Archive format and bounds | [Archive packing](archive-packs.md) |
| Work budgets, memory lifetimes, and metrics | [Memory accounting](query-memory.md) |
| Build checks, differential suites, and CI artifacts | [Testing](testing.md) |
| Contribution and documentation conventions | [Contributing](../CONTRIBUTING.md) |

## Design and evidence

- [First-release design](design.md): implementation status and remaining acceptance work.
- Architecture decisions: [native gateway](adr/0001-native-gateway.md),
  [independent catalog](adr/0002-independent-catalog.md),
  [first-release scope](adr/0003-first-release-scope.md).
- [Related implementations](prior-art.md) and [third-party notices](../THIRD_PARTY_NOTICES.md).
- [Validation evidence index](evidence.md): scoped reports and retained raw results.

## Legacy architectures

- [JSON v2 deployment](legacy-json-v2.md) and [MVP design](mvp-design.md).
- [JSON v2 validation](validation.md): historical regression baseline.
- [All-S3 v1 archive](results/legacy-v1/README.md): superseded architecture;
  these results do not qualify the SQLite runtime.
