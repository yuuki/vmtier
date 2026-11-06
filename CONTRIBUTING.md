# Contributing to vmtier

Contributions to code, documentation, and reproducible bug reports are welcome.
vmtier is an independent Apache-2.0 project. Start with the [README](README.md)
and [getting started](docs/getting-started.md). The current runtime is
[`vmtier serve`](docs/serve.md); the separate JSON v2 commands remain for regression
testing and migration. The [first-release design](docs/design.md) distinguishes
implemented behavior from planned work.

## Reporting a problem or proposing a change

Use [GitHub issues](https://github.com/yuuki/vmtier/issues) for reproducible bugs
and proposals. Include the vmtier commit, VictoriaMetrics version, catalog backend,
relevant configuration with credentials removed, steps to reproduce, expected and
actual behavior, and relevant logs or test results. For query discrepancies, include
the expression, time bounds, tenant, and a small reproducible dataset when possible.
Do not include private metric data, credentials, or access tokens in public reports.

For changes to storage formats, native RPC, the fixed catalog contract, or deployment
scope, explain the workload or failure that motivates the change before implementing
it. Record material format, protocol, or operational decisions in an
[ADR](docs/adr/). Prefer a small change that solves a demonstrated problem.

## Development setup

Follow the [build instructions](docs/getting-started.md) from a checkout of this
repository. Bootstrap creates disposable `.deps/` and `vendor/` trees from pinned
sources. Do not edit those generated files to make a test pass. Keep stock
VictoriaMetrics binaries unmodified and record the revision when evaluating an
upgrade.

| Path | Responsibility |
| --- | --- |
| `cmd/vmtier/` | CLI commands, service ownership, scheduling, and shutdown |
| `internal/archive/` | Native export, preflight, streaming verification, and publication |
| `internal/catalog/` | SQLite and legacy JSON catalog state, coverage, and maintenance |
| `internal/cold/`, `internal/gateway/` | Cold reads, caching, matchers, and native read RPC |
| `internal/config/` | Configuration validation and defaults |
| `internal/objectstore/`, `internal/pack/` | Blob storage, S3 request budget, pack/index encoding |
| `scripts/` | Pinned build tools, E2E/recovery/qualification fixtures, and evidence packaging |
| `docs/`, `examples/` | User/developer guides, recorded evidence, and configuration examples |

For runtime changes, run relevant focused tests and the repository checks:

```sh
make build test race vet
python3 -m unittest discover -s scripts -p 'test_*.py'
```

Use `make GO=/path/to/go ...` when Go is outside `PATH`. Changes to archive
contents, publication, query results, native RPC, or S3 behavior also need the
stock-VM differential suites. [Testing](docs/testing.md) describes setup, the
SQLite and JSON v2 suites, offline recovery, workload/catalog qualification, CI,
and evidence interpretation. Run the recovery suite for migration or restore
changes; use the qualification fixtures for capacity or memory claims. Test observable behavior
and meaningful failure boundaries rather than mirroring private implementation
details. For documentation-only changes, check links, commands against current
behavior, and planned versus measured claims; benchmarks need not be rerun.

## Documentation and code conventions

Write repository documentation, code comments, commit/PR descriptions, and
user-facing diagnostics in English. Test fixtures may contain other languages
when exercising Unicode behavior. Format Go changes with `gofmt`. Preserve upstream
license text and attribution.

Keep the [documentation index](docs/README.md) current when adding or moving a guide.
Put current usage in the user guides, future behavior in the design, and measured
results in dated validation reports. Update configuration examples with related
behavior changes; examples must not contain credentials or assume access to a
contributor's environment.

Preserve recorded result JSON and provenance. Editing documentation does not turn
an earlier run into a new result. Store new measurements separately until the report
is deliberately updated. A local S3 test double is not a live provider test; a
recent-only Hot reconstruction is not a natural retention-expiry test. Real-provider
tests require an explicitly configured disposable target.

## Pull requests and releases

Explain the concrete problem, resulting behavior, tests, and limitations in the
pull request. Include documentation updates for changed commands, configuration,
compatibility, or operational behavior. Keep changes small enough to review and
diagnose, and identify checks that were not run.

Before distributing binaries, preserve the licenses and notices described in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), record linked dependencies, and
verify a clean build. Local test success does not imply a published release or
hosted CI run. Publication and deployment are separate maintainer actions.
