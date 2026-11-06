# Contributing to vmtier

vmtier is an independent Apache-2.0 project. Start with the [README](README.md),
[MVP architecture](docs/mvp-design.md), and [first-release design](docs/design.md).
The design distinguishes implemented behavior from the next implementation phase.

## Language and scope

Write repository documentation, code comments, commit/PR descriptions, and
user-facing diagnostics in English. Test fixtures may contain other languages
when exercising Unicode behavior. Preserve upstream license text and attribution.

Prefer a small change that solves a demonstrated problem. Record material format,
protocol, or operational decisions in an ADR. Explain the workload or failure that
requires additional machinery; avoid speculative HA, storage abstractions, and
configuration switches without a concrete use case.

## Development and validation

Use the pinned source and bootstrap instructions in the README. Do not edit
generated `vendor/` or `.deps/` files to make a test pass. Keep stock VictoriaMetrics
binaries unmodified and record the revision when evaluating an upgrade.

For runtime changes, run relevant unit tests and the repository checks:

```sh
make build test race vet
```

Run stock-VM differential suites for changes to archive contents, publication,
query results, RPC, or S3 behavior. [Testing](docs/testing.md) describes their setup
and assertions. Test observable behavior and meaningful failure boundaries rather
than mirroring private implementation details. For documentation-only changes,
check links, commands against current behavior, and planned versus measured claims;
performance benchmarks need not be rerun.

Preserve recorded result JSON and provenance. Translation or design work does not
turn an earlier run into a new result. Store new measurements separately until the
report is deliberately updated. A local S3 test double is not a live provider test;
a recent-only Hot reconstruction is not a natural retention-expiry test.

## Pull requests and releases

Explain the concrete problem, resulting behavior, tests, and limitations. Keep
runtime changes small enough to review and diagnose. Examples must not contain
credentials or assume access to a contributor's environment. Real-provider tests
require an explicitly configured disposable target.

Before distributing binaries, preserve the licenses and notices described in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), record linked dependencies, and
verify a clean build. Local test success does not imply a published release or
hosted CI run. Publication and deployment are separate maintainer actions.
