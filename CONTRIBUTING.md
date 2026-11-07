# Contributing to vmtier

vmtier is an independent Apache-2.0 project. Start with the [README](README.md),
[architecture and source map](docs/architecture.md), and [documentation index](docs/README.md).
The [first-release design](docs/design.md) distinguishes implementation from remaining qualification.

## Language and scope

Write repository documentation, code comments, commit/PR descriptions, and
user-facing diagnostics in English. Test fixtures may contain other languages
when exercising Unicode behavior. Preserve upstream license text and attribution.

Prefer a small change that solves a demonstrated problem. Record material format,
protocol, or operational decisions in an ADR. Explain the workload or failure that
requires additional machinery; avoid speculative HA, storage abstractions, and
configuration switches without a concrete use case.

## Development and validation

Use the pinned source and [build instructions](docs/getting-started.md#build-from-source). Do not edit
generated `vendor/` or `.deps/` files to make a test pass. Keep stock VictoriaMetrics
binaries unmodified and record the revision when evaluating an upgrade.

For runtime changes, run relevant unit tests and the repository checks:

```sh
make build test race vet vulncheck
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

Distribute binaries only from the [release workflow](docs/releasing.md). It builds
a tag twice in the pinned builder and requires identical checksums. It also
qualifies the packaged binary, attaches licenses, notices, an SBOM, and signed
provenance, and saves a draft release. The CI `release-check` job fails when a
change links a new module, drops one, or changes a module's license files, until
`release/licenses.json` records the reviewed license and digest. Update
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) by hand in the same pull request;
no check reads it. Local test success does not imply a published
release or hosted CI run. Publication and deployment are separate maintainer actions.

## Documentation changes

Keep the README focused on project purpose, status, and entry points. Put setup
steps in `docs/getting-started.md`, current behavior in the service/reference guides,
operational procedures in `docs/operations.md`, and legacy-only procedures in
`docs/legacy-json-v2.md`. Add new pages to `docs/README.md`.

Keep design proposals separate from implemented behavior. Add measured reports to
`docs/evidence.md`; preserve existing result bytes, paths, revisions, and caveats.
When changing commands or defaults, check `cmd/vmtier`, `internal/config`, examples,
CI, and the release smoke test in `scripts/release.py` together. Check relative links and heading anchors after moving content.
