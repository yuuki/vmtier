# AGENTS.md

This file holds only what the code does not show. Design decisions are in
[docs/design.md](docs/design.md).

## Rules the code does not show

- Publication order is the durability contract: export, verify (raw oracle
  and counts), PUT, read back and compare bytes, then one catalog
  transaction, the only visibility point. Never run object I/O inside a
  database transaction.
- A failed attempt leaves at most unreferenced orphans (no GC) and never
  advances coverage. Retries use fresh attempt keys; after an ambiguous
  commit, reload coverage first. Replaying a committed window with the same
  publication digest adds no rows; a different digest is an error.
- Published windows are append-only and immutable; restore and migration use
  a new catalog ID. The catalog cannot be rebuilt from an object listing. An
  object key is never reused for different bytes.
- SQLite has one owner process (directory flock); PostgreSQL checks the
  lease and fencing token inside each publication transaction, and
  `ErrLeaseLost` stops the archive loop.
- Missing, corrupt or timed-out Cold data is an error, never an empty
  result. Caches hold only immutable, checksum-verified bytes; check catalog
  availability on every query, even with a warm cache.
- Bound every RPC, archive window and S3 request by a context deadline.
- All S3 attempts, SDK retries included, share one per-process request
  budget; a permit lives until the response body is closed.
- Reserve query work before backend work; release it with the iterator.
- Check the explicit limits before allocating; decode the oracle
  incrementally.
- Metric labels are bounded by the configured tenants and sources: no
  per-series, per-key or per-request labels.
- Never log credentials, DSNs or tokens.
- A new configuration setting needs validation, docs and an `examples/`
  update.
- Record material format, protocol or operational decisions in an ADR
  (`docs/adr/`).
- Fuzz targets assert properties, not only the absence of panics.
- Bump pinned tool versions in the `Makefile` and the workflows together.
- English for code, comments, docs, commits, pull requests and diagnostics.

## Compatibility contracts

Call any change out in the pull request:

- VM pin (VictoriaMetrics OSS `v1.155.0-cluster`): `go.mod`,
  `pack.VMVersion`/`pack.VMRevision` and `scripts/vm-source.sh` change
  together; upgrade only as `docs/compatibility.md` says. Never relabel old
  packs or regenerate an old fixture to make a new decoder pass.
- Pack and index format v1: `internal/pack` (labels must be valid UTF-8:
  the JSON index cannot carry other bytes).
- Catalog schemas (SQLite 1, PostgreSQL 2, JSON v2), with the contract and
  blob locator fixed at initialization: `internal/catalog`.
- Object key layout: `internal/archive/pipeline.go`, validated by
  `validObjectKey` and `validKey`.
- Public surface: CLI commands, flags and `vmtier_*` metrics
  (`cmd/vmtier`), configuration keys (`internal/config`), refusal reasons
  (`internal/querylimit`), supported and rejected RPCs (`internal/gateway`),
  `/health`, `/ready`, `/metrics`, the JSON v2 catalog HTTP API
  (`internal/catalog/http.go`), release assets (`release/`).

## Before opening a pull request

- `make bootstrap` (once per checkout or `go.mod` change), then `make check`.
- Catalog repositories, PostgreSQL or migration: also `make test-integration`.
- Archive contents, publication, query results, RPC or S3 behaviour:
  `make build stock`, then the stock-VM differential suites
  (`scripts/e2e_serve.py`, `scripts/e2e.py`), plus `scripts/e2e_recovery.py`
  or `scripts/e2e_postgres.py` for recovery or migration paths.
- Decoders, key handling or matchers: also `make test-fuzz`.
- Release tooling or new linked modules: also `make test-scripts` and the CI
  `release-check` job; update `release/licenses.json` and
  `THIRD_PARTY_NOTICES.md`.
- Commands, defaults or metrics: update `docs/`, `examples/` and the smoke
  test in `scripts/release.py`.

## Git conventions

Branches: `<type>/<issue>-<summary>`, or `<type>/<summary>` without an issue
(`fix/166-connect-errors`, `feat/qp-counters`). `<type>` is a commit type
below; `<summary>` is 2–5 lowercase ASCII words joined by `-`. Branches named
by tools keep their names (`agent/*`, `claude/*`, `copilot/*`,
`dependabot/*`). Never push to the default branch.

Commits follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/):

    <type>(<scope>)!: <subject>

    <body>

    <footer>

- `type`: `feat` (new user-visible behaviour), `fix` (bug fix), `perf`,
  `refactor` (no behaviour change), `test`, `docs`, `build` (build system,
  dependencies, packaging), `ci` (workflows), `chore` (anything else, e.g.
  `chore(release): prepare v1.2.3`), `revert`.
- `scope`: optional; the package or area changed: `archive`, `catalog`,
  `pack`, `cold`, `gateway`, `objectstore`, `config`, `release`.
- `!` and a `BREAKING CHANGE: <what changed and how to migrate>` footer for
  any change to a compatibility contract above.
- `subject`: imperative, lower case (except identifiers), no final period,
  at most 72 characters including the prefix.
- `body`: why the change is needed and what it affects, wrapped at 72
  columns; required unless the subject says everything.
- `footer`: `Fixes #N` / `Refs #N`, `BREAKING CHANGE:`, trailers such as
  `Co-Authored-By:`.
- One logical change per commit; regenerated files go in the commit that
  changes their source. Pull request titles use the same format (they become
  the squash commit).

## Forbidden

- Disabling, skipping, deleting or weakening tests, fuzz properties, lint
  rules or CI checks to get green; hiding failures (`|| true`,
  `continue-on-error`, blanket excludes); a `//nolint` without the linter
  name and a reason.
- Editing `vendor/` or `.deps/` by hand, or patching stock VictoriaMetrics.
- Changing a compatibility contract without calling it out and following
  the upgrade procedure.
- Rewriting recorded evidence under `docs/results/`, or presenting local or
  test-double runs as provider or production qualification.
- Adding dependencies without justification (test-only where possible;
  linked modules need a license review in `release/licenses.json`).
- Loosening workflow permissions, unpinning actions, adding caches to
  release jobs, or adding `pull_request_target`.
- Committing secrets, credentials or private metric data; rewriting history
  or force-pushing; tagging or publishing releases.

## Needs human review

Even with green CI: `.github/` workflows and `dependabot.yml`; `release/`,
`scripts/release.py` and license records; permissions, tokens and
credential handling; the VM pin and anything in `internal/pack`; catalog
schemas, migrations, publication and lease/fencing logic; object key
construction; the S3 client and request budget; the compatibility
contracts; new dependencies; large refactors.

## Environment notes

- `make bootstrap` fetches the VM source from codeload.github.com. Where
  that host is unreachable, `go mod download && go mod verify && go mod
  vendor` gives the vendor tree that lint and tests need (not `make stock`).
- With `GH_TOKEN` set but the GitHub API unreachable, run
  `env -u GH_TOKEN make check`.
