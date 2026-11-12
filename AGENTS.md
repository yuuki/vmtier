# AGENTS.md

## Rules
- Durability contract: export, verify (oracle and counts), PUT, read back and compare bytes, then one catalog transaction, the only visibility point.
- Never run object I/O inside a database transaction; a failed attempt leaves at most unreferenced orphans and never advances coverage.
- Record every object key in the catalog's ledger before its PUT; publication consumes the records, and garbage collection deletes only keys it claimed from the ledger, never listed or published ones.
- Retries use fresh attempt keys; after an ambiguous commit, reload coverage first; replaying a window with another publication digest is an error.
- Published windows are append-only and immutable; restore and migration use a new catalog ID; the catalog cannot be rebuilt from an object listing.
- SQLite has one owner process (directory flock); PostgreSQL checks the lease and fencing token inside each publication transaction.
- Missing, corrupt or timed-out Cold data is an error, never an empty result; check catalog availability on every query, even with a warm cache.
- Caches hold only immutable, checksum-verified bytes; all S3 attempts (SDK retries too) share one per-process budget, a permit held until the body closes.
- Query work is reserved before backend work and released with the iterator.
- Never patch stock VictoriaMetrics: vmtier serves through an unmodified stock vmselect.
- Record material format, protocol or operational decisions in an ADR (`docs/adr/`).

## Compatibility contracts
- VM pin `v1.155.0-cluster`: `go.mod`, `pack.VMVersion`/`pack.VMRevision`, `scripts/vm-source.sh` change together; upgrade as `docs/compatibility.md` says.
- Never relabel old packs or regenerate an old fixture to make a new decoder pass; pack and index format v1: `internal/pack`.
- Catalog schemas (SQLite 2, PostgreSQL 3, JSON v2): `internal/catalog`; object keys: `internal/archive/pipeline.go`, `validObjectKey`, `validKey`.
- CLI commands, flags and `vmtier_*` metrics (`cmd/vmtier`); configuration keys (`internal/config`); refusal reasons (`internal/querylimit`).
- Supported and rejected RPCs (`internal/gateway`); `/health`, `/ready`, `/metrics`; JSON v2 catalog HTTP API (`internal/catalog/http.go`); `release/` assets.

## Before opening a pull request
- `make bootstrap` (once per checkout or `go.mod` change), `make check`; also catalog, PostgreSQL or migration → `make test-integration`.
- Archive contents, publication, query results, RPC or S3 → `make build stock`, then the stock-VM differential, recovery or migration suites (`scripts/e2e*.py`).
- Decoders, key handling or matchers → `make test-fuzz`; commands, defaults or metrics → `docs/`, `examples/`, the smoke test in `scripts/release.py`.
- Leases, the upload, the publication transaction or garbage collection → update `spec/VmtierPublication.tla` and `make test-tla` (Java 11+); the timing contract (`validateTime`, `RequiredEnd`, the gateway's coverage check) → `spec/lean` and `make test-lean`.
- Release tooling or new linked modules → `make test-scripts`, the CI `release-check` job, `release/licenses.json`, `THIRD_PARTY_NOTICES.md`.

## Git conventions
- Branches: `<type>/<issue>-<summary>` (`<type>/<summary>` without an issue); tools' `agent/*`, `claude/*`, `copilot/*`, `dependabot/*` keep their names.
- Commits and PR titles: Conventional Commits 1.0.0, types `feat` `fix` `perf` `refactor` `test` `docs` `build` `ci` `chore` `revert`; scopes: `archive`, `catalog`, `pack`, `cold`, `gateway`, `objectstore`, `config`, `release`.
