# AGENTS.md

Guide for coding agents and contributors working in this repository. Human
contributors should also read [CONTRIBUTING.md](CONTRIBUTING.md); user-facing
documentation starts at [README.md](README.md) and [docs/README.md](docs/README.md).

## 1. Overview and architecture

vmtier archives older VictoriaMetrics (VM) cluster samples to S3-compatible
storage and serves Hot and Cold data through an **unmodified stock vmselect**.
`vmtier serve` combines three parts in one process
([architecture](docs/architecture.md)):

- **Archive scheduler** (`internal/archive`): per tenant/source stream, exports
  the oldest eligible window from a Hot-only stock vmselect as native blocks,
  rotates bounded packs (`internal/pack`), verifies the samples against an
  independent raw JSON oracle and count queries, uploads each pack under a fresh
  attempt key, reads every object back, and only then publishes the window in
  one catalog transaction.
- **Catalog** (`internal/catalog`): the authority for coverage. SQLite (one
  owner, directory lock) or PostgreSQL (several read gateways, one archiver with
  a DB-time lease and fencing token). It cannot be rebuilt from an object
  listing.
- **Cold read gateway** (`internal/gateway`, `internal/cold`): implements the
  pinned `vmselectapi.API` storage RPCs. It checks coverage, selects bounded pack
  references, reads indexes and byte ranges through an immutable verified LRU
  cache, decodes and clips samples, and rebuilds wire blocks. VM evaluates
  MetricsQL.

Data flow: stock vminsert -> Hot vmstorage -> (export) -> archiver -> S3 packs
plus catalog rows -> gateway -> stock vmselect. The retained JSON v2 runtime
(`archive`, `gateway`, `catalog` commands) exists for regression tests and
offline migration only ([legacy](docs/legacy-json-v2.md)).

## 2. Directory layout

| Path | Contents |
| --- | --- |
| `cmd/vmtier` | CLI dispatch, `serve` lifecycle and archive loop, health/metrics HTTP, offline commands |
| `internal/config` | Strict JSON configuration, policy validation, effective limits, client construction |
| `internal/archive` | Export, oracle verification, pack staging/upload/read-back, publication, preflight |
| `internal/catalog` | SQLite/PostgreSQL repositories, leases, maintenance, JSON v2 authority |
| `internal/pack` | Pack/index format v1, portable block decoder, rotation, VM codec pin |
| `internal/cold` | Coverage checks, plan selection, cache, range reads, query work accounting |
| `internal/gateway` | vmselect RPC adapter, matcher, admission and result limits |
| `internal/objectstore` | Local and S3 `BlobStore`, shared S3 request budget, I/O counters |
| `internal/querylimit` | Query refusal errors and their stable reasons |
| `scripts/` | Bootstrap, stock VM build, Python differential/recovery/HA suites, release tooling, and their stdlib unit tests |
| `release/` | Pinned release builder (`Dockerfile`, `build.sh`), reviewed license record `licenses.json` |
| `docs/` | Guides, reference, design, ADRs, and recorded evidence under `docs/results/` |
| `examples/` | Configuration templates, intentionally invalid until policy values are set |
| `vendor/`, `.deps/`, `.bin/`, `.cache/` | Generated and gitignored: never edit or commit |

## 3. Build

Requirements: Go 1.27.3 (go.mod), CGO with a C compiler (SQLite), Python 3.15.2
for the harnesses (CI version), curl, standard Linux build tools.

```sh
make bootstrap   # verify go.sum, regenerate vendor/, fetch the pinned VM source into .deps/
make build       # .bin/vmtier (-mod=vendor)
make stock       # unmodified stock vmstorage/vminsert/vmselect into .bin/ (e2e only)
```

Every Go target uses `-mod=vendor` and `-p $(GOMAXPROCS)` (default 3); the
Makefile also sets `GOCACHE` to `.cache/go-build`. `bootstrap` downloads a
checksum-pinned VM source archive from codeload.github.com; where that host is
unreachable, `go mod download && go mod verify && go mod vendor` produces the
vendor tree that lint and tests need (not `make stock`).

## 4. Test

| Command | What | Needs |
| --- | --- | --- |
| `make test` | Go unit tests | bootstrap |
| `make test-race` (= `make race`) | Go unit tests with `-race` | bootstrap |
| `make test-scripts` | Python harness and release-tool unit tests (stdlib only) | Python |
| `make test-fuzz` | Every `FuzzXxx` target for `FUZZTIME` (default 30s) each | bootstrap |
| `make test-integration` | PostgreSQL repository and CLI tests under `-race` | `VMTIER_TEST_POSTGRES_DSN` (disposable PostgreSQL 17 database) |
| `make bench` | Benchmarks to `bench.txt` (`BENCH_COUNT`, `BENCHTIME`); compare with benchstat | bootstrap |
| `python3 scripts/e2e_serve.py ...`, `e2e.py`, `e2e_recovery.py`, `e2e_postgres.py` | Stock-VM differential, recovery, HA and migration suites | `make build stock`, Moto (`scripts/requirements-e2e.txt`), HAProxy/psql for PostgreSQL |
| `python3 scripts/postgres_ha.py --postgres-bin DIR --race ...` | Synchronous failover and transport faults | PostgreSQL 17 binaries |

PostgreSQL tests skip without `VMTIER_TEST_POSTGRES_DSN`; `make test-integration`
refuses to run without it so that a skip is never mistaken for a pass. The S3
store test against a real endpoint needs `VMTIER_TEST_S3_*`. Docs:
[testing](docs/testing.md) gives exact commands, fresh `--workdir` rules and the
recorded evidence; e2e suites are long (tens of minutes) and need fresh work
directories.

Test conventions: packages that start goroutines run `goleak.VerifyTestMain`
(`leak_test.go`); fault injection uses `objectstore.BlobStore` and
`catalog.Store`/`Repository` wrappers and `httptest` sources (see
`internal/archive/durability_test.go`); fuzz targets assert properties
(round trips, differential agreement with the pinned VM codec, key
confinement), not only absence of panics. Keep a fuzz crasher under
`testdata/fuzz/FuzzXxx/` as a regression seed.

## 5. Static analysis

| Command | What |
| --- | --- |
| `make lint` | golangci-lint v2.15.0 with `.golangci.yml` (reads `vendor/`) and `go mod tidy -diff` |
| `make fmt` | `golangci-lint fmt` (gofmt) |
| `make vet` | `go vet` |
| `make lint-actions` | actionlint and zizmor over `.github/` (offline unless `GH_TOKEN` is set) |
| `make vuln` (= `make vulncheck`) | govulncheck on reachable code |
| `cccc --table --top-cognitive 20 cmd internal` | Cognitive Complexity; CI fails above 30 (`cccc.toml`) |

CI also runs CodeQL (Go, Python) in `security.yml`. Tool versions are pinned in
the Makefile and workflows; bump them together.

## 6. Coding conventions

- English for code, comments, docs, commit and PR text, and diagnostics.
- Comments explain why; keep the existing density. Exported identifiers carry
  doc comments that state the contract.
- Logging: `log/slog` with key/value attributes (`tenant`, `source`, `start_ms`,
  ...). Never log credentials, DSNs or tokens.
- Errors: wrap with `fmt.Errorf("...: %w", err)`; use the sentinels
  (`catalog.ErrConflict`, `ErrInvalidPublication`, `ErrLeaseLost`,
  `objectstore.ErrNotFound`, `ErrKeyReuse`, ...) and `querylimit.Errorf` for
  refusals. Missing, corrupt or timed-out Cold data is an error, never an empty
  result.
- Configuration is strict JSON (unknown fields and trailing data rejected). A
  new setting needs validation, docs and example updates
  ([CONTRIBUTING](CONTRIBUTING.md#documentation-changes)).
- Commit subjects are short imperative sentences without a type prefix
  (`Split archiver pass setup from its stream loop`); the body explains why.
- Record material format, protocol or operational decisions in an ADR
  (`docs/adr/`).

## 7. Compatibility contracts

These must not change silently; call out any change in the PR.

- **VM pin.** Codecs and read RPCs target VictoriaMetrics OSS
  `v1.155.0-cluster`, commit `cab3d5fb...` only: `go.mod`, `pack.VMVersion` and
  `pack.VMRevision`, `scripts/vm-source.sh`. Dependabot ignores the VM module.
  An upgrade follows [compatibility](docs/compatibility.md#codec-and-upgrades):
  new captured fixtures in `internal/pack/testdata/`, malformed-input tests, and
  differential e2e runs. Never relabel old packs or regenerate an old fixture to
  make a new decoder pass.
- **Pack format v1** (`internal/pack`): immutable payload (concatenated VM
  portable blocks) plus a JSON index that records format/VM version, tenant,
  half-open logical window, size, SHA-256, series dictionary with the VM-ordered
  `native_name`, and block directory. Every pack byte belongs to exactly one
  block. Labels must be valid UTF-8 (the JSON index cannot carry other bytes).
- **Catalog**: SQLite schema 1, PostgreSQL schema 2, JSON v2. The contract
  (coverage origin, window, age, lateness, 1 ms dedup, tenants, sources) and the
  blob locator are fixed at initialization; published windows are append-only
  and immutable; restore and migration use a new catalog ID.
- **Object keys**: `tenants/<account:project>/attempts/<random>/<index sha>.pack|.json`,
  never reused for different bytes, validated by `validObjectKey`/`validKey`
  (no `..`, hidden components or escaping the tenant namespace).
- **Public surface**: CLI commands and flags (`cmd/vmtier`,
  [commands](docs/commands.md)); configuration keys (`internal/config`);
  `vmtier_*` metric names and labels (`cmd/vmtier/serve.go`,
  `capacity_metrics.go`, `main.go`); `querylimit` reasons; supported and
  explicitly rejected RPCs ([compatibility](docs/compatibility.md#rpc-and-http-support));
  `/health`, `/ready`, `/metrics`; the JSON v2 catalog HTTP API; release asset
  names and contents ([releases](docs/releasing.md)).

## 8. Concurrency, durability and resource management

- **Publication order is the durability contract**: export -> verify (oracle and
  counts) -> PUT -> read back and compare bytes -> one catalog transaction. Commit
  is the only visibility point. Object I/O never runs inside a database
  transaction. A failed or interrupted attempt leaves at most unreferenced
  orphans (GC is not implemented) and never advances coverage.
- **Idempotence**: replaying a committed window with the same publication digest
  succeeds without new rows; a different digest is an error. After an ambiguous
  commit, reload coverage before retrying. Retries use fresh attempt keys.
- **Ownership**: SQLite has one process owner (directory flock); PostgreSQL
  checks the lease and fencing token inside each publication transaction, and
  `ErrLeaseLost` stops the archive loop.
- **Contexts**: every RPC, archive window and S3 request is bounded by a context
  deadline (`service.archive_timeout`, gateway `MaxRequestDuration`, S3
  timeout). Service shutdown (`runServe`) cancels the root context, waits for
  the archive loop, shuts down HTTP, stops the RPC server, then closes the blob
  store and catalog. A goroutine is owned by the component that starts it and
  must end with its context.
- **Budgets**: all S3 attempts (including SDK retries) share one request budget
  per process; query work is reserved before backend work and released with the
  iterator. Close response bodies: a budget permit lives until `Close`.
- **Caches** hold immutable, checksum-verified bytes only; catalog availability
  is checked on every query even with a warm cache.

## 9. Performance constraints

- Hot paths: portable decode and wire rebuild (`pack.LoadBlock`), index parsing,
  matcher evaluation per series/block, range coalescing, raw oracle streaming.
  Benchmarks: `internal/pack`, `internal/archive`, `internal/gateway`.
- Memory is bounded by explicit limits (pack/index/input bytes, series, blocks,
  samples, plan size, query work) checked before allocation; the oracle is
  decoded incrementally. These are logical allowances, not RSS ceilings
  ([memory accounting](docs/query-memory.md)).
- Metric labels are bounded by configured tenants and sources; do not add
  per-series, per-key or per-request labels.

## 10. Before opening a PR

```sh
make bootstrap   # once per checkout or go.mod change
make check       # lint, lint-actions, test-race, test-scripts
```

Also, depending on the change:

- Catalog repositories, PostgreSQL or migration: `make test-integration`.
- Archive contents, publication, query results, RPC or S3 behaviour: the
  stock-VM differential suites (`make build stock`, then `scripts/e2e_serve.py`
  and friends), plus the recovery or migration suite for those paths.
- Decoders, key handling or matchers: `make test-fuzz` (longer `FUZZTIME` for
  substantive changes).
- Release tooling or new linked modules: `make test-scripts`, the CI
  `release-check` job, `release/licenses.json` and `THIRD_PARTY_NOTICES.md`.
- Commands, defaults or metrics: `docs/`, `examples/` and the smoke test in
  `scripts/release.py`.

## 11. Rules for AI agents

Forbidden:

- Disabling, skipping, deleting or weakening tests, fuzz properties, lint rules
  or CI checks to get green; hiding failures (`|| true`, `continue-on-error`,
  blanket excludes). A `//nolint` needs the linter name and a reason.
- Editing generated or vendored files by hand (`vendor/`, `.deps/`), or patching
  stock VictoriaMetrics.
- Changing the VM pin, pack/catalog formats, object key layout, or any contract
  in section 7 without calling it out and following the upgrade procedure.
- Rewriting recorded evidence under `docs/results/` or presenting local or
  test-double runs as provider or production qualification.
- Adding dependencies without justification (test-only where possible; linked
  modules need a license review in `release/licenses.json`).
- Loosening workflow permissions, unpinning actions, adding caches to release
  jobs, or adding `pull_request_target`.
- Committing secrets, credentials or private metric data; rewriting history or
  force-pushing; tagging or publishing releases.

## 12. Changes that need human review

Even with green CI: `.github/` workflows and `dependabot.yml`; `release/`,
`scripts/release.py` and license records; permissions, tokens and credential
handling; the VM pin and anything in `internal/pack` (decoders of untrusted
bytes, format); catalog schemas, migrations, publication and lease/fencing
logic; object key construction; the S3 client and request budget; public
contracts in section 7; new dependencies; large refactors.
