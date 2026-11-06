# Getting started

This guide builds vmtier and exercises the current SQLite `serve` runtime against
disposable, unmodified VictoriaMetrics processes. For your own cluster, continue
with the [service guide](serve.md). The [documentation index](README.md) links to
architecture, development, and migration references.

## Prerequisites

The tested environment is Linux amd64. Install:

- Go **1.27.3**, the version used by the checked-in CI workflow and recorded tests.
  The module declares a minimum of **1.25.3**; other toolchains have not received
  the same validation.
- Python **3.12 or newer** with `venv` and `pip` for the S3 demo. Python is also
  used to assemble the vendored Go dependencies during bootstrap.
- A C compiler/toolchain: the SQLite driver and stock VM build require CGO.
- Git, Bash, Make, curl, tar, and `sha256sum`.
- Several GB of free disk space for source archives, build caches, binaries, and
  demo data. The demos need permission to bind loopback TCP listeners.

Docker and access to a real S3 account are not needed for these demos. Bootstrap
requires access to GitHub source archives and the Go module proxy; installing the
S3 test dependencies also requires access to Python package repositories.

## Build from source

```sh
git clone https://github.com/yuuki/vmtier.git
cd vmtier
make bootstrap build
.bin/vmtier version
```

Run subsequent commands from the repository root. If Go is outside `PATH`, use
`make GO=/path/to/go build` and the same override for other Make targets.

[bootstrap.sh](../scripts/bootstrap.sh) downloads checksum-pinned VictoriaMetrics
sources and the SQLite driver, then generates `.deps/` and `vendor/`. The driver
uses bundled SQLite; installing a separate SQLite server is unnecessary.
`make build` creates `.bin/vmtier` with vendored dependencies. An ordinary
`go install ...@latest` is not the documented build path for this source layout.
Do not edit generated dependencies or patch stock VM to make a test pass.

## Run the local SQLite demo

Build the stock `vmstorage`, `vminsert`, and `vmselect` binaries, then run the
service integration harness:

```sh
make stock
python3 scripts/e2e_serve.py --backend local --workdir .cache/demo-serve-local
```

The harness creates its own configuration, local object directory, SQLite catalog,
Hot cluster, reference cluster, and query frontend. It ingests a synthetic dataset,
archives eligible windows, and compares exact results for Cold, Hot, and boundary
queries. It also checks metadata, ownership, restart, recovery, and failures.

On success, `.cache/demo-serve-local/results.json` records `"status": "passed"`
and 360 exact query comparisons, plus scenario checks. Logs and fixture state stay
in the work directory for inspection; the harness stops its processes on exit.
Use a new or empty work directory on each run. Omitting `--workdir` creates a
new temporary directory automatically. This is a test run, not a persistent demo
server, and it does not use an existing production cluster.

The fixture replaces Hot with a recent-only dataset to prove that older queries
use Cold. It does not wait for natural Hot retention expiry. See
[Testing](testing.md) for assertion details and additional checks.

## Exercise S3 requests locally

Install the pinned Moto test backend in an isolated Python environment:

```sh
python3 -m venv .deps/e2e-venv
. .deps/e2e-venv/bin/activate
python -m pip install -r scripts/requirements-e2e.txt
python scripts/e2e_serve.py --backend s3 --replicas 2 --workdir .cache/demo-serve-s3
```

Keep that environment active when running S3 suites so `moto_server` is on `PATH`.
The harness starts Moto behind a compatibility proxy that rejects conditional
requests and strips ETags. It uses disposable test credentials and a local bucket;
no cloud credentials are required. The S3 run also records 360 exact comparisons
on success. These results test the request path, not a real provider's authentication,
durability, quotas, or performance.

## Connect an existing cluster

Use the [service guide](serve.md) to prepare a full configuration, initialize a
SQLite catalog, start `serve`, and register its RPC listener with a stock vmselect.
The [example guide](../examples/README.md) identifies current and legacy configs.
Before initializing state, establish:

1. Fixed tenants and Hot-only export sources covering all required Hot shards.
2. An externally enforced maximum write lateness and sufficient Hot retention for
   archival, retries, and recovery.
3. A positive, window-aligned coverage origin whose data is still available in Hot.
4. Persistent local catalog storage and an object store that permits immediate reads.

The provided deployment configs intentionally fail validation until you choose
`coverage_start_ms` and can truthfully set `lateness_enforced=true`. They are
starting points, not ready-to-run production configurations. Preflight busy windows
with the read-only [capacity preflight command](preflight.md) before binding the
catalog's immutable policy. Preflight checks source completeness and archive limits;
it does not measure uploads or concurrent query capacity. Use the
[memory reference](query-memory.md) and [scoped workload measurements](workload-validation.md)
to plan separate query and host-capacity checks.

For development checks, continue with [Contributing](../CONTRIBUTING.md). For an
existing JSON v2 archive, use [offline migration](sqlite-catalog.md) rather than
starting a second writer on the same objects.
