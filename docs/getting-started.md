# Getting started

Use the SQLite `serve` runtime for the current implementation. All commands below
run from the repository root. For supported behavior and outstanding qualification,
read [limits](limits.md) and [compatibility](compatibility.md).

## Build from source

Use Linux amd64, Go 1.27.3 and Python 3.15.2 (the pinned toolchains),
a C compiler with CGO enabled, make, curl, tar, and sha256sum. Allow enough disk space for Go module/build caches and disposable test data. Docker is not required.

```sh
./scripts/bootstrap.sh
make build test
.bin/vmtier version
```

Bootstrap downloads checksum-pinned stock VictoriaMetrics sources into `.deps/`,
then runs `go mod download`, `go mod verify`, and `go mod vendor` using committed
module versions. It regenerates `vendor/`; stock VM builds retain their own upstream
vendor tree. No VM source is patched. Do not edit generated dependencies.
SQLite is bundled through CGO; no separate SQLite server is required. If Go is
outside `PATH`, use `make GO=/path/to/go bootstrap build test stock`.

## Run a disposable demo

Build the pinned stock VM binaries and run the SQLite local-backend harness:

```sh
make stock
python3 scripts/e2e_serve.py --backend local --workdir .cache/demo-serve-local
```

The harness starts isolated VM processes, ingests synthetic data, archives it,
reconstructs Hot with recent samples only, and compares Cold/Hot/boundary queries
with a full-history reference. It stops its processes when finished and retains
logs and `results.json`. The work directory must not already exist, and the host
must permit local TCP listeners. This is an automated demonstration, not a
long-running deployment or a natural retention-expiry test.

For S3 API testing using a disposable Moto backend:

```sh
python3 -m venv .venv-e2e
. .venv-e2e/bin/activate
python -m pip install -r scripts/requirements-e2e.txt
python scripts/e2e_serve.py --backend s3 --replicas 2 --workdir .cache/demo-serve-s3
```

These tests do not connect to an existing VM or real S3 bucket. See
[testing](testing.md) for regression suites and interpretation of results.

## Configure an existing Hot cluster

1. Copy [examples/serve-sakura.json](../examples/serve-sakura.json) to `serve.json`.
   Set the endpoint, region, existing bucket, prefix, tenants, and Hot-only
   vmselect source URLs. Do not export from a combined Hot/Cold frontend.
2. Choose archive age H, enforced lateness L, Hot retention R, safety margin, and
   window width such that `H > L >= 0` and `R > H + safety_margin + window`.
   Durations use Go syntax (`720h`, not `30d`). Set a positive, window-aligned
   `coverage_start_ms` within retained source history. The example intentionally
   uses zero and `lateness_enforced=false`; set the latter to true only after all
   write paths enforce the lateness bound.
3. Run [capacity preflight](preflight.md) on representative busy windows before
   catalog initialization fixes the contract. It checks source completeness and
   pipeline bounds without publishing data.
4. Choose a durable local catalog path and unique ID. Keep the path and ID in the
   configuration identical to the initialization arguments below. Create the
   durable parent directory first.

Supply S3 credentials through `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
optionally `AWS_SESSION_TOKEN`, never through JSON. Actual Sakura connectivity
and performance still require deployment-specific validation.

With the edited configuration and the example path/ID:

```sh
mkdir -p .state
.bin/vmtier catalog-init -backend sqlite -config serve.json \
  -path .state/catalog-sqlite -id sakura-serve-001
.bin/vmtier serve -config serve.json \
  -listen 127.0.0.1:8489 -http 127.0.0.1:8490
```

The service refuses missing state and starts catch-up immediately after opening
initialized state. Keep it running and check from another terminal:

```sh
curl --fail http://127.0.0.1:8490/health
curl --fail http://127.0.0.1:8490/ready
curl --fail http://127.0.0.1:8490/metrics
```

Readiness can return 503 while required windows are catching up. It checks catalog
and coverage, not every object or the external ingestion contract.

## Connect the query frontend

For a Hot vmstorage read listener at `127.0.0.1:8401` and the gateway above:

```sh
.bin/vmselect -httpListenAddr=127.0.0.1:8482 \
  -storageNode=127.0.0.1:8401,127.0.0.1:8489 \
  -replicationFactor=1 -globalReplicationFactor=1 \
  -search.skipSlowReplicas=false -search.denyPartialResponse=true \
  -search.disableCache -dedup.minScrapeInterval=1ms
```

Replace the Hot address with all required Hot storage nodes. Configure 1 ms
deduplication consistently on storage and vmselect. Hot and Cold are complementary,
so replica skipping or partial responses can hide missing data. Keep listeners on
a trusted network and block mutation APIs at the HTTP access-control layer.
Compare Cold-only and boundary queries before changing any retention settings.

Continue with [service scheduling and resource limits](serve.md),
[operations and recovery](operations.md), and [offline catalog maintenance](sqlite-catalog.md).
