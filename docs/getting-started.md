# Getting started

Choose SQLite for a single-owner local catalog or PostgreSQL for multiple
gateways and fenced archiver failover. The walkthrough below uses SQLite; follow
the [PostgreSQL setup](postgres-catalog.md) and [HA deployment](ha-deployment.md)
for the external-catalog alternative. All commands run from the repository root. For supported behavior and outstanding qualification,
read [limits](limits.md) and [compatibility](compatibility.md).

## Build from source

Use Linux amd64, Go 1.27.4 and Python 3.15.2 (the pinned toolchains),
a C compiler with CGO enabled, make, curl, tar, and sha256sum. Allow enough disk
space for Go module/build caches and disposable test data. Bootstrap needs network
access to GitHub source archives and the Go module proxy; S3 test setup also needs
Python package repositories. Docker and a real S3 account are not required for
the disposable demos; Docker is needed only to build [release archives](releasing.md).

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

## Install a release archive

A release archive holds a static `linux/amd64` binary that needs no Go, C
compiler, or shared libraries. Download every asset of one release into an empty
directory, [verify them](releasing.md#verify-a-release-archive), and read that
release's [support scope](releasing.md#support-scope); a pre-release such as
`v0.1.0-rc.2` is not production-supported. Stock VictoriaMetrics, S3, and the
query frontend come from your existing deployment.

As root, install the binary, a service user, the configuration templates, and
the [example systemd unit](../examples/systemd/vmtier.service) from the archive:

```sh
VERSION=v0.1.0-rc.2
tar -xzf vmtier-$VERSION-linux-amd64.tar.gz
cd vmtier-$VERSION-linux-amd64
install -m 0755 vmtier /usr/local/bin/vmtier
useradd --system --home-dir /var/lib/vmtier --shell /usr/sbin/nologin vmtier
install -d -m 0700 -o vmtier -g vmtier /var/lib/vmtier
install -d -m 0750 -g vmtier /etc/vmtier
install -m 0640 -g vmtier examples/serve-sakura.json /etc/vmtier/serve.json
install -m 0600 examples/systemd/vmtier.env /etc/vmtier/vmtier.env
install -m 0644 examples/systemd/vmtier.service /etc/systemd/system/vmtier.service
```

Edit `/etc/vmtier/serve.json` as in [configure an existing Hot cluster](#configure-an-existing-hot-cluster),
with an absolute catalog path under `/var/lib/vmtier`, and put the S3 credentials
in `/etc/vmtier/vmtier.env`. Run [capacity preflight](preflight.md), then
initialize the catalog as the service user and start the unit:

```sh
runuser -u vmtier -- /usr/local/bin/vmtier catalog-init -backend sqlite \
  -config /etc/vmtier/serve.json -path /var/lib/vmtier/catalog-sqlite -id sakura-serve-001
systemctl daemon-reload
systemctl enable --now vmtier
curl --fail http://127.0.0.1:8490/health
journalctl -u vmtier
```

The unit runs `serve` as `vmtier` with the file system read-only except
`/var/lib/vmtier`. `systemctl stop` sends SIGTERM, which drains work and releases
the catalog lock; stop the unit before offline `inspect`, `verify`, or backup and
restore, which take the same lock. A local object store outside `/var/lib/vmtier`
(development only) also needs its directory in `ReadWritePaths=`. For PostgreSQL,
start from `examples/serve-postgres.json`, set the variable named by
`catalog.dsn_env` in the environment file, and run `catalog-init -backend postgres`
with that variable set instead. Then [connect the query frontend](#connect-the-query-frontend).

## Run a disposable demo

Build the pinned stock VM binaries and run the SQLite local-backend harness:

```sh
make stock
python3 scripts/e2e_serve.py --backend local --workdir .cache/demo-serve-local
```

The harness starts isolated VM processes, ingests synthetic data, archives it,
reconstructs Hot with recent samples only, and runs 360 exact Cold/Hot/boundary
query comparisons with a full-history reference. It creates its own configuration,
SQLite state, and local objects. The host must permit local TCP listeners. Use a
new or empty work directory; omitting `--workdir` creates a temporary directory.
The harness stops its processes when finished and retains logs and `results.json`.
This is an automated demonstration, not a long-running deployment or a natural
retention-expiry test.

For S3 API testing using a disposable Moto backend:

```sh
python3 -m venv .venv-e2e
. .venv-e2e/bin/activate
python -m pip install -r scripts/requirements-e2e.txt
python scripts/e2e_serve.py --backend s3 --replicas 2 --workdir .cache/demo-serve-s3
```

The activated environment supplies `moto_server`. A local proxy rejects conditional
requests and removes ETags to exercise the supported S3 API subset. These tests
use disposable credentials and do not connect to an existing VM or real S3 bucket. See
[testing](testing.md) for regression suites and interpretation of results.

## Configure an existing Hot cluster

1. Copy [examples/serve-sakura.json](../examples/serve-sakura.json) to `serve.json`
   (see the [example guide](../examples/README.md) for other backends).
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
To move this deployment to PostgreSQL, use the [stopped-writer migration](postgres-migration.md);
keep the same archive contract and S3 location, and present one redundant logical
Cold endpoint to vmselect. Do not list complete gateway replicas as independent
storage shards.
