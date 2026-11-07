# PostgreSQL HA implementation and local qualification

This report covers stages 2–5 of [issue #25](https://github.com/yuuki/vmtier/issues/25)
and their integration with the updated runtime. All tests use disposable loopback
services. These results are implementation evidence, not a production availability
guarantee or a real-provider qualification.

## Integration with the updated runtime

The merge with `main` at `29cb29ebcf2e0d97e325b498203d6951eb8a8e46` retains the five
stage commits and combines their PostgreSQL backend with bounded pack rotation,
reader caching/admission, offline recovery, and the updated dependencies. The
integration runs use stock VictoriaMetrics **v1.155.0-cluster** at
`cab3d5fbeeedd5d0374c1dabde93939af4c2499c`, Go 1.27.3, PostgreSQL 17.11,
Python 3.12.14, Moto 5.2.5, and HAProxy 3.0.11. CI separately pins Python 3.15.2.

`make build test race vet stock` passed with PostgreSQL integration enabled, as did
all 23 Python harness tests. SQLite and JSON v2 local/S3 regression suites passed
1,440 exact sample-query comparisons; SQLite S3 used six windows and 42 packs.
Offline recovery added 480 exact comparisons across cutover, backup restoration,
catch-up, and committed-state recovery after SIGKILL. The existing workload/RSS
and large-catalog measurements were not rerun by this integration check.

The [race-enabled database fault report](results/ha-integration/postgres-faults.json)
records primary fencing and explicit promotion, preserved acknowledged metadata,
stale-writer rejection, and bounded transport errors. Same-handle read recovery
took 225.119 ms, restored synchronous-write recovery 1,470.510 ms, and a silent
blackhole returned an error after 5,001.154 ms. These are loopback observations.

The first forced-multipack gateway run
[failed](results/ha-integration/failed-health-timeout.json): the fixture's 100 ms
health-check interval also imposed a short response deadline. Its
[HAProxy log](results/ha-integration/failed-health-timeout-haproxy.log) records both
gateways marked down after approximately 105 ms, closing pooled RPC connections
during exact comparisons. The harness and deployment example now use a 1 s
interval with an explicit 3 s check timeout. The original failure is preserved.

Both reruns passed with `--replicas 2 --pack-max-blocks 4`, zero block-cache
capacity, and unchanged exact-result assertions:

| Run | Final windows / packs | Exact sample-query comparisons | Additional checks |
| --- | --- | --- | --- |
| [Gateway HA](results/ha-integration/gateway.json) | 6 / 42 | 360 | Archiver takeover, gateway/proxy loss, metadata via the stable endpoint and a measured gateway, S3 retries |
| [SQLite migration](results/ha-integration/migration.json) | 6 / 35 | 360 | Imports 3 windows / 14 packs / 1 empty window with identical metadata, no additional PUTs, and unchanged source; then repeats HA and query checks |

Together with the regressions and recovery suite above, the integration runs
passed **2,640 exact sample-query comparisons**, plus metadata and failure checks.
The reports were produced from the staged merge before committing, so provenance
records stage-5 HEAD `6ac66bd` and a dirty worktree. The tested vmtier binary SHA-256
is `74274b831b936d3171bf77759d4e5d820e0743d115a612390451fb24b23fd0f9`;
the reports also retain stock-binary and harness hashes. Historical results below
were not overwritten. Hosted CI remains a separate check of the pushed commit.

## Historical stage evidence

The following stage results use stock VictoriaMetrics **v1.128.0-cluster** at
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. They remain at their original paths and
do not validate the updated dependency set. See [compatibility](compatibility.md)
for VM pin boundaries and [testing](testing.md) for current reproduction commands.

## Repository and service checks

The recorded stage worktree passed `make build test race vet` with PostgreSQL integration enabled. Tests cover
atomic publication, replay after later commits, independent clients, consistent
bounded snapshots, explicit schema upgrade, DB-time ownership and fencing, expiry
while waiting for a lock, cancellation/drain/release ordering, primary-only reads,
and transport deadlines. The existing Python harness tests also pass.

Stage 3's [gateway report](results/ha-stage3/gateway.json) records two PostgreSQL
service candidates and two HAProxy instances behind a stable L4 fixture. Killing
the archive owner advances the fencing token and the survivor completes the
remaining stream windows. Killing a gateway and a proxy interrupts established
paths; the same stock-vmselect endpoint reconnects and returns exact results.
All 360 sample-query comparisons and both tenants' metadata comparisons pass.
S3 outages trigger SDK retries through each process's allocated budget.

The S3 measurements describe observed workload bounds, not saturation capacity.
Static quota division is separately tested. The outer L4 fixture is a single
process, so this test does not qualify redundancy of a production load balancer.

## Migration checks

Repository and CLI tests validate stopped-owner exclusion, source identity and
contract/blob matching, empty windows, preserved generations and references,
atomic rollback, and refusal to overwrite an existing destination. The migration
mode of `e2e_postgres.py` additionally imports an actual partially archived SQLite
fixture, verifies metadata equality and zero object PUTs during import, and then
completes archival with PostgreSQL. A verified empty window is included. The same
Cold/Hot/boundary comparisons are run after removing historical samples from Hot.
This is a controlled reconstruction of Hot absence, not natural retention expiry.
The [migration report](results/ha-stage4/migration.json) records three imported
windows, two packs, and one explicit empty window; catch-up finishes with six
windows and five packs, with the stopped source unchanged.

## PostgreSQL faults

`postgres_ha.py` creates a primary and physical synchronous standby on separate
local data directories. It uses a stable test proxy and the same repository handle
across endpoint redirection. The suite verifies:

- A dropped COMMIT acknowledgement is reconciled by status and identical replay.
- Lease expiry/takeover rejects an old writer; the token remains monotonic across
  database promotion and the pre-promotion writer is rejected afterward.
- An unpromoted standby cannot supply authoritative query plans.
- Reset/refused connections and a silent packet blackhole return errors. The
  blackhole includes the separate driver cancellation connection.
- After the old primary is confirmed stopped, explicit standby promotion preserves
  acknowledged identity, coverage, empty windows, pack descriptors, and generation.
- A replacement synchronous standby is created before validating new publication;
  the test does not relax synchronous durability to resume writes.

The script records read-recovery and restored synchronous-write times separately,
plus UTC timestamps, tool versions, source revision/dirty state, test-binary SHA256,
and cleanup outcomes. Existing work directories are refused so prior evidence is
preserved. Failure paths retain diagnostics and partial reports.

The [final race-enabled fault report](results/ha-stage5/postgres-faults.json)
records PostgreSQL 17.11 and Go 1.27.3 on 2026-11-06. The observed same-client read
recovery was **214.906 ms**, restored synchronous-write recovery **961.713 ms**,
reset-partition failure **0.375 ms**, and silent-blackhole failure **5001.231 ms**.
All owned PostgreSQL processes were stopped during cleanup. These are single-run
loopback measurements, not percentiles or deployment targets.

## Regression evidence

The recorded stage-5 runtime also passed the SQLite and JSON v2 differential suites:

| Runtime / blobs | Exact sample comparisons | Report |
| --- | ---: | --- |
| SQLite / local | 360 | [JSON](results/ha-stage5/sqlite-local.json) |
| SQLite / Moto S3, replication 2 | 360 | [JSON](results/ha-stage5/sqlite-s3.json) |
| JSON v2 / local | 360 | [JSON](results/ha-stage5/json-local.json) |
| JSON v2 / Moto S3, replication 2 | 360 | [JSON](results/ha-stage5/json-s3.json) |

Together with the new gateway and migration suites, these cover **2,160 exact
sample-query comparisons**, plus metadata and failure/recovery checks. Reports are
copied without rewriting their provenance. Their parent revision and dirty flag
identify the stage worktree used; binary hashes identify the executed builds.

## Reproduction and limits

Build/bootstrap first and use explicitly disposable test targets. For gateway and
migration tests, install HAProxy, `psql`, and the Python E2E requirements:

```sh
export VMTIER_TEST_POSTGRES_DSN='postgres://vmtier@127.0.0.1:5432/vmtier_test?sslmode=disable'
make build test race vet stock
python scripts/e2e_postgres.py --replicas 2 --workdir .cache/pg-gateways
python scripts/e2e_postgres.py --replicas 2 --migrate-sqlite --workdir .cache/pg-migration
python scripts/postgres_ha.py --postgres-bin /usr/lib/postgresql/17/bin \
  --race --workdir .cache/pg-faults
```

The fault harness runs as an unprivileged user, initializes its own database
processes, and needs no existing server. The main CI workflow exercises gateway
and migration behavior; the PostgreSQL fault workflow provisions version 17 and
uploads reports/logs even on failure. Hosted results for these new workflows are
separate from the local evidence recorded here.

Database promotion is explicitly driven by the fixture after process-stop fencing.
Automatic election controllers, cross-host clock skew, real multi-AZ network
partitions, redundant production L4/VIP infrastructure, actual S3 providers, and
natural Hot retention transitions are not exercised. Recovery measurements from
this small same-host fixture are not an SLO. Production deployments still need
the topology, durability, fencing, resource allocation, and backup procedures in
the [HA deployment guide](ha-deployment.md).
