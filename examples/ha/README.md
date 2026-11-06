# PostgreSQL and gateway HA deployment example

Run two `vmtier serve` instances on separate hosts with the same catalog ID,
archive contract, S3 location, and [configuration](config.json). Replace all
example addresses and choose the coverage origin explicitly. The origin must
align with the six-hour window and still be available from Hot storage during
initial catch-up. Use the existing ingestion/lateness requirements; setting
`lateness_enforced` does not enforce them in the upstream ingestion service.

Supply `VMTIER_POSTGRES_DSN` through the deployment's secret mechanism. It must
resolve to the writable PostgreSQL primary, select a dedicated catalog schema,
and use verified TLS outside loopback tests. Provision the database and schema
first. Initialize once with:

```sh
.bin/vmtier catalog-init -backend postgres -config config.json -id metrics-catalog-001
```

Start on both hosts with distinct listeners reachable by the proxies:

```sh
.bin/vmtier serve -config config.json -listen :8401 -http :8080
```

Both instances serve reads. Only the holder of the PostgreSQL archiver lease
publishes windows; the fencing token is checked inside the publication
transaction. A replacement waits for lease expiry after an ungraceful failure.
`vmtier_archive_leader` identifies the current local lease holder. Gateways use
coverage-based `/ready` checks regardless of archiver role.

## One logical Cold endpoint

Run the supplied [HAProxy configuration](haproxy.cfg) on two independent hosts
and put a redundant managed TCP load balancer or fenced floating VIP in front
of them. TCP health checks should remove failed proxy hosts and existing
connections must reconnect. A single HAProxy or a single load-balancer process
would remain a failure point. The outer endpoint and PostgreSQL primary routing
must themselves have qualified failover mechanisms.

Configure stock `vmselect` with Hot storage nodes and **one** Cold endpoint:

```text
-storageNode=hot-storage-a:8401,hot-storage-b:8401,cold.example:8401
-search.denyPartialResponse=true
-search.disableCache
-dedup.minScrapeInterval=1ms
```

Choose VM replication flags for the actual Hot topology, as described in the
main service guide. Do not list both complete Cold replicas as independent
storage shards. A failed connection can make an in-flight query fail; clients
may retry. No successful partial result should be accepted during failover.
HAProxy checks the service HTTP `/ready` endpoint on port 8080 while forwarding
native RPC TCP on port 8401. Restrict access to trusted peers; this example does
not add RPC authentication or public TLS termination.

## Aggregate S3 limits

The example gives a cluster budget of 80 HTTP attempts/s, eight concurrent
requests and a queue of 64. `service.max_replicas=2` divides this into 40
attempts/s, four concurrent requests and a queue of 32 per process. SDK retries
use the same per-process admission budget. Independent pacers can each admit
an initial request together: for aggregate rate R, interval T and N processes,
the pacing envelope is approximately R*T+N attempts rather than one perfectly
coordinated global stream. Retain provider headroom for this burst and transport
jitter. No capacity is borrowed from an idle peer, so one surviving instance
keeps its original allocation.

This is static allocation, not distributed admission. Enforce at most two
simultaneous S3 clients sharing this allocation, including replacement,
maintenance and rollout processes. On Kubernetes use two replicas and
`maxSurge: 0` with `maxUnavailable: 1`, and wait for a terminating pod to exit
before admitting its replacement; Deployment surge settings alone do not
count terminating pods. Two explicit hosts with stop-before-restart supervision
are simpler. Reserve budget separately for other applications sharing the
bucket. Every instance must use the same allocation and maximum count.

## Qualification

The disposable harness requires PostgreSQL 17, HAProxy with TCP proxying, HTTP
health checks and an admin stats socket, `psql`, Moto and prebuilt stock
VM/vmtier binaries. The local run uses HAProxy 3.0.11; the hosted CI distro
package remains a separate execution environment:

```sh
export VMTIER_TEST_POSTGRES_DSN='postgres://vmtier@127.0.0.1:5432/vmtier_test?sslmode=disable'
python scripts/e2e_postgres.py --replicas 2 --workdir .cache/ha-e2e
```

Set `PSQL`, `HAPROXY` or `MOTO_SERVER` if they are not on PATH. The DSN must be a
disposable loopback database. The harness creates and removes one random schema,
keeps process logs and a `results.json`, and never requires production storage.
It kills a lease holder during catch-up, verifies standby publication, reads
through both gateways, kills one gateway and one proxy, and compares 360 exact
Cold/Hot/boundary queries plus metadata with a full-data stock VM reference.
Two simultaneous S3 failures expose SDK retry attempts and check their aggregate
admission against the static budget with stated transport timing tolerances.
This measures the exercised workload, not a saturation proof; configuration
unit tests independently verify the per-process division. All result caches
are disabled.

The harness uses two HAProxy instances behind a third HAProxy representing the
stable external L4 endpoint. It verifies loss of a gateway and a proxy, not
redundancy of that outer load balancer. It also uses a single PostgreSQL server;
database replication, promotion, partition handling, old-primary fencing and
synchronous-commit durability require separate qualification. RPO 0 is a
property of the configured database failure model, not of this example alone.

To qualify an actual SQLite cutover with the same failure and query checks, use
an additional fresh work directory:

```sh
python scripts/e2e_postgres.py --migrate-sqlite --replicas 2 --workdir .cache/ha-migration-e2e
```

This mode archives a partial prefix using the SQLite service, including a
source-verified empty window, and stops that owner before importing into a new
PostgreSQL identity. It compares normalized contract, blob location, coverage,
window digests and ordered pack references, verifies imported payloads, and
checks that migration issued no blob PUTs. PostgreSQL then catches up, survives
the same gateway/proxy failures, and passes the full query comparison after old
Hot data is removed. The stopped SQLite source must remain unchanged. See the
[migration guide](../../docs/postgres-migration.md) for production cutover and
rollback boundaries.
