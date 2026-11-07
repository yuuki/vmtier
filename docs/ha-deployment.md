# PostgreSQL and gateway HA deployment

Run two `serve` processes against one PostgreSQL catalog and one shared S3 blob
locator. Both read gateways are active. One archiver holds the database lease;
the other can take over after release or expiry. See the
[PostgreSQL service guide](postgres-catalog.md) for initialization and schema
upgrades, and [the deployment examples](../examples/ha/README.md) for RPC routing.
The [qualification report](postgres-ha-validation.md) links executed gateway,
migration, and PostgreSQL failure evidence with their deployment limits.

## Failure domains and endpoints

Place PostgreSQL primary and synchronous standby in separate failure domains.
Use an operated HA database service or controller with a stable writable-primary
endpoint, explicit promotion eligibility, and fencing of the old primary. vmtier
does not elect or promote PostgreSQL servers. Metadata commits request synchronous
commit, but deployments must configure the synchronous standby and durability
policy. Losing all eligible synchronous standbys can stop writes; silently
falling back to asynchronous replication changes the RPO.

Expose gateways as **one logical Cold storage node** to stock vmselect. Keep Hot
as the complementary storage node, with partial responses disabled. Simply adding
both gateways as independent storage nodes makes a failed gateway capable of
failing the complete query. Use a redundant L4 endpoint and a pair of health-checked
TCP proxies. Health checks target the gateway HTTP `/ready` endpoint; archive
leadership is not a read-readiness requirement.

A proxy or gateway failure may fail an in-flight RPC. Stock vmselect must reconnect
through the same endpoint; retries and actual query parity are qualified by the
local harness. The L4 service/VIP, PostgreSQL endpoint, S3 availability, Hot VM, and
ingestion policy remain deployment dependencies. Replicating vmtier does not remove
their failure modes.

## Static S3 budget allocation

For PostgreSQL configurations, set `service.max_replicas` to the maximum number of
simultaneous S3 clients using this allocation, including maintenance and replacement
processes. `store.requests_per_second`, `max_concurrent_requests`, and
`max_queued_requests` specify the **total vmtier allocation for the bucket**.
Each process receives `requests_per_second / max_replicas`; concurrency and queue
slots use integer division, with the remainder left unused. Limits must leave at
least one concurrent and queued request per replica.

For example, total limits `80 / 8 / 64` and `max_replicas=2` give each process
`40 requests/s / 4 concurrent / 32 queued`. Archive and read traffic, including
SDK retries, share that process's allocation. A surviving process keeps its share;
automatic redistribution could oversubscribe the bucket during a partition.

The deployment must enforce the configured maximum: use two fixed hosts or a
fixed replica count with zero rollout surge. Isolate an unreachable old process
before starting a replacement if it could still access S3. Archiver fencing does
not stop that process's reads. Reserve capacity for verification jobs or stop an
instance while they run. Other bucket clients need their own reserved headroom;
prefix separation does not isolate bucket quotas.

This is static allocation, not distributed admission. Configuration changes must
keep the combined allocation of old and new processes within the bucket budget.
Independent pacers can admit their first request together; over a measurement
interval of length T the aggregate envelope includes up to one boundary admission
per process in addition to the configured steady rate times T. This is not one
globally synchronized sliding-window limiter.
Do not scale above the declared maximum or raise per-process limits during an
outage. The configured totals should retain provider headroom for request pacing
and clients outside vmtier. SQLite keeps its existing process-local budget semantics.

## Failure behavior and recovery

- Loss of an archive owner leaves already committed reads available. A new owner
  must acquire a higher fencing token before publishing. If Cold coverage falls
  behind the required boundary, affected queries and readiness fail.
- Catalog unavailability causes errors even with warm object caches. PostgreSQL
  standby routing is rejected; a lagging replica cannot report stale coverage as
  authoritative. Reconnection uses the configured endpoint.
- The worst recoverable archive interruption depends on remaining Hot history:
  detection, lease expiry, database recovery, and catch-up must fit retention
  headroom. HA does not make an unlimited outage lossless.
- Size `service.archive_lease_ttl` for representative database RTT and the largest
  permitted multi-pack publication. Publication holds the catalog state lock, so
  lease renewal can wait behind it; an expired owner is rejected when publication
  validates its fence. Long transactions or delayed renewal can therefore fail an
  attempt safely instead of making progress. Test this alongside archive timeout
  and retention headroom rather than assuming the default TTL fits every workload.
- Keep clock synchronization on database hosts. Lease decisions use database time;
  database promotion must fence the old primary and preserve acknowledged state.
- A lost commit response is ambiguous. Reload coverage and reconcile the original
  publication before retrying. Never infer a commit from S3 object existence.

Use PostgreSQL native backups/PITR and verify the matching object history. HA
replication does not protect against operator deletion, a corrupt backup, or an
incorrect source/retention policy.
