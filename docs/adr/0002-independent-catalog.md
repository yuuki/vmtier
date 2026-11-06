# ADR 0002: Separate catalog authority from the S3 data plane

## Status

Accepted for the MVP. This preserves the native packs and stock vmselect gateway from [ADR 0001](0001-native-gateway.md), replacing catalog updates through conditional S3 PUT. Automatic migration from the old all-S3 catalog is not provided. See the [first-release design](../design.md) for subsequent work.

## Context

The target deployment assumes Sakura Object Storage does not support conditional PUT. This is a deployment requirement, not a claim that public API documentation enumerates every conditional-header behavior. The previous design depended on `If-Match`, `If-None-Match`, and ETag-based CAS. Merely substituting ordinary PUT could allow concurrent writers to overwrite the manifest and lose committed coverage.

Changing payloads to Parquet or TSDB blocks would not resolve metadata publication and concurrency. Keep the native payload, codec, and stock VM query integration, and move the authority that controls publication.

## Decision

1. **S3 BlobStore:** use ordinary PUT, GET, and Range GET. Attach Content-MD5 to PUT and verify uploaded length and SHA-256 with a read-back GET. Correctness does not depend on ETags, conditional PUT, or LIST.
2. **Object keys:** include a cryptographically random nonce per attempt and never reuse a published key. The application prevents retries or competing writers from overwriting published payloads; this is not provider-enforced Object Lock.
3. **Catalog service:** provide application-level generation CAS over local persistent state, separately from S3. Clients supply a catalog ID, expected generation, and new complete windows. S3 conditional headers are not involved.
4. **Fixed contract:** retain coverage origin, window, dedup=1ms, archive age H, lateness L, tenants, and source IDs/URL hashes. The authority validates append-only transitions and complete tenant-by-source windows. A configuration change cannot conceal a gap.
5. **Persistence:** use flock, file fsync, atomic rename, and directory fsync. Separate explicit `catalog-init` from normal startup; missing state is never silently initialized as empty.
6. **Readers:** fetch an authoritative snapshot for every query. Validate the expected ID, process-local generation monotonicity, and identical content at the same generation. Retain referenced snapshots/objects across updates. If the catalog is unavailable, return an error rather than using a stale successful snapshot.

Publication order is: verify source completeness, upload to new keys, read back and verify all objects, commit the catalog with CAS, then expose the data to queries. A stop before commit, a losing writer, or a lost response may leave orphans. An orphan does not establish coverage. The MVP performs neither GC nor automatic deletion.

## Alternatives

| Approach | Benefit | Decision or condition for reconsideration |
| --- | --- | --- |
| All S3 with conditional PUT | CAS without another service or persistent disk. | Does not meet the target backend requirement. |
| All S3 with ordinary PUT and an operational single-writer rule | Small implementation. | Accidental duplicate writers and lost responses can lose updates. LIST or timestamped keys alone do not establish an authoritative latest generation. |
| An S3 lock object written with ordinary PUT | Appears to add locking. | Without create-if-absent/CAS, it moves the same race to the lock object. |
| Separate catalog service and bounded state file | Few dependencies; implements CAS, contract checks, and durable publication. | Selected for the MVP, with a single authority/disk, backups, and capacity management. |
| SQLite catalog | Transactions, journaling, indexes, and consistent backup facilities. | The bounded append-only MVP avoids adding a database dependency, schema migrations, and backup API. Reconsider as entry counts or query load grow. SQLite alone does not provide multi-host HA. |
| PostgreSQL or another external database | Multi-client transactions and established HA/backup operations. | Adds deployment, credentials, migrations, and operational dependencies. A future option for larger or HA deployments that already operate a database. |

## Failure model and limitations

The catalog trusts one correct authority and durable local storage. NFS locking/fsync semantics, consensus between authorities, HA, and automatic failover are outside this implementation. Detecting rollback to an old backup across complete client/server restarts is not guaranteed. Recovery requires a new ID, isolation of old writers, coverage/object auditing, and explicit client cutover.

HTTP is intended for trusted networks. Listeners default to loopback, with an optional bearer token through `VMTIER_CATALOG_TOKEN`. Deployment must provide a TLS proxy and network ACLs where needed. The token is not fine-grained RBAC. Authorized writers must perform source completeness checks correctly; the authority does not re-export VM sources to independently verify them.

Objects are published only after GET returns their expected content. Subsequent missing or modified objects fail integrity checks. These checks do not add provider availability or durability guarantees. MD5 is not treated as cryptographic tamper detection; SHA-256 and the trusted catalog provide vmtier's content checks.

## Validation and operational consequences

The independent catalog and local S3 backend are tested through a proxy that rejects conditional requests and strips ETags. [Validation results](../validation.md) distinguish these runs from historical all-S3 tests.

The real Sakura service, real credentials, provider failures, quotas, IAM, SigV4 behavior, and long-term durability have not been tested. Bucket-wide rate and concurrency controls for Sakura's Standard or Archive plans are separate requirements; moving the catalog does not satisfy them. Follow the [official API documentation](https://manual.sakura.ad.jp/cloud/objectstorage/api.html) and [2026-04-09 checksum compatibility announcement](https://cloud.sakura.ad.jp/news/2026/04/09/objectstorage-checksum/) rather than recommending an old SDK based on superseded restrictions.
