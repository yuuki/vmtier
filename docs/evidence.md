# Validation evidence

Reports describe their recorded revisions, environments, fixtures, and limitations.
They are not automatically refreshed when implementation changes. Historical
results using an earlier VM pin do not validate the current dependency set; see
[compatibility](compatibility.md) for upgrade boundaries. Follow
[testing](testing.md) to produce new results and preserve their provenance.
Raw artifacts remain under [`results/`](results/) at their existing paths.

| Report | Scope |
| --- | --- |
| [postgres-ha-validation.md](postgres-ha-validation.md) | PostgreSQL leases/fencing, gateway failover, stopped SQLite migration, and local synchronous-database faults; current integration and retained earlier-pin stage evidence |
| [serve-validation.md](serve-validation.md) | SQLite service and differential baseline |
| [sqlite-catalog-validation.md](sqlite-catalog-validation.md) | SQLite repository and original offline commands |
| [implementation-validation.md](implementation-validation.md) | Archive and reader implementation |
| [archive-validation.md](archive-validation.md) | Archive bounds |
| [raw-oracle-validation.md](raw-oracle-validation.md) | Incremental raw oracle |
| [reader-validation.md](reader-validation.md) | Cache, shared reads, and range coalescing |
| [metadata-validation.md](metadata-validation.md) | Metadata comparisons and deadline measurements |
| [workload-validation.md](workload-validation.md) | Workload and combined memory measurements |
| [catalog-capacity-validation.md](catalog-capacity-validation.md) | Historical catalog scale |
| [recovery-validation.md](recovery-validation.md) | Offline recovery and JSON v2 cutover |
| [foundation-validation.md](foundation-validation.md) | Foundation checks |
| [ci-evidence-validation.md](ci-evidence-validation.md) | CI evidence packaging |
| [release-validation.md](release-validation.md) | Hosted and cross-environment reproducible release builds, release workflow without a tag, systemd deployment example |
| [validation.md](validation.md) | Historical JSON v2 MVP baseline |

The [all-S3 v1 reports](results/legacy-v1/README.md) belong to a superseded
architecture and are excluded from qualification of either current service backend.

Local Moto/proxy checks exercise selected API constraints; they do not certify
real-provider authentication, quotas, durability, or performance. Reconstructing
Hot with recent data proves absence for the fixture, not natural retention expiry.
Logical memory accounting is not a process RSS ceiling. The
[first-release design](design.md) tracks remaining qualification and acceptance.
