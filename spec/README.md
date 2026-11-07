# Formal model of archive publication

[`VmtierPublication.tla`](VmtierPublication.tla) is a TLA+ model of how
archivers publish windows into the PostgreSQL catalog: the archiver lease and
its fencing token, the upload of a window under a fresh attempt key with a
read-back comparison, and the single catalog transaction that makes the window
visible. `make test-tla` checks it with TLC; CI runs the same target.

The model is small and adversarial rather than detailed. It explores every
interleaving of two archivers over a few windows while:

- the database clock advances at any time, so a lease expires while its holder
  still believes it is the leader and can still try to publish;
- a database reply is lost, so an archiver does not know whether its
  transaction committed, and may replay the same publication;
- an upload stores wrong bytes, and any attempt can be abandoned, leaving an
  orphan object.

## What corresponds to what

| Model | Code |
|---|---|
| `Acquire`, `Renew`, `Release` | `AcquireLease`, `RenewLease`, `ReleaseLease` in [`internal/catalog/postgres_lease.go`](../internal/catalog/postgres_lease.go) |
| `mode` (`follower`, `leader`, `draining`), `StepDown` | `runPostgresArchiveLoop`, `runArchiveLeadership` in [`cmd/vmtier/postgres_service.go`](../cmd/vmtier/postgres_service.go) |
| `LoadStatus`, `StartAttempt`, stale `localGen`/`localCov` | `RunPass`, `publishStream` in [`internal/archive/sqlite.go`](../internal/archive/sqlite.go) |
| `Put`, `VerifyUpload`, fresh keys | `uploadPack` in [`internal/archive/pipeline.go`](../internal/archive/pipeline.go) |
| `Publish` | `publishWindow`, `writePublication`, `reserveObjectKey` in [`internal/catalog/postgres.go`](../internal/catalog/postgres.go) |
| `LeaseValid` | `validateLease`, with `clock_timestamp()` as `dbNow` |

A window is a pair `<<stream, index>>` and its verified content is the same
pair: export and oracle verification yield the same samples whenever they
succeed, which the model takes as given. The publication digest covers the
pack references, so two publications of a window are equal if and only if
their keys are. Each window has one pack.

## Properties

| Property | Meaning |
|---|---|
| `CoverageContiguous` | Coverage of each stream is contiguous and backed by exactly one window per index. A failed attempt never advances it. |
| `GenerationCounts` | One catalog generation per publication. |
| `ReferencedObjectsVerified` | Every key in the catalog names an object holding exactly the verified window. Orphans are allowed. |
| `MonotoneFencing` | No publication commits after one made with a newer fencing token. |
| `TokensUnique` | A fencing token is handed to one owner only. |
| `WindowsAppendOnly` | Published windows are never removed or changed. |
| `ReferencedObjectsImmutable` | An object never changes once the catalog references it. |

## Configurations

Each `.cfg` file states its expected outcome on its first line. The variants
that must fail show that the checks would catch the defect they introduce, so
the model does not pass vacuously.

| Configuration | Expected | What it shows |
|---|---|---|
| `VmtierPublication.cfg` | pass | The protocol as implemented, two streams of one window. |
| `VmtierPublication_TwoWindows.cfg` | pass | The same with one stream of two windows. |
| `VmtierPublication_NoFencingCatalog.cfg` | pass | Without the lease check in `publishWindow`, the catalog still stays consistent. |
| `VmtierPublication_NoFencing.cfg` | `MonotoneFencing` violated | Without the lease check, a stale holder publishes after its successor. |
| `VmtierPublication_ReusedKeys.cfg` | `ReferencedObjectsVerified` violated | Keys derived from the window: a retry overwrites a published object. |
| `VmtierPublication_NoReadBack.cfg` | `ReferencedObjectsVerified` violated | Without the read-back comparison, a wrong upload is published. |

Two consequences for the design:

- The catalog's consistency rests on the generation check, the coverage-end
  check, the digest comparison and the object-key reservation inside the
  transaction, together with fresh keys and read-back outside it. The lease
  adds exclusivity: one archiver at a time, and no publication by a holder
  whose successor already published.
- Safety does not depend on clocks. The database clock decides only when a
  lease expires, that is, liveness.

## Not modeled

- PostgreSQL failover. Replication, promotion and fencing of the old primary
  are outside the model, which treats the database as one serializable node.
- Several packs per window, empty windows and the SQLite backend, whose
  directory lock gives one owner without a lease.
- Readers. Objects are never deleted (no GC), so the invariants on the
  catalog and objects imply that every plan a reader selects stays readable.
- Liveness. Faults are unbounded within their budgets, so progress is not
  checked.

## Running

```sh
make test-tla                                       # every configuration
python3 scripts/tlc.py --java java --jar .cache/tla/tla2tools-1.7.4.jar \
  --version 1.7.4 --sha256 <digest> --only VmtierPublication.cfg spec/VmtierPublication.tla
```

The runner downloads the pinned `tla2tools.jar` release into `.cache/tla/`,
checks its SHA-256 digest, and needs Java 11 or later. Change the model
together with the code it mirrors: a change to the lease, publication or
upload protocol should keep every configuration ending as it states.
