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
  orphan object;
- garbage collection claims, deletes and forgets the recorded key of an
  unpublished attempt at any time, racing every archiver step;
- Cold retention retires the oldest window of a stream at any time.

## What corresponds to what

| Model | Code |
|---|---|
| `Acquire`, `Renew`, `Release` | `AcquireLease`, `RenewLease`, `ReleaseLease` in [`internal/catalog/postgres_lease.go`](../internal/catalog/postgres_lease.go) |
| `mode` (`follower`, `leader`, `draining`), `StepDown` | `runPostgresArchiveLoop`, `runArchiveLeadership` in [`cmd/vmtier/postgres_service.go`](../cmd/vmtier/postgres_service.go) |
| `LoadStatus`, `StartAttempt`, stale `localGen`/`localCov` | `RunPass`, `publishStream` in [`internal/archive/sqlite.go`](../internal/archive/sqlite.go) |
| `Put`, `VerifyUpload`, fresh keys | `uploadPack` in [`internal/archive/pipeline.go`](../internal/archive/pipeline.go) |
| `ledger`, the recording in `StartAttempt` | `RecordPendingObjects` in [`internal/catalog/postgres_pending.go`](../internal/catalog/postgres_pending.go), called by `uploadPack` before its PUT |
| `Publish` | `publishWindow`, `consumePending`, `writePublication`, `reserveObjectKey` in [`internal/catalog/postgres.go`](../internal/catalog/postgres.go) |
| `Retire`, `origin`, `retired` | `RetireWindows` in [`internal/catalog/postgres_pending.go`](../internal/catalog/postgres_pending.go), driven by `Collector.retire` in [`internal/archive/gc.go`](../internal/archive/gc.go) |
| `gc`, `GcClaim`, `GcDelete`, `GcForget`, `GcAbandon` | `Collector.RunPass` in [`internal/archive/gc.go`](../internal/archive/gc.go): `ClaimUnpublishedObjects`, then `Delete` from the collector's memory, then `ForgetDeletedObjects`; a failed deletion (`DeferClaimedObjects`) or a crash abandons the key, which stays claimed. Ledger calls in [`internal/catalog/postgres_pending.go`](../internal/catalog/postgres_pending.go) |
| `strays` | Uploads that land after their key stopped being pending: acknowledged late orphans |
| `LeaseValid` | `validateLease`, with `clock_timestamp()` as `dbNow` |

A window is a pair `<<stream, index>>` and its verified content is the same
pair: export and oracle verification yield the same samples whenever they
succeed, which the model takes as given. The publication digest covers the
pack references, so two publications of a window are equal if and only if
their keys are. Each window has one pack.

## Properties

| Property | Meaning |
|---|---|
| `CoverageContiguous` | Coverage of each stream is contiguous from its retained start and backed by exactly one window per index. A failed attempt never advances it. |
| `GenerationCounts` | One catalog generation per publication, retired or not. |
| `ReferencedObjectsVerified` | Every key in the catalog names an object holding exactly the verified window. Orphans are allowed; garbage collection never deletes a referenced object. |
| `PublishedKeysUnrecorded` | A published key has no ledger record, so garbage collection can never claim it. |
| `StoredObjectsAccounted` | Every stored object is published, recorded in the ledger, or a late orphan: retirement and garbage collection leave nothing else behind. |
| `MonotoneFencing` | No publication commits after one made with a newer fencing token. |
| `TokensUnique` | A fencing token is handed to one owner only. |
| `WindowsAppendOnly` | Published windows are never changed, and leave only by retirement, which moves the stream's start past them and never back. |
| `ReferencedObjectsImmutable` | An object never changes once the catalog references it. |

## Configurations

Each `.cfg` file states its expected outcome on its first line. The variants
that must fail show that the checks would catch the defect they introduce, so
the model does not pass vacuously.

| Configuration | Expected | What it shows |
|---|---|---|
| `VmtierPublication.cfg` | pass | The protocol as implemented, two streams of one window. Attempts record their keys and publication consumes them; garbage collection is off. |
| `VmtierPublication_TwoWindows.cfg` | pass | The same with one stream of two windows, whose oldest window may be retired at any time. |
| `VmtierPublication_Collect.cfg` | pass | The protocol with garbage collection and Cold retention acting at any time on two streams, within smaller attempt, clock and lost-reply bounds. |
| `VmtierPublication_RetainCollect.cfg` | pass | The same on one stream of two windows, so that retirement and an attempt for the next window interleave. |
| `VmtierPublication_NoPendingCheck.cfg` | `ReferencedObjectsVerified` violated | A publication that consumes the record without checking that it is still pending publishes a key that garbage collection claimed; the collector then deletes its object. |
| `VmtierPublication_RetireUnrecorded.cfg` | `StoredObjectsAccounted` violated | Retirement that does not record the retired key leaves the object behind for good. |
| `VmtierPublication_NoFencingCatalog.cfg` | pass | Without the lease check in `publishWindow`, the catalog still stays consistent. |
| `VmtierPublication_NoFencing.cfg` | `MonotoneFencing` violated | Without the lease check, a stale holder publishes after its successor. |
| `VmtierPublication_ReusedKeys.cfg` | `ReferencedObjectsVerified` violated | Keys derived from the window, without the ledger: a retry overwrites a published object. |
| `VmtierPublication_NoReadBack.cfg` | `ReferencedObjectsVerified` violated | Without the read-back comparison, a wrong upload is published. |

Two consequences for the design:

- The catalog's consistency rests on the generation check, the coverage-end
  check, the digest comparison and the object-key reservation inside the
  transaction, together with fresh keys and read-back outside it. The lease
  adds exclusivity: one archiver at a time, and no publication by a holder
  whose successor already published.
- Safety does not depend on clocks. The database clock decides only when a
  lease expires, that is, liveness. The same holds for garbage collection: the
  minimum age of a claimed key is not modeled, and a claim may race any step of
  an attempt; the pending check in the publication transaction keeps a claimed
  key out of the catalog.

## Not modeled

- PostgreSQL failover. Replication, promotion and fencing of the old primary
  are outside the model, which treats the database as one serializable node.
- Several packs per window, empty windows and the SQLite backend, whose
  directory lock gives one owner without a lease.
- Readers. Garbage collection never deletes a published object, so the
  invariants on the catalog and objects imply that every plan a reader selects
  stays readable while its windows are published. A plan selected before a
  retirement relies on the minimum age of garbage collection exceeding the
  gateway's request cap, a timing assumption the model does not include.
- Copies of the catalog. The model has one database; a restored or copied
  PostgreSQL catalog is kept from collecting the original's pending keys by
  the database incarnation recorded with each key (ADR 0006), which the model
  does not include.
- Key freshness. Attempt keys are fresh by construction (`FreshKeys`); the code
  relies on 128-bit random nonces. With reused keys, a key forgotten by
  garbage collection could be recorded again by another attempt, and the
  pending check would not tell the attempts apart.
- Liveness. Faults are unbounded within their budgets, so progress is not
  checked.

## The timing contract in Lean

[`lean/VmtierTiming.lean`](lean/VmtierTiming.lean) proves, for every
integer input, the arithmetic of the timing contract in
[docs/design.md](../docs/design.md#data-and-time-contract). It models
`cold.Reader.RequiredEnd` (Go's truncating `/` is `Int.tdiv`),
`archive.Config.validateTime`, the coverage check of `cold.Reader.Search`
and `archive.RetentionBoundary`.

| Theorem | Meaning |
|---|---|
| `requiredEnd_gt` | The required end trails `now - H` by less than one window, whatever the sign. |
| `requiredEnd_le` | Once `now ≥ H`, the gateway never requires a window `validateTime` would refuse as too young. |
| `timeValid_iff`, `archivable_interval_nonempty` | A window can be archived exactly while `start + w + H ≤ now < start + R - s`, an interval of `R - H - s - w > 0` milliseconds. |
| `export_complete` | With `validateTime` checked at the export's start and before publication, every sample ingress accepts for the window was accepted before the export, and Hot retained it throughout: the window is complete. |
| `query_complete` | A query the gateway accepts misses no accepted sample: each lies below the coverage end (Cold) or is younger than `R` (Hot). |

| `retentionBoundary_le` | A Cold retention pass that runs (its boundary passed the origin) never retires a window younger than `cold_retention` plus one window. |
| `retention_complete` | With retirement, an accepted query still misses no accepted sample younger than `cold_retention` and at or after the stream's origin (the coverage origin, or a later one for a stream added with `catalog-add-streams`), provided the catalog's clock at the retirement was at most one window ahead of the gateway's: each lies in Cold's retained range or in Hot. An `example` shows that a larger skew loses data. |

An `example` shows that `query_complete` needs `R > H + s + w`: with
`R = H + w`, the gateway accepts a query that Hot can no longer answer.

The proofs assume what the code cannot check: Hot retains samples younger
than `R`, ingress enforces `L`, export and oracle verification return what
Hot holds, and the catalog has one complete window per index, which the TLA+
model checks. `make test-lean` builds them with the toolchain that
`lean/lean-toolchain` pins (elan on `PATH` installs it); CI runs it.

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
