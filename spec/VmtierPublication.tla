-------------------------- MODULE VmtierPublication --------------------------
(***************************************************************************)
(* Archive publication of vmtier with the PostgreSQL catalog.               *)
(*                                                                         *)
(* Archivers export a window, upload it under a fresh attempt key, read it *)
(* back, and publish it in one catalog transaction: the only visibility    *)
(* point (AGENTS.md, "Durability contract"). An archiver publishes only    *)
(* while it holds the archiver lease, and the transaction checks the lease *)
(* owner, its fencing token and its expiry against the database clock.     *)
(*                                                                         *)
(* The model is adversarial on purpose:                                    *)
(*   - the database clock advances at any time, so a lease can expire      *)
(*     while its holder still believes it is the leader (a paused or       *)
(*     partitioned process), and that stale holder can still try to        *)
(*     publish;                                                            *)
(*   - a database reply can be lost (an ambiguous outcome): the archiver   *)
(*     does not learn whether its transaction committed, and may replay    *)
(*     the same publication;                                               *)
(*   - an upload can store wrong bytes, and any attempt can be abandoned,  *)
(*     leaving an unreferenced orphan object;                              *)
(*   - garbage collection claims, deletes and forgets the recorded keys of *)
(*     unpublished attempts at any time, racing every archiver step.       *)
(*                                                                         *)
(* Every attempt records its key in the catalog's object ledger before its *)
(* PUT; publication consumes that pending record in its transaction, and   *)
(* garbage collection deletes only the objects of keys it claimed.         *)
(*                                                                         *)
(* Code this models (see spec/README.md for the full mapping):             *)
(*   AcquireLease, RenewLease, ReleaseLease internal/catalog/postgres_lease.go *)
(*   publishWindow, writePublication      internal/catalog/postgres.go      *)
(*   uploadPack (nonce key, PUT, read-back) internal/archive/pipeline.go    *)
(*   RecordPendingObjects, ClaimUnpublishedObjects, ForgetDeletedObjects    *)
(*                                        internal/catalog/postgres_pending.go *)
(*   Collector.RunPass                    internal/archive/gc.go            *)
(*   RunPass, publishStream               internal/archive/sqlite.go        *)
(*   runArchiveLeadership                 cmd/vmtier/postgres_service.go    *)
(*                                                                         *)
(* A window is identified by <<stream, index>>; its verified content is    *)
(* the same pair, since export plus oracle verification yields the same    *)
(* samples whenever it succeeds. The publication digest covers the pack    *)
(* references, so two publications of a window are equal iff their keys   *)
(* are.                                                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Archivers,        \* archiver processes, each with its own lease owner ID
    Streams,          \* tenant/source streams
    NumWindows,       \* windows per stream to archive: indexes 0..NumWindows-1
    MaxAttempts,      \* bound on started attempts (each takes a fresh key)
    MaxTokens,        \* bound on lease acquisitions (fencing tokens)
    TTL,              \* lease TTL in database clock ticks
    MaxTime,          \* bound on the database clock
    MaxLostReplies,   \* bound on database replies that never arrive
    FreshKeys,        \* TRUE as in uploadPack: every attempt gets a new key
    ReadBack,         \* TRUE as in uploadPack: compare the stored bytes
    FencePublication, \* TRUE as in publishWindow: validate the lease
    Ledger,           \* TRUE as in uploadPack: record each key before its PUT
    CheckPending,     \* TRUE as in publishWindow: consume a pending record
    Collect,          \* TRUE when garbage collection runs
    NoOwner,          \* model value: the lease row has no owner
    Absent,           \* model value: no object under a key
    Corrupt           \* model value: an object with wrong bytes

ASSUME NumWindows \in Nat \ {0} /\ MaxAttempts \in Nat /\ MaxTokens \in Nat
ASSUME TTL \in Nat \ {0} /\ MaxTime \in Nat /\ MaxLostReplies \in Nat
ASSUME {FreshKeys, ReadBack, FencePublication, Ledger, CheckPending, Collect} \subseteq BOOLEAN
ASSUME (CheckPending \/ Collect) => Ledger

Indexes == 0..(NumWindows - 1)
Wins    == Streams \X Indexes

\* uploadPack draws a random 128-bit nonce per attempt. Without it, a key
\* would be derived from the window alone, as in the ReusedKeys variant.
Keys == IF FreshKeys THEN 1..MaxAttempts ELSE Wins

Modes  == {"follower", "leader", "draining"}
Phases == {"none", "staged", "uploaded", "verified"}
\* pending_objects: "none" (no row), "pending" or "deleting" (claimed).
LedgerStates == {"none", "pending", "deleting"}

\* Placeholders of the same type as real keys, so that TLC compares like values.
NoKey   == IF FreshKeys THEN 0 ELSE <<NoOwner, 0>>
NoStage == [stream |-> NoOwner, idx |-> 0, key |-> NoKey, phase |-> "none"]

VARIABLES
    dbNow,        \* database clock_timestamp()
    lease,        \* archive_lease row
    gen,          \* catalog_state.generation
    coverage,     \* coverage.end_ms of each stream, as a window index
    windows,      \* committed windows with their pack key (the digest)
    objects,      \* blob store: key -> stored value
    nextAttempt,  \* attempts started so far
    lostReplies,  \* database replies lost so far
    mode,         \* archiver role, as runPostgresArchiveLoop sees it
    held,         \* fencing token the archiver believes it holds (0: none)
    localGen,     \* generation of the archiver's last Status read
    localCov,     \* coverage of the archiver's last Status read
    stage,        \* the archiver's current attempt
    granted,      \* history: <<token, owner>> of every acquisition
    ledger        \* pending_objects: state of each key

vars == <<dbNow, lease, gen, coverage, windows, objects, nextAttempt,
          lostReplies, mode, held, localGen, localCov, stage, granted, ledger>>

catalog == <<lease, gen, coverage, windows, ledger>>
local   == <<mode, held, localGen, localCov, stage>>

-----------------------------------------------------------------------------

LeaseRows == [owner : Archivers \cup {NoOwner}, token : 0..MaxTokens,
              expires : 0..(MaxTime + TTL)]

WindowRows == [stream : Streams, idx : Indexes, key : Keys,
               token : 0..MaxTokens, gen : 1..(Cardinality(Wins))]

StageRows == {NoStage} \cup
             [stream : Streams, idx : Indexes, key : Keys,
              phase : Phases \ {"none"}]

TypeOK ==
    /\ dbNow \in 0..MaxTime
    /\ lease \in LeaseRows
    /\ gen \in 0..Cardinality(Wins)
    /\ coverage \in [Streams -> 0..NumWindows]
    /\ windows \subseteq WindowRows
    /\ objects \in [Keys -> Wins \cup {Absent, Corrupt}]
    /\ nextAttempt \in 0..MaxAttempts
    /\ lostReplies \in 0..MaxLostReplies
    /\ mode \in [Archivers -> Modes]
    /\ held \in [Archivers -> 0..MaxTokens]
    /\ localGen \in [Archivers -> 0..Cardinality(Wins)]
    /\ localCov \in [Archivers -> [Streams -> 0..NumWindows]]
    /\ stage \in [Archivers -> StageRows]
    /\ granted \subseteq (1..MaxTokens) \X Archivers
    /\ ledger \in [Keys -> LedgerStates]

\* Database reply outcomes: whether the reply to the archiver is lost.
Replies == IF lostReplies < MaxLostReplies THEN BOOLEAN ELSE {FALSE}

CountLost(lost) == lostReplies' = lostReplies + (IF lost THEN 1 ELSE 0)

\* readLease: the lease is live while expires_at > clock_timestamp().
Live == lease.owner # NoOwner /\ lease.expires > dbNow

\* validateLease in publishWindow.
LeaseValid(p) == lease.owner = p /\ lease.token = held[p] /\ Live

\* The archiver has no attempt in progress.
Idle(p) == stage[p].phase = "none"

-----------------------------------------------------------------------------

Init ==
    /\ dbNow = 0
    /\ lease = [owner |-> NoOwner, token |-> 0, expires |-> 0]
    /\ gen = 0
    /\ coverage = [s \in Streams |-> 0]
    /\ windows = {}
    /\ objects = [k \in Keys |-> Absent]
    /\ nextAttempt = 0
    /\ lostReplies = 0
    /\ mode = [p \in Archivers |-> "follower"]
    /\ held = [p \in Archivers |-> 0]
    /\ localGen = [p \in Archivers |-> 0]
    /\ localCov = [p \in Archivers |-> [s \in Streams |-> 0]]
    /\ stage = [p \in Archivers |-> NoStage]
    /\ granted = {}
    /\ ledger = [k \in Keys |-> "none"]

\* The database clock moves on. Nothing tells an archiver that its lease
\* expired: it finds out only when a renewal or publication is refused.
Tick ==
    /\ dbNow < MaxTime
    /\ dbNow' = dbNow + 1
    /\ UNCHANGED <<catalog, objects, nextAttempt, lostReplies, local, granted>>

\* AcquireLease: take an unowned or expired lease with the next fencing token,
\* or return the live lease of the same owner without extending it (a retry
\* after a lost reply). A live lease of another owner refuses (ErrLeaseHeld).
Acquire(p) ==
    /\ mode[p] = "follower"
    /\ \/ (Live /\ lease.owner = p)
       \/ (~Live /\ lease.token < MaxTokens)
    /\ \E lost \in Replies :
        LET next == IF Live
                    THEN lease
                    ELSE [owner |-> p, token |-> lease.token + 1,
                          expires |-> dbNow + TTL]
        IN /\ lease' = next
           /\ granted' = granted \cup {<<next.token, p>>}
           /\ mode' = [mode EXCEPT ![p] = IF lost THEN "follower" ELSE "leader"]
           /\ held' = [held EXCEPT ![p] = IF lost THEN 0 ELSE next.token]
           /\ CountLost(lost)
    /\ UNCHANGED <<dbNow, gen, coverage, windows, objects, nextAttempt,
                   localGen, localCov, stage, ledger>>

\* RenewLease extends only a live acquisition. Any error, including a lost
\* reply to a renewal that did commit, makes runArchiveLeadership cancel and
\* drain its archive work.
Renew(p) ==
    /\ mode[p] = "leader"
    /\ \E lost \in Replies :
        LET ok == LeaseValid(p) IN
        /\ lease' = IF ok THEN [lease EXCEPT !.expires = dbNow + TTL] ELSE lease
        /\ mode' = IF ok /\ ~lost THEN mode ELSE [mode EXCEPT ![p] = "draining"]
        /\ CountLost(lost)
    /\ UNCHANGED <<dbNow, gen, coverage, windows, objects, nextAttempt, held,
                   localGen, localCov, stage, granted, ledger>>

\* The context of the leadership ends for another reason (shutdown, a renewal
\* timeout): the archive work is cancelled and drained.
StepDown(p) ==
    /\ mode[p] = "leader"
    /\ mode' = [mode EXCEPT ![p] = "draining"]
    /\ UNCHANGED <<dbNow, catalog, objects, nextAttempt, lostReplies, held,
                   localGen, localCov, stage, granted>>

\* ReleaseLease after the worker drained. The fencing counter is kept. The
\* transaction may fail or time out; the lease then expires on its own.
Release(p) ==
    /\ mode[p] = "draining"
    /\ Idle(p)
    /\ \E applied \in BOOLEAN :
        LET mine == lease.token = held[p] /\ lease.owner \in {NoOwner, p} IN
        lease' = IF applied /\ mine
                 THEN [lease EXCEPT !.owner = NoOwner, !.expires = 0]
                 ELSE lease
    /\ mode' = [mode EXCEPT ![p] = "follower"]
    /\ held' = [held EXCEPT ![p] = 0]
    /\ UNCHANGED <<dbNow, gen, coverage, windows, objects, nextAttempt,
                   lostReplies, localGen, localCov, stage, granted, ledger>>

\* RunPass starts with Status: one consistent read of the catalog.
LoadStatus(p) ==
    /\ mode[p] = "leader"
    /\ Idle(p)
    /\ localGen' = [localGen EXCEPT ![p] = gen]
    /\ localCov' = [localCov EXCEPT ![p] = coverage]
    /\ UNCHANGED <<dbNow, catalog, objects, nextAttempt, lostReplies, mode,
                   held, stage, granted>>

\* publishStream: the next window of a stream after the coverage the archiver
\* last read. An archiver keeps going with its stale view after a failure in
\* the same pass, as RunPass does with its local generation.
\* RecordPendingObjects records the key before its PUT, in its own short
\* transaction, and refuses a key that is published or already recorded. A
\* lost reply to it is the same as the attempt starting and being abandoned.
StartAttempt(p, s) ==
    /\ mode[p] = "leader"
    /\ Idle(p)
    /\ localCov[p][s] < NumWindows
    /\ nextAttempt < MaxAttempts
    /\ LET i == localCov[p][s]
           k == IF FreshKeys THEN nextAttempt + 1 ELSE <<s, i>>
       IN /\ ~Ledger \/ (ledger[k] = "none" /\ \A x \in windows : x.key # k)
          /\ ledger' = IF Ledger THEN [ledger EXCEPT ![k] = "pending"] ELSE ledger
          /\ stage' = [stage EXCEPT ![p] =
                [stream |-> s, idx |-> i, key |-> k, phase |-> "staged"]]
    /\ nextAttempt' = nextAttempt + 1
    /\ UNCHANGED <<dbNow, lease, gen, coverage, windows, objects, lostReplies,
                   mode, held, localGen, localCov, granted>>

\* blobs.Put of the verified export. It may store wrong bytes. An upload
\* already in flight can finish while the archiver drains.
Put(p) ==
    LET st == stage[p] IN
    /\ mode[p] \in {"leader", "draining"}
    /\ st.phase = "staged"
    /\ \E v \in {<<st.stream, st.idx>>, Corrupt} :
        objects' = [objects EXCEPT ![st.key] = v]
    /\ stage' = [stage EXCEPT ![p].phase = IF ReadBack THEN "uploaded" ELSE "verified"]
    /\ UNCHANGED <<dbNow, catalog, nextAttempt, lostReplies, mode, held,
                   localGen, localCov, granted>>

\* blobs.Get and bytes.Equal: only an exact copy can be published.
VerifyUpload(p) ==
    LET st == stage[p] IN
    /\ mode[p] \in {"leader", "draining"}
    /\ st.phase = "uploaded"
    /\ stage' = [stage EXCEPT ![p] =
            IF objects[st.key] = <<st.stream, st.idx>>
            THEN [st EXCEPT !.phase = "verified"]
            ELSE NoStage]
    /\ UNCHANGED <<dbNow, catalog, objects, nextAttempt, lostReplies, mode,
                   held, localGen, localCov, granted>>

\* An attempt fails or times out at any point. Its objects stay as orphans.
Abandon(p) ==
    /\ ~Idle(p)
    /\ stage' = [stage EXCEPT ![p] = NoStage]
    /\ UNCHANGED <<dbNow, catalog, objects, nextAttempt, lostReplies, mode,
                   held, localGen, localCov, granted>>

\* publishWindow: one transaction under the catalog_state row lock.
\*  1. validate the lease (owner, token, expiry by database time);
\*  2. an existing window replays only with the same digest;
\*  3. otherwise the expected generation must match (ErrConflict), the window
\*     must start at the stream's coverage end, its keys must be new, and
\*     each must still have its pending record (ErrUnrecordedObject);
\*  4. consume the pending records, insert the window and its keys, advance
\*     coverage and generation.
\* A lost reply leaves the archiver unsure: it may replay the same
\* publication, abandon it, or reload the catalog first.
Publish(p) ==
    LET st       == stage[p]
        leaseOK  == ~FencePublication \/ LeaseValid(p)
        existing == {x \in windows : x.stream = st.stream /\ x.idx = st.idx}
        replay   == existing # {} /\ \A x \in existing : x.key = st.key
        keyUsed  == \E x \in windows : x.key = st.key
        pending  == ~CheckPending \/ ledger[st.key] = "pending"
        commit   == /\ leaseOK
                    /\ existing = {}
                    /\ gen = localGen[p]
                    /\ coverage[st.stream] = st.idx
                    /\ ~keyUsed
                    /\ pending
        row      == [stream |-> st.stream, idx |-> st.idx, key |-> st.key,
                     token |-> held[p], gen |-> gen + 1]
    IN
    /\ mode[p] \in {"leader", "draining"}
    /\ st.phase = "verified"
    /\ \E lost \in Replies :
        /\ windows' = IF commit THEN windows \cup {row} ELSE windows
        /\ coverage' = IF commit THEN [coverage EXCEPT ![st.stream] = st.idx + 1]
                       ELSE coverage
        /\ gen' = IF commit THEN gen + 1 ELSE gen
        /\ ledger' = IF commit /\ CheckPending
                     THEN [ledger EXCEPT ![st.key] = "none"] ELSE ledger
        /\ CountLost(lost)
        /\ IF lost
           THEN UNCHANGED <<mode, localGen, localCov, stage>>
           ELSE IF commit \/ (leaseOK /\ replay)
           THEN /\ localGen' = [localGen EXCEPT ![p] = gen']
                /\ localCov' = [localCov EXCEPT ![p][st.stream] = st.idx + 1]
                /\ stage' = [stage EXCEPT ![p] = NoStage]
                /\ UNCHANGED mode
           ELSE \* An error drops the attempt. ErrLeaseLost ends the pass and
                \* the leadership (RunPass, runArchiveLoop).
                /\ stage' = [stage EXCEPT ![p] = NoStage]
                /\ mode' = IF leaseOK THEN mode
                           ELSE [mode EXCEPT ![p] = "draining"]
                /\ UNCHANGED <<localGen, localCov>>
    /\ UNCHANGED <<dbNow, lease, objects, nextAttempt, held, granted>>

\* ClaimUnpublishedObjects: a pending key whose key is not published becomes
\* deleting, in one transaction. Its row lock orders it with a publication
\* that consumes the same record. The minimum age only spares running
\* attempts (liveness), so the model lets a claim happen at any time.
Claim(k) ==
    /\ Collect
    /\ ledger[k] = "pending"
    /\ \A x \in windows : x.key # k
    /\ ledger' = [ledger EXCEPT ![k] = "deleting"]
    /\ UNCHANGED <<dbNow, lease, gen, coverage, windows, objects, nextAttempt,
                   lostReplies, local, granted>>

\* Collector.RunPass deletes the object of a claimed key, then forgets the
\* record (ForgetDeletedObjects). A failure in between only repeats the
\* deletion in the next pass, so the model takes both as one step. An upload
\* still in flight can store the object again afterwards (Put), leaving an
\* orphan whose key can no longer be published: it has no pending record.
DeleteClaimed(k) ==
    /\ Collect
    /\ ledger[k] = "deleting"
    /\ objects' = [objects EXCEPT ![k] = Absent]
    /\ ledger' = [ledger EXCEPT ![k] = "none"]
    /\ UNCHANGED <<dbNow, lease, gen, coverage, windows, nextAttempt,
                   lostReplies, local, granted>>

Next ==
    \/ Tick
    \/ \E k \in Keys : Claim(k) \/ DeleteClaimed(k)
    \/ \E p \in Archivers :
        \/ Acquire(p) \/ Renew(p) \/ StepDown(p) \/ Release(p)
        \/ LoadStatus(p) \/ Put(p) \/ VerifyUpload(p) \/ Abandon(p)
        \/ Publish(p)
        \/ \E s \in Streams : StartAttempt(p, s)

Spec == Init /\ [][Next]_vars

\* Archivers and streams are interchangeable: TLC may check one state of each
\* class of permuted states.
Symmetry == Permutations(Archivers) \cup Permutations(Streams)

-----------------------------------------------------------------------------
(* Safety properties.                                                      *)

\* Coverage of every stream is contiguous and backed by exactly one
\* published window per index: no gap, no overlap, no coverage without a
\* publication ("never advances coverage" on a failed attempt).
CoverageContiguous ==
    \A s \in Streams :
        /\ {x.idx : x \in {y \in windows : y.stream = s}}
               = {i \in Indexes : i < coverage[s]}
        /\ \A x, y \in windows :
               x.stream = s /\ y.stream = s /\ x.idx = y.idx => x = y

\* One generation per publication.
GenerationCounts == gen = Cardinality(windows)

\* Every reference in the catalog names an object that holds exactly the
\* verified window. Orphans are allowed; dangling or corrupt references not.
ReferencedObjectsVerified ==
    \A x \in windows : objects[x.key] = <<x.stream, x.idx>>

\* A publication never commits after one made with a newer fencing token: a
\* stale lease holder cannot publish once a successor has.
MonotoneFencing ==
    \A x, y \in windows : x.gen < y.gen => x.token <= y.token

\* A fencing token is handed to one owner only.
TokensUnique ==
    \A g, h \in granted : g[1] = h[1] => g[2] = h[2]

\* A published key has no ledger record: garbage collection can never claim
\* it, so it never deletes a published object.
PublishedKeysUnrecorded ==
    \A x \in windows : ledger[x.key] = "none"

\* Published windows are append-only and immutable.
WindowsAppendOnly == [][windows \subseteq windows']_vars

\* Objects referenced by the catalog never change afterwards.
ReferencedObjectsImmutable ==
    [][\A x \in windows : objects'[x.key] = objects[x.key]]_vars

=============================================================================
