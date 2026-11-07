# Offline recovery and JSON v2 cutover validation

The dedicated [recovery harness](../scripts/e2e_recovery.py) exercises the offline
operator sequence with unmodified stock VictoriaMetrics and local disposable
objects. It checks the full query path before and after JSON v2 import, catch-up
from an older SQLite backup under a new ID, and real service process crashes.
This is the scoped recovery evidence for issue #18; natural retention expiry
(#16), real-provider qualification (#15), and physical power-loss durability are
separate work.

## Scenario and assertions

The primary fixture uses two tenants, one source, 16 metric series per tenant,
30-second samples, one-hour archive windows, and the pinned VM revision
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`. Special values, stale markers, duplicate
timestamps, absent labels, resets, and millisecond boundary samples come from the
existing differential fixture. Stock vmselect result caching is disabled and
deduplication is `1ms`. Each phase compares 30 expressions for each tenant over a
Cold-only range and a Hot/Cold boundary range: **120 exact comparisons per phase**.
The `@` expression stays inside Cold when using the Cold-only frontend.

1. The JSON v2 one-shot writer archives three windows for both tenants and exits.
   The old gateway and catalog service stop before `catalog-import`. Import
   preserves all coverage, six pack/index references, and the content hashes,
   lengths, modification times, and inodes of all twelve existing objects. The
   SQLite service becomes ready without rewriting or re-exporting those windows.
   Both JSON and imported SQLite pass the reference comparisons.
2. The SQLite owner stops for an offline backup at generation 6. The original
   service restarts, and a real wall-clock eligibility transition makes one more
   hour eligible for both tenants. It advances to generation 8, then stops. No
   production clock override or archive-contract change is involved.
3. `catalog-restore` reads the older backup into a fresh directory with a new ID.
   The restored service cannot declare readiness or answer a query crossing its
   missing suffix. The errors must identify incomplete archive coverage; an
   unrelated HTTP failure cannot satisfy the test.
4. Native pack and index objects are staged and read back, then a loopback proxy
   pauses the independent raw verification response. `SIGKILL` terminates the
   actual service before publication. Its catalog snapshot remains unchanged.
   Restart catches up the missing suffix from retained Hot data, ignores the
   staged orphan keys, becomes ready at generation 8, and passes the reference
   comparisons. Orphans remain on disk, intentionally unreferenced.
5. A second `SIGKILL` occurs after successful publication. Restart preserves the
   complete catalog snapshot, performs no further source requests, and passes
   the reference comparisons again.

The expired-backup negative case uses a separate one-second-window fixture and a
20-second configured recovery deadline. A real archived window is imported and
backed up. Hot is then **explicitly reconstructed as empty**, and its raw export
is checked for absence. The harness waits for the real configured retention
safety deadline to pass, restores the backup into another new ID, and starts
the service. The scheduler refuses catch-up before any source request, readiness
returns HTTP 503, raw export of the missing suffix returns the coverage error,
and generation/coverage remain unchanged. Raw export avoids stock vmselect's
latency adjustment for recent instant queries.

Stock VM has a minimum actual retention of 24 hours; this short fixture is a
recovery refusal test with simulated source-history absence. It does not exercise
natural retention expiry or prove an externally enforced lateness contract.

## Measurements

Both final runs passed on 2026-11-06 with `GOMAXPROCS=2` for every workload child.
The [one-replica report](results/recovery/local.json) and
[two-replica report](results/recovery/replicated.json) each record 480 exact
comparisons, for **960 total**. Both use runtime source from `c692cba` without
runtime code changes. The vmtier binary SHA-256 is
`63396250a4c99cd0325b5d6b266c3c87e86a92565f36c2148e1f81f1a9f772b4`;
the Go source-tree SHA-256 is
`e2a8a391dbd6e321a8653a346c51b2196d8d0f9ac7c55ec5605489f55b21d069`.
The recovery harness SHA-256 is
`42b4f81c2504441caf472efa3422300c106f1f1dc9a84245b693a804fabf9003`.

| Observation | One replica | Two replicas |
| --- | ---: | ---: |
| Exact comparisons | 480 | 480 |
| Initial JSON generation / imported SQLite generation | 3 / 6 | 3 / 6 |
| Restored backup generation / caught-up generation | 6 / 8 | 6 / 8 |
| JSON-owner stop to SQLite readiness | 0.145 s | 0.148 s |
| Backup age when catch-up completes | 58.501 s | 57.990 s |
| Original-owner stop to recovery readiness | 0.354 s | 0.351 s |
| Catch-up including injected precommit crash | 0.333 s | 0.327 s |
| Configured Hot retention headroom at recovery | 852,438 s | 852,424 s |
| Staged objects left unpublished by precommit SIGKILL | 2 | 2 |
| Source requests after committed restart | 0 | 0 |
| Expired-restored generation before / after | 1 / 1 | 1 / 1 |
| Source requests after expired restore | 0 | 0 |

The two final runs overlapped in the same development container. Timings are
observations of this small fixture, not isolated benchmarks or an operational
recovery SLO. The configured 240-hour recovery retention is conservative relative
to stock Hot's 30-day fixture retention; the reported headroom does not choose a
production recovery margin. Each report includes all process commands, identities,
coverage, object identities, error bodies, and source/binary/harness hashes.

Three failed harness-development reports remain alongside the passing runs:
[Cold `@` boundary](results/recovery/failed-cold-at-boundary.json) requested the
first Hot millisecond through a Cold-only frontend; [recent instant query](results/recovery/failed-recent-instant.json)
allowed stock vmselect's latency offset to shift the intended missing suffix;
[export status](results/recovery/failed-export-status.json) required query API
HTTP 422 even though stock raw export returns HTTP 400 with the correct coverage
error. The final suite corrects the query intervals and requires the precise
endpoint status plus the coverage error body, without weakening exact successful
result comparisons. These were harness failures, not accepted runtime behavior
changes. Python compilation and all 11 existing harness regression tests pass.

## Operator sequence

Use the complete commands in [catalog maintenance](sqlite-catalog.md#import-json-v2)
and [offline backup/restore](sqlite-catalog.md#offline-backup-and-restore).

1. Stop the JSON writer and catalog/gateway owners. Preserve the old authority and
   immutable objects. Import into a nonexistent destination under a new ID with
   the original contract, source URLs, and blob locator; inspect coverage and
   references before starting SQLite.
2. Start `serve` with the imported path and ID. Check `/ready`, representative
   Cold-only queries, and boundary queries before switching the frontend. An old
   writer must not resume against shared objects while recovery is in progress.
3. Stop `serve` for `catalog-backup`, and copy the complete backup directory to an
   independent failure domain. Keep the metadata/checksum alongside `catalog.db`.
   Copying only the live main database file may omit WAL data.
4. For recovery, isolate the old owner, restore into a fresh directory/new ID,
   and start `serve` with the saved contract and original object location. Keep
   traffic gated on readiness and verify the missing suffix through Cold alone.
   Compare backup age plus restore/catch-up time against remaining Hot headroom,
   including the safety margin. This small fixture does not choose that margin
   for an operator's workload.
5. If required Hot history has expired, preserve the failed state and recover a
   complete copy of the missing history or authority metadata. An empty response,
   fresh empty catalog, or S3 listing cannot establish the missing coverage.

Only catalog references make objects visible. The harness's local object scan is
an assertion about no import rewrites and ignored orphans; the recovery commands
never discover publication from object listing. There is no online backup, HA,
automatic rollback detector, orphan deletion, or physical power-loss claim.

## Reproduction

After the normal pinned build in [testing](testing.md):

```sh
python3 scripts/e2e_recovery.py --gomaxprocs 2 --workdir .cache/recovery-local
python3 scripts/e2e_recovery.py --gomaxprocs 2 --replicas 2 --workdir .cache/recovery-replicated
```

Use a fresh work directory. The default run intentionally waits for one real
eligibility transition after 60 seconds. If a slower executor cannot complete
initial migration/backup before that transition, increase `--rollover-delay`.
The suite fails rather than changing the backup's meaning. Hosted CI runs the
one-replica local case and packages the complete work directory even on failure.
