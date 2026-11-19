# ADR 0012: Committed totals per stream

## Status

Accepted and implemented for the service backends (#58). The coverage columns
join SQLite schema 3 and PostgreSQL schema 4 with the object sizes of
[ADR 0011](0011-object-sizes-in-the-ledger.md); `catalog-upgrade` fills them.

## Context

`/metrics` reported committed windows, packs and descriptor lengths only as
catalog totals, so operators could not see which tenant or source makes the
Cold tier grow. Each scrape computed those totals by scanning the whole `packs`
and `windows` tables. On SQLite the scan holds the catalog's single connection,
and with PostgreSQL every scraped replica runs it against the shared database.

Grouping that scan by stream was measured with the
[catalog-capacity fixture](../catalog-capacity-validation.md) (ten tenants,
87,600 hourly windows, one pack each, SQLite): a scrape took a median of about
13 ms with the ungrouped scan and about 100 ms grouped (a join of `windows` and
`packs`), and 24-hour selections running next to scrapes went from about 18 ms to
about 106 ms at the 95th percentile, because they wait for the connection.
Storing per-window byte sums would still need a grouped scan of `windows`
(about 40 ms on a copy of the same catalog).

## Decision

- Each coverage row keeps its stream's committed pack count and data and index
  lengths (`committed_packs`, `committed_data_bytes`, `committed_index_bytes`).
- The publication transaction adds the window's packs in the statement that
  already advances the stream's coverage end; a replay of a committed window
  changes nothing. The Cold retention transaction subtracts the retired windows
  in the statement that already advances the stream's start. Both run under the
  catalog's existing serialization (one SQLite connection, the PostgreSQL state
  row lock).
- A stream's window count is not stored: its windows are exactly the range
  `[start_ms, end_ms)` of its coverage row (the invariant that restore and the
  TLA+ model check), so it is `(end_ms − start_ms) / window`.
- A capacity observation reads the catalog status and the coverage rows in one
  read-only transaction. The catalog totals are the sums over the streams.
- `catalog-upgrade` adds the columns and fills them from the packs in the
  upgrade transaction. `catalog-restore` refuses a backup whose totals differ
  from its packs. `catalog-import-postgres` fills the destination's totals from
  the copied packs and requires them to equal the source's. New streams start
  at zero.
- `/metrics` adds `vmtier_stream_committed_object_bytes{tenant,source,kind}`,
  `vmtier_stream_windows{tenant,source}` and `vmtier_stream_packs{tenant,source}`
  for every stream of the catalog. The catalog totals keep their names and label
  sets.

## Consequences

- A scrape costs one row per stream instead of a scan of the history; it no
  longer competes with publication and selection as the catalog grows.
- The totals are derived data that every writer of windows must keep: the two
  publication paths (JSON v2 import goes through SQLite publication), the two
  retention paths, upgrades and the PostgreSQL import. Tests compare the totals
  with the packs after random publications, replays and retirements on both
  backends.
- The publication protocol is unchanged: no decision reads the totals, so
  `spec/VmtierPublication.tla` does not model them.
- Label values come from the stored contract, so the series are bounded by its
  tenants and sources, like `vmtier_stream_coverage_end_ms`.

## Alternatives considered

| Alternative | Why not |
| --- | --- |
| Group the existing scan by stream | About 7 times slower per scrape on the fixture, and selections wait behind it |
| Byte sums per window row | Still a grouped scan of every window per scrape |
| Cache scan results between scrapes | Reports stale values; the metrics otherwise omit what they cannot read now |
| Per-tenant labels on the existing totals | Changes the result of every query that does not aggregate |
