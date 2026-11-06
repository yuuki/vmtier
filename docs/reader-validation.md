# Bounded reader cache and adjacent ranges

The reader groups selected blocks within each immutable pack, sorts byte offsets,
and merges exactly adjacent uncached blocks into Range GETs of at most **4 MiB**.
It does not fetch gaps or combine different objects. Every original checksum and
native block is validated before the query can return success. The whole selected
payload and physical decoded row allowance are charged before payload I/O;
cache hits remain subject to the same query budget. Query and metadata operations
still consult catalog authority and required coverage on every request.

The immutable raw-byte cache uses individual LRU eviction. Each entry accounts
for its owned payload bytes, cache-key bytes and a conservative 256-byte
map/list bookkeeping allowance. A slice of a combined response is copied into
its own allocation before insertion, so a small entry cannot retain an
unaccounted 4 MiB buffer. An entry that cannot fit is not retained. The exposed
accounting is a logical resident-cache allowance, not measured Go heap or RSS.
Parsed indexes are **not cached**: they are parsed and validated per query, with
existing index-size and structural limits. Their transient allocations and Go
allocator/GC overhead are outside this cache total.

Concurrent misses share an index fetch or an identical combined range fetch.
Identity includes the object key, offset, length and checksum (the ordered
original checksums for a combined range). Overlapping but differently grouped
ranges are not deduplicated. There are at most **16** pending shared reads and
no pending-key queue; excess distinct work gets a resource-limit error. Each
caller can cancel independently. The last waiter requests backend cancellation;
the pending entry stays counted until the backend unwinds. A new caller waits
for an abandoned read to drain, honoring its own deadline, then retries.
Failed responses are not retained. Native payload validation still runs for
cached data before results are returned.

Shared-read buffers are separate from the cache and the RPC logical work
reservation. Range responses are bounded as above; index `Get` is bounded by
the configured blob-store object limit (64 MiB by default) before parsing rejects
index JSON above its 16 MiB default limit. These independent limits do not
establish a combined process-memory guarantee under archive/query concurrency.

## Focused measurements

Reproduce with the pinned dependencies and Go toolchain:

```sh
go test -mod=vendor -v ./internal/cold
go test -race -mod=vendor ./internal/cold
```

The following local fixture results were recorded on 2026-11-06 with Go 1.27.3,
Linux amd64 and `GOMAXPROCS=3`. Counts are fixture observations, not a production
throughput or latency claim.

| Fixture | Observation |
| --- | --- |
| Four adjacent native blocks | One Range GET instead of four; exact sample values, label names/values and series discovery; warm operations add no GETs |
| Eight simultaneous queries for that pack | One index GET plus one Range GET, 1,198 total downloaded bytes; subsequent warm query adds zero GETs/bytes |
| Cache after those queries | Five entries, 3,147 accounted bytes under a 1 MiB allowance, zero pending reads |
| Two-entry LRU pressure sequence `a,b,a,c,a,b` | Six reads, four GETs, 682 accounted bytes; inserting `c` preserves recently used `a` |
| Eighty larger native blocks, total payload above 4 MiB | Multiple adjacent GETs, each at most 4 MiB, exact complete block count and full byte coverage |

Focused checks also cover omitted selector gaps, exact byte-limit boundaries,
truncated/oversized/corrupt final combined blocks, backend errors, cancellation,
retry after failure, a canceled leader with a live follower, abandoned-read
cleanup, the finite pending map, checksum identity and catalog failure despite a
warm cache. A failed combined read returns no partial result. Existing SQLite
multiple-pack, tenant, coverage and authority checks remain in the same suite.

Stock-vmselect differential E2Es remain required for integration qualification;
these focused measurements alone do not establish provider compatibility,
metadata latency at the default five-second deadline, sustained workload capacity
or a combined RSS bound. See [testing](testing.md) and the
[first-release design](design.md).
