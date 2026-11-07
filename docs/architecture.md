# Architecture and source map

The current `serve` process owns one SQLite authority, the archive scheduler,
the Cold read RPC gateway, and a shared blob-store client. See the
[README diagram](../README.md#how-it-works) for deployment topology and
[first-release design](design.md) for rationale and remaining acceptance work.

## Archive and publication

The scheduler selects the oldest eligible window per tenant/source from committed
coverage. It exports native blocks from a Hot-only stock vmselect, rotates bounded
packs, and compares samples with an independent raw JSON response. Objects are
uploaded under immutable keys and verified by read-back before one SQLite
transaction publishes the window and advances its stream coverage. Verified empty
windows publish no pack references. Interrupted attempts do not advance coverage;
uncommitted objects may remain as orphans because GC is not implemented.

The catalog is the authority. It binds the timing contract, tenants, sources,
identity, and blob locator. It cannot be rebuilt from an object listing. Its local
lock protects one state directory; it is not distributed fencing.

## Query path

An unmodified stock vmselect fans out to Hot storage and the Cold gateway and
performs MetricsQL evaluation. The gateway checks catalog coverage, selects bounded
pack references, reads indexes and ranges through an immutable LRU cache, validates
payloads, and decodes/clips/rebuilds native wire blocks. Shared cache misses and
adjacent ranges reduce duplicate I/O. Metadata uses validated block reads too.

Query admission reserves logical work allowances. Archive buffers, cache flights,
serialization, runtime memory, and allocator overhead also consume process memory;
see [memory accounting](query-memory.md). All S3 HTTP attempts, including retries,
share the service request budget. Required coverage gaps and object errors fail
queries rather than returning successful partial data.

## Source layout

| Path | Responsibility |
| --- | --- |
| [`cmd/vmtier`](../cmd/vmtier) | CLI dispatch, service lifecycle/scheduler, health/metrics, offline commands |
| [`internal/config`](../internal/config) | Strict JSON decoding, policy validation, effective limits, client construction |
| [`internal/archive`](../internal/archive) | Export, independent oracle verification, packing/publication pipeline, preflight |
| [`internal/catalog`](../internal/catalog) | SQLite repository/maintenance and retained JSON v2 local/HTTP authority |
| [`internal/pack`](../internal/pack) | Native pack/index format, validation, rotation, codec version pins |
| [`internal/cold`](../internal/cold) | Coverage checks, selection, cache, range reads, query work accounting |
| [`internal/gateway`](../internal/gateway) | vmstorage-compatible read RPCs, result conversion and admission |
| [`internal/querylimit`](../internal/querylimit) | Query limit errors and refusal classification |
| [`internal/objectstore`](../internal/objectstore) | Local/S3 blob access, request budget, I/O statistics |
| [`scripts`](../scripts) | Bootstrap/build, stock-VM differential fixtures, qualification, evidence packaging |
| [`examples`](../examples) | Editable configuration templates; intentionally invalid until policy is set |

## Retained JSON v2 runtime

`archive`, `gateway`, and the HTTP `catalog` service remain separate-process tools.
Their writer uses one pack per window and the JSON catalog has a 10,000-entry
lifetime limit. SQLite uses multiple packs and indexed selections without that
lifetime cap. The [offline importer](sqlite-catalog.md#import-json-v2) preserves
existing objects while creating a new SQLite authority. See
[command reference](commands.md) for the backend boundary and
[legacy deployment](legacy-json-v2.md) for its procedures.
