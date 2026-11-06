# Query and archive memory accounting

`query_memory_bytes` is a shared admission allowance for logical query work. It
is **not** a heap or RSS limit. The defaults remain 128 MiB per query
(`max_query_bytes`) and 256 MiB shared: at most two full reservations. Excess work
fails immediately with `memory_admission`; there is no memory-admission queue.
Regex compilation happens after admission. The stock RPC server separately caps
active requests at eight with no queue wait.

A reservation survives backend errors until the backend unwinds, and survives a
successful block search until its iterator closes or its deadline/service context
expires. Metadata methods release after constructing their complete response;
the upstream RPC API then owns serialization. This leaves at most eight RPC
response buffers outside the two reservations. It does not justify a claim that
all process memory is covered by the shared allowance.

## Reader work model

The reader charges these components before reading selected payloads, on both
cache hits and misses:

| Component | Charge and lifetime |
| --- | --- |
| Encoded indexes and selected payload | Cumulative byte lengths over the complete query, including indexes of packs whose labels do not match |
| Parsed index | Current directory only: 256-byte base, 128 bytes per series slice capacity, native-name capacity, 128-byte map base plus 96 bytes per label and label strings, 128 bytes per block slice capacity plus checksum strings |
| Decoded samples | 32 bytes per selected physical row, before clipping/deduplication; this allows for portable timestamp/value arrays and retained VM timestamp/value arrays |
| Result/selection bookkeeping | Twice the native-name length plus 512 bytes per selected block, including every repeated name across blocks |

The limit applies to cumulative read, sample and result charges **plus the
current parsed directory**. The parsed-directory charge is released between
packs. An unrelated directory from an earlier hour is not counted as retained
memory when reading the next hour. Cumulative index I/O still bounds 24-hour
selectors even if they select only one series.

The directory estimate is structural accounting, not a Go allocator measurement.
An index is capped at 16 MiB encoded before parsing and validated afterward; JSON
decoder scratch, temporary maps during validation and allocator size classes
remain outside this estimate. At most one such parse is active per admitted
query. Queries also have independent plan (up to 16 MiB), block (100,000), series
(100,000) and serialized response (128 MiB) limits. Result names must be charged
per block: a long name stored once in a directory can otherwise be copied into
thousands of tiny result blocks.

All selected checksums, native blocks and completeness checks still finish before
the RPC success prefix. A budget failure returns no partial result. Accounting
does not change catalog authority or S3 request admission.

## Components outside query admission

| Component | Existing bound and accounting |
| --- | --- |
| Immutable cache | Configured `cache_bytes` (zero disables it; maximum 256 MiB). Every entry owns its raw bytes and charges key bytes plus 256 bytes of bookkeeping. Parsed indexes are not cached. Evictions do not immediately reclaim references held by active callers. |
| Shared reads | At most 16 flights, each with a store response capped at 64 MiB; normal index validation caps accepted indexes at 16 MiB and coalesced payload ranges at 4 MiB. The count includes canceled flights until their backend unwinds. `io.ReadAll` growth, flight bookkeeping and cached copies are additional transient allocation. |
| Archive pack | One active pack, default 64 MiB payload and 16 MiB index bounds, plus the parsed directory and an upload readback buffer. The whole native stream is capped at 128 MiB. |
| Archive verifier | At most 5,000,000 physical rows per stream window by default, including replicas, before deduplication. Timestamp/value pairs occupy 16 bytes per slice element; slice capacity, keys/maps and a temporary per-series deduplication pair of arrays add memory. Raw JSON verification streams rows, with a 128 MiB response cap; reordered fields may buffer one bounded series. |
| Archive concurrency | `serve` processes one stream window at a time. Archive and query traffic share the same blob instance and S3 request budget. |
| Runtime and protocol | Go stacks, GC reserve, codec/JSON scratch, regex state, HTTP/TLS/SDK buffers, SQLite C allocations, OS mappings, catalog snapshots and upstream RPC serialization need measured headroom. |

The independent bounds deliberately do not sum to 256 MiB. Retention of backing
slices, dead objects awaiting GC, retries and canceled work can overlap. RSS must
be sampled under the actual combined workload before choosing a host/container
limit. A measured peak is evidence for that workload, not a proof for every
valid input.

## Observability

All counters and peaks reset on process restart. No query strings, timestamps or
unbounded error text are metric labels.

| Metric | Meaning |
| --- | --- |
| `vmtier_query_memory_reserved_bytes` | Current fixed admission reservations, not allocation |
| `vmtier_query_read_bytes_total` | Logical encoded index/payload bytes charged across completed searches, including failed searches and cache hits; actual transport bytes use `vmtier_object_download_bytes_total` |
| `vmtier_query_decoded_bytes_total` | Charged physical-row work at 32 bytes per row |
| `vmtier_query_result_estimate_bytes_total` | Charged copied-name and block bookkeeping estimates |
| `vmtier_query_index_estimate_peak_bytes` | Largest observed single parsed directory estimate |
| `vmtier_query_work_peak_bytes` | Highest single-query logical charge observed; an index that itself causes refusal can exceed the limit |
| `vmtier_cache_used_bytes`, `vmtier_cache_evictions_total` | Retained cache accounting and actual LRU eviction count |
| `vmtier_archive_verifier_bytes`, `vmtier_archive_verifier_peak_bytes` | Retained sample slice capacities at 16 bytes per element, two key copies/native-name bytes, 256 bytes per series for maps/slice headers, and temporary per-series dedup arrays; observed during SQLite archiving |
| `vmtier_archive_pack_bytes`, `vmtier_archive_pack_peak_bytes` | Payload/index backing capacities, structural parsed index estimate and readback capacity around pack upload; excludes in-progress builder scratch |
| `vmtier_go_heap_objects_bytes` | Go runtime heap object bytes, including objects not yet collected |
| `vmtier_go_heap_alloc_bytes_total` | Cumulative Go heap allocations; its delta measures allocation traffic, not retained memory |
| `vmtier_go_memory_total_bytes` | Total memory mapped by the Go runtime |
| `vmtier_go_heap_released_bytes` | Heap memory released to the OS; subtract from Go runtime total for an approximate runtime-managed footprint, not RSS |

Go metrics use `runtime/metrics.Read`, without forcing GC. Measure process RSS
externally (for example `/proc/PID/status`) and record sampling intervals, PID
changes, cold/warm phases, simultaneous archive progress, cache evictions and
refusal/recovery. Sum contemporaneous process samples to report the combined
vmtier/Hot/reference footprint; summing each process's independent historical
peak can overstate concurrent memory. SQLite and other C/OS allocations explain
part of the difference between Go runtime memory and RSS.

## Validation and default selection

Focused tests cover repeated long names across tiny blocks, rejection before
payload I/O, exact byte-budget boundaries, cache eviction accounting, matcher
admission ordering, and success/failure cleanup of archive component gauges.
Existing tests retain cancellation/deadline/iterator reservation checks and
complete-result failure behavior. Stock-VM differential and combined workload
measurements are required alongside these tests.

The [combined workload measurements](workload-validation.md) retain the 128/256 MiB
work defaults with a 16 MiB cache. The larger 10,000-series target was refused;
the scoped 5,000-series fixture passed concurrent archive/query/cache pressure
with 255.117 MiB sampled vmtier peak RSS. No default was raised to make it fit.
That figure depends on physical block sizes and the measured workload, and is
not a recommended container limit. Configure cache separately and retain margin
above observed RSS for workload variation. The implementation supplies bounded
logical work and component evidence, not a universal process-memory guarantee.
