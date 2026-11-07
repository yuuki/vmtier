# Bounded multi-pack archive windows

Both SQLite and PostgreSQL service backends use the same pack-rotation pipeline
within a fixed logical tenant/source/window.
Every emitted pack retains the same start/end and tenant in its version-1 index;
rotation does not subdivide coverage or change the native payload format. The
retained JSON v2 writer still produces one pack per window because its catalog
format permits one entry per tenant/source/window.

A pack rolls before the next complete native record would exceed payload bytes,
encoded index bytes, index accounting, distinct series, or block count. A record
that cannot fit an empty pack fails the attempt. Native-name and block framing,
portable decoding, checksums, and tenant checks remain mandatory. Index sizing
accounts for the complete serialized index, including the header and JSON
escaping, before accepting the record.

The existing defaults remain 64 MiB payload and 16 MiB index per pack, 10,000
series and 100,000 blocks per pack, 512 KiB per native block, and 64 KiB per metric
name. The optional top-level `max_pack_blocks` can lower the block limit to
1–100,000 (`0` or omission means 100,000). This changes packing rather than the
immutable window contract; the same limit is used by capacity preflight. Lower
values increase object and request counts, and do not increase window capacity.

One attempt is also limited to 1,024 packs, 128 MiB of native input, five million
physical samples, the independently bounded raw oracle response, and
`service.archive_timeout` (two minutes by default). Native input and physical
sample counts apply across the complete stream. Physical counts include repeated
replica rows and rows outside the logical interval, before clipping or deduplication.
An attempt exceeding any bound fails without advancing coverage.

The pipeline retains one pack under construction/upload/read-back and a bounded
expected-sample map for the whole window. Upload verification temporarily holds
the returned object bytes alongside that pack; it does not accumulate prior pack
payloads. Index metadata, upload references, decoded-block scratch, deduplication
scratch, HTTP buffers, and allocator overhead also consume memory. These bounds
are not a process RSS guarantee.

Packs can be uploaded and read back before native EOF or oracle completion. They
remain invisible: only the final catalog transaction publishes all references and
advances coverage. Cross-pack duplicate timestamps, conflicting values, and stale
markers are deduplicated together with the pinned VM implementation. The complete
raw matrix and independent count/stale queries must agree before publication.
A failed export, oracle, object read-back, cancellation, or timeout leaves coverage
unchanged; any staged objects are unreferenced attempts. Restart retries from the
committed coverage and allocates fresh object keys. No automatic orphan GC is added.

The Go regressions exercise every rotation boundary, complete-record preservation,
physical/input/pack limits, empty streams, partial framing, cross-pack conflict and
stale semantics, oracle rejection after staging, and failed upload followed by
restart. Run the stock-VM service fixture with forced rotation to check exact query
and metadata behavior after reconstructing Hot without the archived samples:

```sh
python3 scripts/e2e_serve.py --backend local --pack-max-blocks 4 \
  --workdir .cache/multipack-local-fresh
MOTO_SERVER=/path/to/moto_server python3 scripts/e2e_serve.py \
  --backend s3 --replicas 2 --pack-max-blocks 4 \
  --workdir .cache/multipack-s3-fresh
```

The report records the selected block bound and actual pack counts; the forced
fixture requires multiple packs in each published window. These commands describe
reproduction, not a claim that a particular result has been recorded. Natural Hot
retention expiry and production capacity remain separate qualification work.
