# Incremental raw-oracle validation

The archive verifier compares the independent raw JSON matrix with its bounded
native sample map as each row arrives. It no longer retains the complete response
body, decoded matrix, and every row's `RawMessage` allocations. It still requires
the complete success envelope, no warnings or partial-response flag, a closed
matrix, and a successful end-of-body read. Supporting count and stale-count
queries remain mandatory before publication.

The pinned VM emits each series' metric before its values. That order needs only
the current metric and reusable row buffers. Other JSON member orders are also
accepted: values preceding their metric use a temporary, sample-bounded buffer
for that series. Unknown extension fields are skipped incrementally. Duplicate
envelope, data, and series members are rejected instead of allowing later values
to hide earlier data. The raw response remains independently capped at 128 MiB;
the existing physical native-sample cap is applied before deduplication, including
replicated blocks. These changes do not increase the supported window size.

## Allocation evidence

The retained `BenchmarkVerifyRaw` fixture has 100 series with 2,000 samples each
and approximately 2.9 MB of JSON. Its expected native map and response string are
created outside the timed region. A memory-backed HTTP transport eliminates
socket variability while exercising the same `verifyRaw` entry point.

Measured on Linux amd64, AMD EPYC 7763, Go 1.27.3, `GOMAXPROCS=2`, on 2026-11-06:

| Implementation | Allocated bytes/op (three runs) | Allocations/op |
| --- | --- | --- |
| Before incremental decoding (`12414db`, with the same benchmark added) | 52,226,970; 52,222,833; 52,221,849 | 1,601,355; 1,601,337; 1,601,358 |
| Incremental decoding | 12,089,244; 12,092,818; 12,091,350 | 801,318; 801,309; 801,317 |

This fixture allocated about 77% fewer bytes. It measures cumulative Go allocation
per verification, not process peak RSS, archive-plus-query capacity, or a latency
guarantee. Other concurrent builds affected run timings, so no speedup is claimed.

```sh
GOMAXPROCS=2 go test -mod=vendor ./internal/archive \
  -run '^$' -bench '^BenchmarkVerifyRaw$' -benchmem -count 3
go test -mod=vendor ./internal/archive
```

Focused regressions include exact timestamps/values/stale markers, reordered JSON
members, duplicate and missing series, late error/warning/partial fields, every
truncated prefix of a valid fixture, trailing content, end-of-body read errors,
response-byte and sample caps, and replicated native samples before deduplication.
Existing record-boundary truncation tests still reject an export that loses the
winning conflicting duplicate without changing sample counts. Stock-VM
differential results must be recorded separately; this benchmark is not their
replacement.
