# Archive capacity preflight

Run `vmtier preflight` against representative busy windows **before initializing
a SQLite or PostgreSQL catalog**. It uses the service's bounded native pack rotation and the
same independent raw/count verification. It reads only the selected Hot source:
there are no object PUTs, catalog reads, catalog initialization, or publication.
An existing catalog directory is neither opened nor modified.

Prepare a candidate service configuration with the intended H/L/retention/window
policy, source, tenant, and blob locator. The SQLite catalog path may be
nonexistent. PostgreSQL configurations must include their normal S3 budget and
maximum-replica settings, but the DSN environment variable need not be set. No
catalog or blob connection is opened and S3 credentials are not needed. As for
archival, the selected interval must be old enough for H and still safely inside
the declared Hot retention. Lateness enforcement remains an operator assertion.

```sh
make build
# Pick an actual busy, UTC-window-aligned [start,end) in the source's retained data.
.bin/vmtier preflight -config candidate.json \
  -tenant 1:2 -source hot \
  -start 1793948400000 -end 1793952000000 > preflight.json
```

The example timestamps identify one hour on 2026-11-06; replace them with a
representative interval satisfying the candidate policy at execution time. Both
endpoints are required. One invocation accepts exactly one logical window and one
configured tenant/source. Tenant and source flags may be omitted when each has
only one configured value. Repeat for other busy streams/windows, including
replicated or unusually large native records. The existing JSON v2 writer remains
a single-pack pipeline; this command sizes the shared SQLite/PostgreSQL service
archive pipeline.

The command uses `service.archive_timeout` as its complete attempt deadline
(default two minutes). It does not offer larger pack or sample limits than the
service. It exits unsuccessfully on a source, completeness, time, or capacity
failure. When a run starts, stdout contains a JSON observation even on failure;
`verified: false` and the nonzero exit status must never be treated as a successful
estimate. No coverage is published in either case.

## Reading the observation

- `policy`, `hot_retention_ms`, `safety_margin_ms`, and `attempt_budget_ms` describe
  the selected stream contract and available attempt time.
- `pack_limits` reports effective payload, index, input, series, blocks, record,
  and metric-name bounds; `max_packs_per_window`, `max_physical_samples`, and
  `max_raw_response_bytes` report independent whole-window/verifier bounds.
- `native_input_bytes` counts the native HTTP response consumed, including its
  framing. `physical_samples` counts accepted native rows before interval clipping
  or deduplication, including replicas and rows outside the requested interval.
  `native_records` counts accepted native records.
- `data_bytes`/`index_bytes`, `blocks`, `unique_series`, and `packs` describe emitted
  packs. Each pack has its own physical-row/series/block counts. A series repeated
  across packs contributes once to `unique_series`; pack series counts overlap.
- `verified_deduplicated_samples` is populated only after independent verification
  succeeds. Empty verified windows have no packs and zero payload/index bytes.

On an error, consumed-input and completed-pack counters are partial observations,
not the complete window size or the amount by which the limit was exceeded. In
particular, a rejected record can consume input without contributing accepted
physical rows. The report retains only counters and does not retain payloads or
export sample values. Preflight still consumes source CPU and network bandwidth.

## Reproducible small examples

The focused fixture contains one series with five physical samples, including a
duplicate timestamp and one row outside the logical interval. It verifies three
deduplicated in-window samples. Run the successful and deliberately over-limit
examples without a VM deployment:

```sh
go test -mod=vendor ./internal/archive -run \
  'TestPreflightMeasuresVerifiedWindowAndEmptyWindow|TestPreflightRejectsIncompleteAndOverBudgetObservations' -v
go test -mod=vendor ./cmd/vmtier -run TestPreflightCommand -v
```

The first successful report has `verified: true`, `unique_series: 1`,
`physical_samples: 5`, `verified_deduplicated_samples: 3`, and one pack. The pinned
fixture consumes 64 native input bytes and emits 29 data bytes plus a 527-byte
index; the test prints the full report. The empty case reports zero packs. The
`physical-limit` case deliberately supplies a one-sample verifier limit to the
library and reports `verified: false` with a physical-sample limit error. The
`record-limit` case supplies a one-byte pack limit, so a complete record cannot
fit. These reduced limits are test inputs, not production configuration knobs.
The CLI test also verifies that existing state remains byte-for-byte intact and
that an absent catalog remains absent without contacting the configured S3 store.

## Decisions and limits

A successful observation establishes only that this particular source response
fits the selected bounded pipeline and passes completeness checks. It does not
measure upload/read-back, object quotas, concurrent queries, cache pressure,
production peak RSS, sustained catch-up throughput, or future cardinality. Retain
the JSON alongside workload/host details and separately measure recovery headroom.

If a representative window fails, first determine whether an individual record,
pack count, physical samples, source completeness, or attempt time caused it.
Changing the candidate window can help some limits before initialization; it
cannot make an oversized individual native record valid. A window is immutable
once bound to a catalog. Do not edit a live catalog's `window` or coverage origin
to satisfy an error message. A different window policy requires a new catalog and
an explicit migration/reinitialization plan with source history still available;
preflight does not perform that migration or discard existing state.
