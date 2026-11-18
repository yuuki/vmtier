# ADR 0010: Lateness runs until Hot serves the sample

## Status

Accepted. It changes the definition of `lateness` in the documentation and in
the comments of `spec/lean`; configuration keys, validation, the catalog
contract and the archiver are unchanged.

## Context

A window is archived once it is older than the archive age `H`, and the time
contract requires `H > L`, where `L` was described as the longest delay that
ingress accepts. The argument for a complete window was that every sample
ingress accepts for it was accepted before the export began.

Accepted is not searchable. Stock VictoriaMetrics returns a sample from a
query or an export only after vminsert has delivered it to vmstorage and
vmstorage has flushed its in-memory buffers. On an idle single node of the pinned release with default flags, the
time from vminsert's HTTP 204 to the sample appearing in `/api/v1/export` was
2 to 4 seconds for an existing series and 4 to 11 seconds for a new one, in two
independent measurements. Stock vmselect hides the last 30 seconds from
queries by default (`-search.latencyOffset`) for the same reason. A loaded or
degraded cluster takes longer.

The native export, the raw oracle and the count verification all read the same
Hot state, so a sample that Hot accepted but does not serve yet is missing from
all three, they agree, and the window is published without it. A published
window is never exported again. Configuration accepts any `H > L`, down to a
difference of one millisecond.

## Decision

- `L` bounds the delay from a sample's timestamp `t` until every Hot source
  returns the sample: no later than `t + L` on the clock of the host that runs
  the archiver. It includes ingress queues and retries, Hot's delay between
  accepting and serving a write, and the clock difference between ingress and
  the archiver. `lateness_enforced` asserts this bound.
- `H - L` is the only margin for a delay that `L` leaves out. The documentation
  gives the measured magnitude and asks for minutes or more.
- An existing catalog needs no change when its `H - L` already covers the
  delay. `H` and `L` are fixed in the contract, so otherwise the operator makes
  ingress reject samples that much earlier than `L`.
- Validation stays `H > L`.

## Alternatives

| Alternative | Reason not selected |
| --- | --- |
| A new configuration key for the accept-to-searchable delay and clock skew, added to the eligibility condition | A second operator-asserted number with the same effect as a larger `H` or a smaller `L`; it would be a new field of the fixed catalog contract |
| Refuse `H - L` below a floor | The delay depends on Hot's load, topology and clocks, so any floor is arbitrary; catalogs initialized with a smaller difference cannot change it; and the harnesses rely on small values with a forced flush |
| Warn at startup below a threshold | Same arbitrary threshold, and vmtier cannot observe the real delay |
| Export a window twice, some time apart, and compare | Doubles the export cost of every window and only moves the question to the length of the pause |

## Consequences

The completeness of a published window still rests on an operator assertion
that vmtier cannot check: a sample that Hot serves later than `t + L` is not
added to Cold, and nothing reports it. The stock-VM harnesses flush Hot before
archiving, so they do not exercise this boundary.
