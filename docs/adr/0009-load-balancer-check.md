# ADR 0009: A load balancer check separate from readiness

## Status

Accepted and implemented for `serve`. It adds the HTTP endpoint `/serving` and
changes the health check of the [HA example](../../examples/ha/README.md);
`/health`, `/ready` and the catalog are unchanged.

## Context

`/ready` fails while any configured stream lacks required coverage. The
required end, `floor((now - H) / w) * w`, moves forward by one window at the
instant that window becomes eligible for archiving, so `/ready` fails from then
until the window is published: at least one archive interval plus the export,
on every window boundary, and for as long as an archive failure lasts.

Coverage is catalog state, so every gateway of one catalog fails `/ready` at
the same time. The HA example removed gateways on `/ready` and closed their
sessions. Both gateways therefore left the proxy together, stock vmselect lost
its Cold storage node, and with partial responses denied every query failed,
including queries over Hot only and over published history. The gateway itself
would have failed only the queries that intersect the missing range
(`cold.Reader.Search`).

## Decision

- `/serving` returns 200 when the process passes the catalog check that every
  query runs first (`cold.Reader.CatalogStatus`: the catalog answers, and its
  contract and blob locator match the configuration), and 503 with the error
  otherwise. It does not look at coverage.
- A load balancer, or a Kubernetes readiness probe, uses `/serving`. A gateway
  that cannot reach the catalog fails all of its queries while another gateway
  may still answer, which is the failure a per-gateway check can route around.
- `/ready` keeps its meaning. It remains the signal for monitoring
  (`vmtier_ready`, the per-stream lag metrics; both show the short lag of every
  window boundary, so an alert needs a duration) and for gating traffic after a
  restore, where an operator waits for catch-up on purpose.

## Alternatives

| Alternative | Reason not selected |
| --- | --- |
| Keep `/ready` in the load balancer and lengthen `fall` | The outage only becomes shorter or rarer; a slow export or a failed source still removes every gateway |
| Check `/health` | A gateway cut off from the catalog would stay in rotation and fail its share of the queries |
| Remove the coverage condition from `/ready` | Changes a published endpoint that the recovery procedure and the harnesses rely on |
| Give `/ready` a grace period after a window becomes eligible | Any fixed period is wrong for some export duration, and it would hide a real gap from monitoring |

## Consequences

Archive lag no longer removes gateways: queries over the missing range fail
with the gateway's coverage error and all others are answered. Deployments
that copied the earlier example should change their check to `/serving`.
The check does not cover the object store; a gateway that cannot read S3 still
passes it, as it passed `/ready` before.
