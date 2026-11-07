# Security policy

vmtier holds S3 and database credentials, decodes data that it reads from object
storage and from VictoriaMetrics HTTP responses, and serves archived samples to
vmselect. Its deployment model has deliberate trust boundaries, documented in
[compatibility](docs/compatibility.md#rpc-and-http-support) and
[guarantees and limits](docs/limits.md):

- The vmselect RPC listener is not an authentication boundary. Run it on a
  trusted network and authorize tenants at the HTTP entry point of stock
  vmselect.
- The legacy JSON v2 catalog HTTP API is an internal API with one shared bearer
  token; put it behind authenticated TLS.
- The catalog (SQLite directory or PostgreSQL database) is authoritative, and
  object contents are verified against the SHA-256 checksums it records.

Weaknesses of that model that are already documented are not vulnerabilities,
but ideas to reduce them are welcome as regular issues. Examples of what is in
scope: a query or tenant filter returning another tenant's samples, an object
key escaping its tenant namespace, a crafted pack, index, export or oracle
response causing memory exhaustion beyond the documented limits, a crash or
silent acceptance of corrupt data, a failed attempt becoming visible as
published coverage, or credentials appearing in logs, metrics or errors.

## Supported versions

vmtier is under development and no release has been published yet. Security
fixes are made on the `main` branch; once releases exist, only the latest
release is supported.

## Reporting a vulnerability

Please do **not** open a public issue, pull request or discussion.

Report the vulnerability privately with GitHub's
[private vulnerability reporting](https://github.com/yuuki/vmtier/security/advisories/new)
(the "Report a vulnerability" button of the Security tab). If that is not
possible, contact the author privately via the contact information on
https://github.com/yuuki.

Please include the vmtier commit or version (`vmtier version`), the
VictoriaMetrics version, the catalog backend, the configuration with
credentials removed, what an attacker controls (network access to the RPC or
HTTP listeners, the object store, the source vmselect, the database, ...), the
steps to reproduce, and the impact. Keep private metric data out of the report
unless it is needed. You should receive an answer within a few weeks; vmtier is
maintained on a best-effort basis.

## Verifying releases

Release archives are reproducible static binaries with signed build provenance
and SBOM attestations. See [releases](docs/releasing.md#verify-a-release-archive).
