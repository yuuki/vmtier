# ADR 0004: Reproducible static release artifacts

## Status

Accepted. The builder, packaging script, CI check, and tag workflow are
implemented; no release has been published. The procedure and support scope are
in [releases](../releasing.md). This addresses the artifact part of #19.

## Context

Delivery step 5 of the [first-release design](../design.md#delivery-and-acceptance)
requires reproducible binaries, redistribution notices, and a support statement.
vmtier needs CGO for the bundled SQLite and links VictoriaMetrics' `gozstd`, which
ships prebuilt zstd archives. A dynamically linked binary from a CI runner
depends on that runner's glibc version. Statically linking glibc avoids that
dependency, but adds LGPL relinking obligations and glibc's static-linking
caveats. Operators also need a way to verify where a downloaded binary came from
and what it contains, without trusting the person who uploaded it.

## Decision

- Ship one platform, `linux/amd64`, as a fully static binary linked against musl
  (MIT) with tags `netgo,osusergo,musl`. `gozstd` then selects its musl archive,
  and DNS and user lookup stay in Go.
- Build in `release/Dockerfile`, based on the official Go Alpine image pinned by
  digest. Compile `git archive` output of the tagged commit with `-trimpath`,
  `-buildvcs=false`, version and commit set through `-X`, and timestamps derived
  from the commit time. Record the toolchain and package versions in `BUILD.json`.
- Require two independent hosted builds with identical `SHA256SUMS` before any
  release, and run the same comparison on every push in CI.
- Run the stock-VM differential suites, `go test` with release tags and static
  musl linking, and `govulncheck` against the packaged binary rather than the
  development build.
- Package every linked module's license and notice files, the Go license, and the
  musl notice. Keep reviewed SPDX expressions in `release/licenses.json`, and fail
  when the linked module set and the record differ.
- Publish a CycloneDX SBOM, SLSA provenance, and an SBOM attestation through
  GitHub artifact attestations (Sigstore). Create only a draft release, in a
  protected `release` environment, so publishing stays a maintainer action.
- Implement packaging in a standard-library Python script with unit tests,
  consistent with the existing harnesses, rather than adding a release tool.

## Alternatives

| Alternative | Reason not selected now |
| --- | --- |
| Dynamic glibc binary from the CI runner | Ties deployments to the runner's glibc release |
| Static glibc, as upstream VM images use | LGPL relinking obligations and NSS/`dlopen` caveats |
| Pure-Go SQLite or `CGO_ENABLED=0` | Changes the qualified SQLite implementation and VM's zstd path |
| GoReleaser or another release tool | CGO cross-compilation is not needed for one platform; a small script keeps the recipe visible and testable |
| Pinning each Alpine package version | Alpine removes superseded packages, so pinned builds would stop working; versions are recorded and compared instead |
| Separate signing keys (GPG, cosign key pairs) | Requires key custody; keyless attestations tie artifacts to the workflow identity |

## Consequences

Release builds need Docker and network access to the module proxy, the checksum
database, and Alpine mirrors. A later rebuild can differ if Alpine has updated
GCC, binutils, or musl. The build record shows which input changed. Adding a
dependency, changing musl, or changing Go requires updating the license record,
the notice, or the builder pin. Other platforms require their own qualification.
Attestations are verified with `gh attestation verify` or another Sigstore
verifier, and are only as trustworthy as the repository's tag and environment
protections.
