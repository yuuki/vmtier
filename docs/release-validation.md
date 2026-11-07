# Release validation

This report records the release-pipeline evidence for
[issue #19](https://github.com/yuuki/vmtier/issues/19) at commit
[`e498b19364b5e115ce57990004e98a55bff35dc9`](https://github.com/yuuki/vmtier/commit/e498b19364b5e115ce57990004e98a55bff35dc9)
on `main`, before any version tag. Untagged builds are named
`v0.0.0-dev.ge498b19364b5`; a tag changes the version string, so these bytes
are not the bytes of a release. No release has been published and no
attestation has been made. [Releases](releasing.md) describes the pipeline and
the support scope.

## Hosted `release-check`

The `release-check` job of the `test` workflow builds the archive twice in the
pinned builder, compares the checksum lists, and smoke-tests the first build.
It passed on both pushes to `main` that carried the release tooling:

| Run | Commit | Job | Result |
| --- | --- | --- | --- |
| [42347369206](https://github.com/yuuki/vmtier/actions/runs/42347369206) | `e6d7d3e` (2026-11-07 06:31 UTC) | [release-check](https://github.com/yuuki/vmtier/actions/runs/42347369206/job/128327913570) | success |
| [42368442498](https://github.com/yuuki/vmtier/actions/runs/42368442498) | `e498b19` (2026-11-07 11:30 UTC) | [release-check](https://github.com/yuuki/vmtier/actions/runs/42368442498/job/128391384497) | success |

The `release-snapshot` artifact of run 42368442498 (ID `13572633485`, ZIP
SHA-256 `fe9b4a9b78494fe573b934dc37cac1b279547dabfe921e4622e476e7b97a81f0`)
holds the first build. Its checksum list and build record are retained under
[`results/release/e498b19/`](results/release/e498b19/):

```text
0d30e9b9f1e17520cc6a9ffbff2d388637a0c9ea782e2f13c6b53cf0f944c448  vmtier-v0.0.0-dev.ge498b19364b5-linux-amd64.build.json
3186af3602ca81f7b6506ea5856cb03b32be8229cf24efada1bcfa6e5c1a3657  vmtier-v0.0.0-dev.ge498b19364b5-linux-amd64.cdx.json
fe56a0323376c85b5aab2d9980b585d9803f570aee5835d5d6fc1b2bec6abd86  vmtier-v0.0.0-dev.ge498b19364b5-linux-amd64.tar.gz
```

`SHA256SUMS` itself has SHA-256
`aa484f1fc876806a3aa465889319143271f78aa9070fa309cbe3df314dfcaf3f`. The binary
has SHA-256 `6f15664c3c37d26b7cbec632890e0b504fc3876205fabe3347e40ab860d6a893`
and 14,520,472 bytes.

## Independent rebuild in another environment

The same commit was rebuilt on 2026-11-07 in a Claude Code cloud container
(x86-64, 4 CPUs, Docker 29.9.2, Python 3.13.18 with zlib 1.3), outside GitHub
Actions. Its network passes through a TLS-intercepting proxy, so the builder was
`release/Dockerfile` with the proxy CA appended to the image's trust bundle,
passed with `--builder-image`, and `VMTIER_RELEASE_DOCKER_RUN_ARGS` set to
`--network host -e HTTPS_PROXY=…`. The base digest, Alpine 3.24.4 packages
(`gcc-15.2.0-r5`, `libgcc-15.2.0-r5`, `musl-1.2.6-r3`, `musl-dev-1.2.6-r3`,
`binutils-2.45.1-r1`), and Go environment in the build record equal the hosted
build's.

`python3 scripts/release.py compare` against the downloaded `release-snapshot`
reported `reproducible: 3 artifacts match`: the archive, SBOM, and build record
are byte-identical, although the packaging Python differed (3.15.2 on the
runner). `smoke` passed on the local build.

## Release workflow dry run

A `workflow_dispatch` run of `release.yml` on `main` exercises every gate except
`publish`, which runs only for a pushed tag:

| Run | Commit | Jobs |
| --- | --- | --- |
| [42369091732](https://github.com/yuuki/vmtier/actions/runs/42369091732) | `e498b19` (2026-11-07 11:39 UTC) | `build (first)`, `build (second)`, `reproducibility`: success; `qualify`: running when this was written |

The two builds ran on separate runners. `reproducibility` reported
`reproducible: 3 artifacts match`. Both uploaded builds (`dist-first`, ID
`13572947735`; `dist-second`, ID `13572659572`) were downloaded and compared with
the local rebuild: all three files match, and `SHA256SUMS` again has SHA-256
`aa484f1f…dcaf3f`. Five builds of `e498b19` in three kinds of environment have
therefore produced the same bytes. `qualify` installed the verified binary and
passed `govulncheck -mode=binary` before the stock-VM suites started.

## Archive contents

The archive has 142 entries. Beside the binary, the top-level `LICENSE`,
`NOTICE`, `THIRD_PARTY_NOTICES.md`, `README.md`, and the example configurations,
it carries 48 license, notice, and patent files of the 28 linked Go modules
(9 direct), reviewed header notices of the bundled zstd 1.5.7 code, the Go
`LICENSE`, `PATENTS`, and `memmove_amd64.s` notice, and the musl 1.2.6
`COPYRIGHT`. The build record lists the SPDX expression and `go.sum` hash of every
module and the embedded SQLite 3.54.1 and zstd 1.5.7 libraries; the CycloneDX SBOM
lists the same components. Commits after `e498b19` also package the
[systemd example](../examples/systemd/vmtier.service).

## systemd deployment example

The [install procedure](getting-started.md#install-a-release-archive) and the
example unit were run on 2026-11-07 under systemd 252 as PID 1, in a privileged
`almalinux/9-init` (AlmaLinux 9.8) container on the host above. The binary came
from the local build of `e498b19`; that archive predates the systemd files, so
`vmtier.service` and `vmtier.env` were copied from the working tree into
`examples/systemd/` of the extracted archive. The test configuration replaced
the S3 store with a local store under `/var/lib/vmtier`, used an absolute
catalog path, set `lateness_enforced` to true (catalog initialization refuses
false), and pointed the source at an address with no listener.

| Check | Result |
| --- | --- |
| Install steps, `catalog-init` as `vmtier`, `systemctl enable --now` | Unit active; catalog and lock files owned by `vmtier` with mode 0600 in 0700 directories |
| `/health`, `/ready`, `/metrics` with no window due | `ok`, 200, `vmtier_ready 1`, `vmtier_build_info{version="v0.0.0-dev.ge498b19364b5",…}` |
| Reinitialized with windows due and an unreachable source | Archive attempts reach the network and fail with `connection refused`; `vmtier_ready 0`; `vmtier_archive_failed_windows_total{…,reason="error"} 1` per tenant |
| `vmtier inspect` while the unit runs | Refused: `catalog state directory is already in use` |
| `systemctl stop` | SIGTERM drain; `Result=success`, exit status 0 |
| `systemd-analyze verify`, `systemd-analyze security` | No findings; exposure level 1.4 ("OK") |

This checks that the sandbox permits the service's file, network, and signal
behavior. It does not archive real data, contact S3, or run on a full host;
the release workflow's `qualify` job exercises the packaged binary against
stock VM.

## Status against issue #19

| Criterion | Evidence | Status |
| --- | --- | --- |
| Supported platform and pinned inputs (Go, C toolchain, CGO, SQLite, VM revision) | [Support scope](releasing.md#support-scope), [build inputs](releasing.md#build-inputs), build record | Met |
| Clean repeatable builds, checksums, build recipe, limitations | Hosted `release-check`, the cross-environment rebuild above, retained build record, [reproducibility limits](releasing.md#reproducibility-limits) | Met |
| License, upstream notices, linked dependency information | Archive contents above; `release/licenses.json` gate | Met |
| Smoke test, maintainer procedure, deployment example | `smoke` in every build job, [maintainer procedure](releasing.md#maintainer-procedure), systemd example above | Met |
| Evidence before claiming release support; excluded areas | [Evidence index](evidence.md), this report; Sakura (#15), natural retention (#16), and enforced ingress lateness (#17) remain open | Not met: pre-releases only |

The first tagged release, its attestations, and its publication remain
maintainer actions (see [first release](releasing.md#first-release)). Record the
tag's run and checksums here once they exist.
