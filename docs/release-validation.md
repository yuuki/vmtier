# Release validation

This report records the release-pipeline evidence for
[issue #19](https://github.com/yuuki/vmtier/issues/19) at commit
[`e498b19364b5e115ce57990004e98a55bff35dc9`](https://github.com/yuuki/vmtier/commit/e498b19364b5e115ce57990004e98a55bff35dc9)
on `main`, before any version tag. Untagged builds are named
`v0.0.0-dev.ge498b19364b5`; a tag changes the version string, so these bytes
are not the bytes of a release. The runs of the tags follow:
[`v0.1.0-rc.1`](#tag-v010-rc1) stopped before attestation, and
[`v0.1.0-rc.2`](#tag-v010-rc2) passed every gate, attested its files, and
created a draft pre-release. [Releases](releasing.md) describes the pipeline
and the support scope.

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
| [42369091732](https://github.com/yuuki/vmtier/actions/runs/42369091732) | `e498b19` (2026-11-07 11:39–11:59 UTC) | `build (first)`, `build (second)`, `reproducibility`, `qualify`: success; `publish`: skipped (no tag) |

The two builds ran on separate runners. `reproducibility` reported
`reproducible: 3 artifacts match`. Both uploaded builds (`dist-first`, ID
`13572947735`; `dist-second`, ID `13572659572`) were downloaded and compared with
the local rebuild: all three files match, and `SHA256SUMS` again has SHA-256
`aa484f1f…dcaf3f`. Five builds of `e498b19` in three kinds of environment have
therefore produced the same bytes.

`qualify` installed the verified binary, passed `govulncheck -mode=binary`, and
ran the stock-VM suites against it. Artifact `release-e2e-evidence` (ID
`13573313601`, ZIP SHA-256
`f43c8668dba0891bdf5020d1a2f0c030c8510f3da5bc25b5bbad290ae07fc99f`, inner
`e2e-evidence.tar.gz` SHA-256
`23894324220a633e3a56d95a11e769989fa06c5a3ebbf51484c7a12b69da32c5`) was
downloaded and extracted with the safety checks of
[CI evidence packaging](ci-evidence-validation.md). Every report says `passed`,
and every report's provenance records vmtier binary SHA-256 `6f15664c…d6a893`,
the packaged binary:

| Suite | Exact comparisons | Report SHA-256 |
| --- | ---: | --- |
| `ci-workload` (workload qualification smoke) | 40 | `fa2cdd8cbd504d84b3a3e0bc0c31ee513d0dd04e4e4e9176c403de1112850795` |
| `ci-local` (JSON v2, local) | 360 | `096a9c74b209abe1d3ff26fa701cf989cc5a44209c9bec5b9bd74b7855a759a2` |
| `ci-s3` (JSON v2, S3 test double, 2 replicas) | 360 | `b8c0e1fa864a0560b992bcd787f84cc70cf4728dcc077d85c473b9242e57fe36` |
| `ci-serve-local` (SQLite `serve`, local) | 360 | `618de4aa25e6f84784534120437c33ea62b49c06295400a5f6e52e56bb4c0ef0` |
| `ci-serve-s3` (SQLite `serve`, S3 test double, 2 replicas) | 360 | `c5af66c686673c25fb6d134a63d092cc94a95da4ec20094671775a92d69ac3c3` |
| `ci-recovery-local` (offline recovery and JSON v2 cutover) | 480 | `8f93e0045108ef65c790ae8483c76a69149b8b7ae5e81b77eef42c999725deff` |
| `ci-postgres-ha` (PostgreSQL, fenced archivers, gateway failover) | 360 | `28743735c644f0c8770ab0c2db6aa168e9094e78721c608f8a881990ca02c40b` |
| `ci-postgres-migration` (stopped SQLite to PostgreSQL) | 360 | `888aa767555bfabca6f99905f3e23360f287dc25fb412308f58e6a90113b356b` |

The S3 suites use a local Moto server behind a compatibility proxy; they are not
a provider qualification. The artifact is retained until 2027-02-05.

## Tag `v0.1.0-rc.1`

The maintainer pushed `v0.1.0-rc.1` on merge commit
[`966c94e`](https://github.com/yuuki/vmtier/commit/966c94e963e6e9d50c669bd3d7f3ffcf4cfb6f45),
whose `test`, `security`, `PostgreSQL fault qualification`, and
`Code Complexity` runs had passed. The tag is unsigned.

| Run | Jobs |
| --- | --- |
| [42983424801](https://github.com/yuuki/vmtier/actions/runs/42983424801) (2026-11-11 07:58 UTC) | `build (first)`, `build (second)` (main ancestry check, build, smoke), `reproducibility`: success; `qualify`: failure; `publish`: skipped |

The smoke test reported binary SHA-256
`d23638aaf034e79bbca686a955b552b546e503060239730f391b90c2a00e7248`.
`qualify` then stopped at `govulncheck -mode=binary`, before any stock-VM suite:
[GO-2026-6859](https://pkg.go.dev/vuln/GO-2026-6859), an HTTP/1 server crash
due to a `MaxBytesReader` header race in `net/http`, found in Go 1.27.3 and fixed
in 1.27.4, with 149 reachable symbols. The advisory appeared after the
dry run above. `publish` did not run, so nothing was attested and no draft or
release exists. The candidate is not released; the next one, `v0.1.0-rc.2`,
is built with Go 1.27.4.

## Tag `v0.1.0-rc.2`

The maintainer pushed `v0.1.0-rc.2` on squash commit
[`903be7e`](https://github.com/yuuki/vmtier/commit/903be7e87db9ce18df100d88b9fc9b857babc951)
(#41), whose `test`
([43020426369](https://github.com/yuuki/vmtier/actions/runs/43020426369)),
`security`, `PostgreSQL fault qualification`, and `Code Complexity` runs had
passed. The tag is annotated and unsigned.

| Run | Jobs |
| --- | --- |
| [43130380192](https://github.com/yuuki/vmtier/actions/runs/43130380192) (2026-11-12 00:51–01:14 UTC) | `build (first)`, `build (second)` (main ancestry check, build, smoke), `reproducibility`, `qualify`, `publish`: success |

Both builds smoke-tested binary SHA-256
`0a9955438171b315579b557e5b94b3edbb1a3148c2d97fb7bf61211ca25eda6e`
(14,685,112 bytes), and `reproducibility` reported
`reproducible: 3 artifacts match`. The recorded `SHA256SUMS` has SHA-256
`54f0dc2c646b7211b13c4c719159f586aaa17d4d4b0b549371c5410d9b6ab231`:

```text
7fe62dafe6d480c61f4256f197d16ebf6c0452de094e7d303fc32ededd6a7274  vmtier-v0.1.0-rc.2-linux-amd64.build.json
5eb5b595d7f026ca57604d4c7a61aab32dc9f09164b22d7064ffe090313f5915  vmtier-v0.1.0-rc.2-linux-amd64.cdx.json
b64a3e71949dc553897c35ccd0403734e5f86aad93c985d6d82b78e5fcc2377f  vmtier-v0.1.0-rc.2-linux-amd64.tar.gz
```

The `dist-first` artifact (ID `13904254509`, ZIP SHA-256
`7859855cc2065628a2321380d548c1f36447fb3dcc8fe1c4ec643a691d8a666c`) was
downloaded, and `python3 scripts/release.py verify --sums-sha256 54f0dc2c…`
accepted it. Its build record names commit `903be7e` and the binary above.

`qualify` installed the verified binary, and `govulncheck -mode=binary`
(v1.9.0) reported `No vulnerabilities found.`, the gate that stopped rc.1. It
then ran the stock-VM suites. Artifact `release-e2e-evidence` (ID
`13905987646`, ZIP SHA-256
`b200845670006dfebc5b11dc150d0befe4d9119cd1c9f5651f3374bc603f5e1e`, inner
`e2e-evidence.tar.gz` SHA-256
`9a0e0ee9874e8be116389eb503bb65565c3658a88ffa3cdf56c05e5bb2048965`, 2,601
members) was downloaded and extracted with the `tarfile` data filter. Every
report says `passed`, and every report's provenance records vmtier binary
SHA-256 `0a995543…25eda6e`, the packaged binary:

| Suite | Report SHA-256 |
| --- | --- |
| `ci-workload` (workload qualification smoke, 40 exact comparisons) | `5609fb5345407d9dffdb35451d98d03ea88e7fc4ce0f15ac367a89cf4e8e8464` |
| `ci-local` (JSON v2, local) | `e907690e5eb503dd6d27a081485e68651683e494c3ae9905427676009d3c55fc` |
| `ci-s3` (JSON v2, S3 test double, 2 replicas) | `d2f80568c08b693b96b74eb124a15ba8b8c90d18409859b79f29ac2b534e75e0` |
| `ci-serve-local` (SQLite `serve`, local) | `05cc066bf96ab1f9e31e23f4c45b33d89106fb7435dd0e2f60021da660eab96d` |
| `ci-serve-s3` (SQLite `serve`, S3 test double, 2 replicas) | `4ef65d5713af062b22c47ec9c15a90ce6991e50af1ebf960ca56e18e110cbbc0` |
| `ci-recovery-local` (offline recovery and JSON v2 cutover) | `90d58ddb98942bc3a5343cd62c4955cef00c647d511a4350c678396534eedfa0` |
| `ci-postgres-ha` (PostgreSQL, fenced archivers, gateway failover) | `0c288d643ad4192d12482cffb1be6e363b55d9c25fe8536f53b14b09a5fd2d3c` |
| `ci-postgres-migration` (stopped SQLite to PostgreSQL) | `a3b40adb05773ee613ec8f3eed0c2bef6e80be64ac471c878ad3613700711730` |

The artifact is retained until 2027-02-10.

`publish` accepted only the files whose `SHA256SUMS` had the recorded SHA-256,
then made two attestations, signed through Sigstore:

| Attestation | Subjects | Rekor |
| --- | --- | --- |
| [SLSA build provenance](https://github.com/yuuki/vmtier/attestations/66493895) | the three files of `SHA256SUMS` | [log index 3821245003](https://search.sigstore.dev?logIndex=3821245003) |
| [CycloneDX SBOM](https://github.com/yuuki/vmtier/attestations/66493904) | `vmtier-v0.1.0-rc.2-linux-amd64.tar.gz@sha256:b64a3e71…cc2377f` | [log index 3821245206](https://search.sigstore.dev?logIndex=3821245206) |

It then created the draft pre-release `vmtier v0.1.0-rc.2` with the four
files. Publishing the draft is the maintainer's step; after it, verify the
published assets from a clean directory with the commands of
[verify a release archive](releasing.md#verify-a-release-archive).

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
| Smoke test, maintainer procedure, deployment example | `smoke` in every build job, stock-VM suites against the packaged binary in `qualify`, [maintainer procedure](releasing.md#maintainer-procedure), systemd example above | Met |
| Evidence before claiming release support; excluded areas | [Evidence index](evidence.md), this report; Sakura (#15), natural retention (#16), and enforced ingress lateness (#17) remain open | Not met: pre-releases only |

The run, checksums, and attestations of the first candidate that passed every
gate, `v0.1.0-rc.2`, are recorded [above](#tag-v010-rc2). Its publication
remains a maintainer action (see [first release](releasing.md#first-release)),
and it is a pre-release: the last criterion is still not met.
