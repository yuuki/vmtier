# Releases

A vmtier release is a static `linux/amd64` binary archive built from one tagged
commit in a digest-pinned container. Two independent builds have to match
byte for byte. The packaged binary then has to pass the stock-VM differential
suites, and the workflow publishes signed provenance and SBOM attestations. The
release workflow saves a **draft**; publishing it is a separate maintainer action.
[ADR 0004](adr/0004-release-artifacts.md) records why the pipeline is shaped
this way.

No release has been published yet. This page describes the tooling and the
procedure. Each release's notes must state the support scope below.

## Support scope

| Area | Released artifact |
| --- | --- |
| Platform | Linux on x86-64 (`GOAMD64=v1`), statically linked against musl; no glibc version requirement |
| VictoriaMetrics | OSS `v1.155.0-cluster`, commit `cab3d5fbeeedd5d0374c1dabde93939af4c2499c` only ([compatibility](compatibility.md)) |
| Runtime | One `vmtier serve` owner with a local SQLite catalog ([limits](limits.md)) |
| Evidence | Local differential, recovery, workload, and catalog reports in the [evidence index](evidence.md), plus the release run's `release-e2e-evidence` artifact |

The following remain unqualified: real Sakura Object Storage (#15), natural Hot
retention expiry (#16), and enforced ingress lateness (#17). HA, GC, Cold TTL,
backfill, other VM versions, and other platforms are outside the first-release
scope. Until those issues close with recorded evidence, tag releases as
pre-releases (`v0.1.0-rc.1`) and do not describe them as production-supported.

## Release contents

| Asset | Contents |
| --- | --- |
| `vmtier-VERSION-linux-amd64.tar.gz` | `vmtier`, `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md`, `README.md`, `examples/serve-sakura.json`, `BUILD.json`, `sbom.cdx.json`, `go-version-m.txt`, and `licenses/` |
| `vmtier-VERSION-linux-amd64.build.json` | Build record: commit, `SOURCE_DATE_EPOCH`, builder digest, Alpine package versions, Go environment, exact build command, binary SHA-256, and every linked module with its `go.sum` hash, SPDX license expression, and notice files |
| `vmtier-VERSION-linux-amd64.cdx.json` | CycloneDX 1.6 SBOM of the binary: linked Go modules, the Go standard library, and musl |
| `SHA256SUMS` | SHA-256 of the three files above |
| Attestations | SLSA build provenance for every file in `SHA256SUMS` and an SBOM attestation for the archive, signed through Sigstore and stored by GitHub |

`licenses/` contains each linked module's license, notice, and patent files,
nested ones included (such as the BSD license of the zstd library in `gozstd`).
It also has the Go distribution license and the musl `COPYRIGHT`.
[`release/licenses.json`](../release/licenses.json) holds the reviewed SPDX
expression for each linked module. The build fails if the linked module set
differs from that record.

## Build inputs

| Input | Pin |
| --- | --- |
| Source | `git archive` of the tagged commit, so uncommitted and untracked files cannot enter the build |
| Builder | [`release/Dockerfile`](../release/Dockerfile): `golang:1.27.3-alpine3.24` pinned by digest, with `gcc` and `musl-dev` from Alpine |
| Go | 1.27.3, `GOTOOLCHAIN=local`; [`release/build.sh`](../release/build.sh) fails unless it equals the `go` directive in `go.mod` |
| Modules | `go.mod`/`go.sum`, fetched from the module proxy, checked against the checksum database, `go mod verify`, then vendored |
| C toolchain | GCC, binutils, and musl versions are recorded in the build record; the builder refuses a musl version without a matching notice |
| Flags | `CGO_ENABLED=1`, tags `netgo,osusergo,musl`, `-trimpath -buildvcs=false`, `-ldflags "-s -w -X main.version=… -X main.commit=… -linkmode=external -extldflags=-static"` |
| Timestamps | `SOURCE_DATE_EPOCH` is the commit time; archive entries use it with fixed owners and modes, and gzip omits name and time |

`vmtier version` and the `vmtier_build_info` metric report the release version,
commit, and Go version. Development builds report the module version and VCS
revision that the Go toolchain stamps.

## Verify a release archive

Download all assets of one release into an empty directory, then:

```sh
sha256sum --check SHA256SUMS
gh attestation verify vmtier-VERSION-linux-amd64.tar.gz --repo yuuki/vmtier
gh attestation verify vmtier-VERSION-linux-amd64.tar.gz --repo yuuki/vmtier \
  --predicate-type https://cyclonedx.org/bom
tar -xzf vmtier-VERSION-linux-amd64.tar.gz
vmtier-VERSION-linux-amd64/vmtier version
```

The provenance names the workflow file, tag, commit, and run that built the
assets. To rebuild independently, check out the tag on Linux with Docker,
Python 3, and git, then compare against the downloaded directory:

```sh
git checkout VERSION
python3 scripts/release.py build --output /tmp/vmtier-rebuild
python3 scripts/release.py compare /path/to/downloaded /tmp/vmtier-rebuild
```

### Reproducibility limits

- The Dockerfile pins the base image by digest, but Alpine mirrors serve only the
  current package release in a branch. A later rebuild can resolve newer GCC,
  binutils, or musl packages. Compare `builder.packages` in both build records
  before you treat a mismatch as a source difference.
- Archive bytes also depend on the zlib that the packaging Python uses. If only
  the `.tar.gz` checksum differs, compare `binary.sha256` in the build records.
- Builds need network access to the Go module proxy, the checksum database, and
  Alpine mirrors. Modules are not mirrored by this project.

## Maintainer procedure

1. Confirm that `main` passes the `test` and `security` workflows, including the
   `release-check` job that builds and compares two snapshots. Update
   `docs/evidence.md` and the support scope above if qualification changed.
2. If dependencies changed, review the new license files, update
   `release/licenses.json` and `THIRD_PARTY_NOTICES.md`, and run
   `make release-snapshot` locally.
3. Choose a SemVer tag. Tags with a hyphen (`v0.1.0-rc.1`) become GitHub
   pre-releases. To promote a candidate, tag the same commit with the final
   version; the workflow builds the pushed tag. Never move or reuse a tag that
   was published.
4. Tag the commit on `main` and push the tag:

   ```sh
   git tag -s v0.1.0-rc.1 -m "vmtier v0.1.0-rc.1"
   git push origin v0.1.0-rc.1
   ```

5. The [release workflow](../.github/workflows/release.yml) runs:

   | Job | Gate |
   | --- | --- |
   | `build (first)`, `build (second)` | Each requires the tagged commit to be on `main`, then separate runners build the archive in the pinned builder; the first also runs `go test` with release tags and static musl linking; both smoke-test the archive |
   | `reproducibility` | `SHA256SUMS` of both builds must be identical |
   | `qualify` | Installs the packaged binary as `.bin/vmtier`, scans it with `govulncheck -mode=binary`, and runs the workload smoke, JSON v2, `serve`, and recovery suites against stock VM |
   | `publish` | Runs in the `release` environment: re-verifies the assets, attests provenance and SBOM, and creates the draft release with generated notes |

6. Review the draft: notes and support scope, the four assets, the attestation
   links in the run summary, and the `release-e2e-evidence` artifact. Then
   publish the draft. If a gate fails before publication, delete the draft and
   the tag, fix the cause on `main`, and tag a new version.

`workflow_dispatch` runs the same build, reproducibility, and qualification gates
on any ref without attesting or creating a release.

### Repository settings

These GitHub settings protect the procedure; the workflows do not configure them:

- Enable **immutable releases**, so published assets and their tags cannot change.
- Add a tag ruleset that limits creating, updating, and deleting `v*` tags to
  maintainers, and require signed tags if maintainers sign.
- Add required reviewers to the `release` environment, so the `publish` job,
  the only job that can sign attestations or write releases, waits for approval.
- Require the `test` and `security` checks on `main`.

## Changing release inputs

- **Go:** change the `go` directive in `go.mod`, the image tag and digest in
  `release/Dockerfile`, and the Go version in the README and getting-started
  guide together. Dependabot proposes builder image updates; one that changes the
  image's Go version fails `release-check` until `go.mod` matches.
- **musl:** when Alpine ships a new musl release, download the matching
  `musl-X.Y.Z.tar.gz` and `.asc` from musl.libc.org, verify the signature,
  replace `release/notices/musl-*-COPYRIGHT` with the tarball's `COPYRIGHT`,
  and update `toolchain.musl` in `release/licenses.json`.
- **Linked modules:** review the license files, then add, change, or remove the
  module's entry in `release/licenses.json` in the same change.
- **Platforms:** add another platform only with qualification evidence for it;
  the archive name, smoke checks, and support scope assume `linux/amd64`.

### Running locally

`make release-snapshot` builds HEAD into `dist/` and smoke-tests it. It requires
Docker and network access. Behind a proxy, `VMTIER_RELEASE_DOCKER_RUN_ARGS` adds
arguments to `docker run`, such as `--network host -e HTTPS_PROXY`.
`--builder-image` substitutes a prebuilt builder and is recorded as
`builder.image_override` in the build record. Neither setting may change outputs;
check with `scripts/release.py compare`.
