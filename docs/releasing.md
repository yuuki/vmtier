# Releases

A vmtier release is a static `linux/amd64` binary archive built from one tagged
commit in a digest-pinned container. Two independent builds have to match
byte for byte. The packaged binary then has to pass the stock-VM differential
suites, and the workflow publishes signed provenance and SBOM attestations. The
release workflow saves a **draft**; publishing it is a separate maintainer action.
[ADR 0005](adr/0005-release-artifacts.md) records why the pipeline is shaped
this way.

No release has been published. The CI `release-check` job has reproduced the
archive on GitHub-hosted runners, a rebuild outside GitHub matched it byte for
byte, and a tagless run of the release workflow passed every gate except
`publish`; [release validation](release-validation.md) records the runs and
checksums. Each
release's notes must state the support scope below.

## Support scope

| Area | Released artifact |
| --- | --- |
| Platform | Linux on x86-64 (`GOAMD64=v1`), statically linked against musl; no glibc version requirement |
| VictoriaMetrics | OSS `v1.155.0-cluster`, commit `cab3d5fbeeedd5d0374c1dabde93939af4c2499c` only ([compatibility](compatibility.md)) |
| Runtime | `vmtier serve` with one SQLite owner, or with a PostgreSQL catalog, multiple Cold read gateways, and one leased archiver ([limits](limits.md), [HA deployment](ha-deployment.md)) |
| Evidence | Local differential, recovery, workload, catalog, and PostgreSQL HA reports in the [evidence index](evidence.md), hosted CI and reproducibility runs in [release validation](release-validation.md), plus the release run's `release-e2e-evidence` artifact |

The following remain unqualified: real Sakura Object Storage (#15), natural Hot
retention expiry (#16), and enforced ingress lateness (#17). HA, GC, Cold TTL,
backfill, other VM versions, and other platforms are outside the first-release
scope. PostgreSQL database HA and old-primary fencing remain operator
responsibilities. Until those issues close with recorded evidence, tag releases as
pre-releases (`v0.1.0-rc.1`) and do not describe them as production-supported.

## Release contents

| Asset | Contents |
| --- | --- |
| `vmtier-VERSION-linux-amd64.tar.gz` | `vmtier`, `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md`, `README.md`, `examples/serve-sakura.json`, `examples/serve-postgres.json`, `examples/systemd/vmtier.service` and `examples/systemd/vmtier.env`, `BUILD.json`, `sbom.cdx.json`, `go-version-m.txt`, and `licenses/` |
| `vmtier-VERSION-linux-amd64.build.json` | Build record: commit, `SOURCE_DATE_EPOCH`, builder digest, Alpine package versions, Go environment, exact build command, binary SHA-256, every linked module with its `go.sum` hash, SPDX license expression, notice files, and requirements, and the C libraries compiled into modules |
| `vmtier-VERSION-linux-amd64.cdx.json` | CycloneDX 1.6 SBOM of the binary: linked Go modules with their requirement graph, SQLite and zstd (with CPEs), the Go standard library, musl, and libgcc |
| `SHA256SUMS` | SHA-256 of the three files above |
| Attestations | SLSA build provenance for every file in `SHA256SUMS` and an SBOM attestation for the archive, signed through Sigstore and stored by GitHub |

`licenses/` contains each linked module's license, notice, and patent files,
nested ones included (such as the BSD license of the zstd library in `gozstd`).
Notices that exist only in C source headers are reviewed into
`release/notices/` and shipped next to the module's files; for example, the
divsufsort and threading notices of the zstd code in `gozstd`. The archive also
has the Go distribution `LICENSE` and `PATENTS`, the Inferno notice of
`runtime/memmove_amd64.s`, and the musl `COPYRIGHT`.

[`release/licenses.json`](../release/licenses.json) holds the reviewed SPDX
expression and a digest of the license files of each linked module. The build
fails when a module is linked or dropped, or when its license files change, for
example on a version bump. It also fails when a C library or toolchain notice no
longer matches the reviewed version. The record covers what the build can
detect; header notices in a new C dependency still need manual review.

## Build inputs

| Input | Pin |
| --- | --- |
| Source | `git archive` of the tagged commit; every exported file must match its committed blob, so local git attributes or filters cannot alter it, and the build refuses uncommitted tracked changes |
| Builder | [`release/Dockerfile`](../release/Dockerfile): `golang:1.27.4-alpine3.24` pinned by digest, with `gcc` and `musl-dev` from Alpine |
| Go | 1.27.4, `GOTOOLCHAIN=local`; [`release/build.sh`](../release/build.sh) fails unless it equals the `go` directive in `go.mod` |
| Modules | `go.mod`/`go.sum`, fetched from the module proxy, rejected unless they match the committed `go.sum` hashes, re-hashed by `go mod verify`, then vendored |
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
gh attestation verify vmtier-VERSION-linux-amd64.tar.gz --repo yuuki/vmtier \
  --signer-workflow yuuki/vmtier/.github/workflows/release.yml \
  --source-ref refs/tags/VERSION --deny-self-hosted-runners
gh attestation verify vmtier-VERSION-linux-amd64.tar.gz --repo yuuki/vmtier \
  --signer-workflow yuuki/vmtier/.github/workflows/release.yml \
  --source-ref refs/tags/VERSION --deny-self-hosted-runners \
  --predicate-type https://cyclonedx.org/bom
tar -xzf vmtier-VERSION-linux-amd64.tar.gz
vmtier-VERSION-linux-amd64/vmtier version
```

To run the verified binary as a service, follow
[install a release archive](getting-started.md#install-a-release-archive).

Pin the signer workflow and tag: `--repo` alone accepts an attestation made by
any workflow in the repository on any ref. A passing check shows that
`release.yml` at that tag signed these bytes. It does not show that the tag was
created under the repository's tag rules; that depends on the settings below.
`python3 scripts/release.py verify DIR` checks the directory by hashes and
requires exactly the four release files, without running anything from it. To rebuild independently, check out the tag on Linux with Docker,
Python 3, and git, then compare against the downloaded directory:

```sh
git checkout VERSION
python3 scripts/release.py build --version VERSION --output /tmp/vmtier-rebuild
python3 scripts/release.py compare /path/to/downloaded /tmp/vmtier-rebuild
```

### Reproducibility limits

- The Dockerfile pins the base image by digest, but Alpine mirrors serve only the
  current package release in a branch. A later rebuild can resolve newer GCC,
  binutils, or musl packages. Compare `builder.packages` in both build records
  before you treat a mismatch as a source difference.
- Archive bytes also depend on the zlib that the packaging Python uses. If only
  the `.tar.gz` checksum differs, compare `binary.sha256` in the build records.
- Builds need network access to the Go module proxy and Alpine mirrors. Modules
  are not mirrored by this project. Behind a TLS-intercepting proxy, the builder
  needs the proxy's CA; [release validation](release-validation.md#independent-rebuild-in-another-environment)
  shows an equivalent builder image that still reproduced the hosted bytes.

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
   | `build (first)`, `build (second)` | Check that the tagged commit is on `main`, then build the archive in the pinned builder on separate runners; the first also runs `go test` with release tags and static musl linking; both smoke-test the archive |
   | `reproducibility` | Hashes both builds, requires identical `SHA256SUMS`, and outputs the SHA-256 of that list before any later job runs test dependencies or the binary |
   | `qualify` | Installs the packaged binary as `.bin/vmtier`, scans it with `govulncheck -mode=binary`, and runs the workload smoke, JSON v2, `serve`, recovery, PostgreSQL HA, and SQLite-to-PostgreSQL migration suites against stock VM |
   | `publish` | Runs in the `release` environment: accepts only files whose `SHA256SUMS` has the recorded SHA-256, without executing them, then attests provenance and SBOM and creates the draft release with generated notes |

6. Review the draft: notes and support scope, the four assets, the attestation
   links in the run summary, and the `release-e2e-evidence` artifact. Then
   publish the draft. If a gate fails before publication, delete the draft and
   the tag, fix the cause on `main`, and tag a new version.

`workflow_dispatch` runs the same build, reproducibility, and qualification gates
on any ref without attesting or creating a release.

### First release

`v0.1.0-rc.1` is the first tag. Until #15, #16, and #17 close with recorded
evidence, use only tags with a hyphen: the workflow then creates a GitHub
pre-release, and its notes say that it is not production-supported. The
remaining steps:

1. Apply the [repository settings](#repository-settings) below; the tag push
   is the first run that can sign or create a release.
2. Merge the release preparation to `main` and wait for its `test`, `security`,
   and `PostgreSQL fault qualification` runs to pass. A `workflow_dispatch`
   run of `release.yml` on that commit rehearses every gate except `publish`.
3. Tag that commit and push the tag as in step 4 above.
4. Approve the `publish` job, review the draft as in step 6, and publish it.
5. Add the tag's run, `SHA256SUMS`, and attestation links to
   [release validation](release-validation.md), and verify the published assets
   from a clean directory with the commands above.

### Repository settings

These GitHub settings protect the procedure; the workflows do not configure them.
A tag push runs `release.yml` from the tagged commit. So a person who can push a
`v*` tag controls which workflow signs. The `main` ancestry check catches
mistakes, not a hostile tag. The tag ruleset is the actual control:

- Add a tag ruleset that limits creating, updating, and deleting `v*` tags to
  maintainers, and require signed tags if maintainers sign.
- Add required reviewers to the `release` environment, so the `publish` job,
  the only job in `release.yml` that requests signing or release permissions,
  waits for approval.
- Enable **immutable releases**, so published assets and their tags cannot change.
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
- **Linked modules:** when the build reports a new, dropped, or changed module,
  read its license files, then update its expression and `notices_sha256` in
  `release/licenses.json`; the error message prints the new digest. When a
  module with an embedded-notice file changes version, such as `gozstd`,
  re-read the C headers that its prebuilt objects come from and refresh
  `release/notices/`.
- **Platforms:** add another platform only with qualification evidence for it;
  the archive name, smoke checks, and support scope assume `linux/amd64`.

### Running locally

`make release-snapshot` builds HEAD into `dist/` and smoke-tests it. It requires
Docker and network access. Behind a proxy, `VMTIER_RELEASE_DOCKER_RUN_ARGS` adds
arguments to `docker run`, such as `--network host -e HTTPS_PROXY`.
`--builder-image` substitutes a prebuilt builder; it prints a warning and is not
recorded, because an equivalent image produces identical outputs and any package
difference appears in `builder.packages`. Check either setting with
`scripts/release.py compare` against a build without it.
