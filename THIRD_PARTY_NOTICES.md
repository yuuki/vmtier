# Third-party source and license record

The repository's existing Apache-2.0 `LICENSE` is preserved. vmtier is an independent, unofficial project and does not imply VictoriaMetrics endorsement. This record describes the development build; it is not a statement that every file in a broad upstream vendor archive is linked into the binary.

## Direct dependencies

| Module | Pinned version | License / notice source |
| --- | --- | --- |
| `github.com/VictoriaMetrics/VictoriaMetrics` | `v1.155.0-cluster`, commit `cab3d5fbeeedd5d0374c1dabde93939af4c2499c` | [Apache-2.0 LICENSE](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/LICENSE), including Copyright 2019-2026 VictoriaMetrics, Inc. No root NOTICE file was present in the inspected archive. |
| `github.com/aws/aws-sdk-go-v2` | `v1.47.7` | Generated `vendor/github.com/aws/aws-sdk-go-v2/LICENSE.txt` and `NOTICE.txt`, Apache-2.0; `internal/sync/singleflight/LICENSE`, BSD-3-Clause. |
| `github.com/aws/aws-sdk-go-v2/credentials` | `v1.20.15` | AWS SDK Apache-2.0 license and notices retained in generated vendor. |
| `github.com/aws/aws-sdk-go-v2/service/s3` | `v1.114.8` | AWS SDK Apache-2.0 license and notices retained in generated vendor. |
| `github.com/aws/smithy-go` | `v1.28.8` | Generated `vendor/github.com/aws/smithy-go/LICENSE` and `NOTICE`, Apache-2.0; nested Go-derived `LICENSE` files, BSD-3-Clause. |
| `github.com/klauspost/compress` | `v1.20.3` | Generated `vendor/github.com/klauspost/compress/LICENSE`, BSD-style three-clause terms; copyright notices include The Go Authors and Klaus Post. Subdirectories may contain additional licenses/notices, which are retained. |
| `github.com/lib/pq` | `v1.12.3` | Generated `vendor/github.com/lib/pq/LICENSE`, MIT, Copyright (c) 2011-2013, 'pq' Contributors. Portions Copyright (c) 2011 Blake Mizerany. |
| `github.com/mattn/go-sqlite3` | `v1.14.53` | Generated `vendor/github.com/mattn/go-sqlite3/LICENSE`, MIT, Copyright (c) 2014 Yasuhiro Matsumoto. The bundled SQLite 3.54.1 amalgamation retains its upstream copyright-disclaimer/blessing notices in `sqlite3-binding.c` and related headers; see [SQLite's copyright statement](https://www.sqlite.org/copyright.html). |
| `golang.org/x/sys` | `v0.49.0` | Generated `vendor/golang.org/x/sys/LICENSE`, BSD-style three-clause terms, Copyright 2009 The Go Authors. |
| Go runtime / standard library | See build toolchain version | Go distribution `LICENSE`, BSD-style three-clause terms, Copyright 2009 The Go Authors, and `PATENTS`; `runtime/memmove_amd64.s` also carries an MIT notice (see below). |

## Reproducible source acquisition

`scripts/vm-source.sh` supplies the shared pin used by bootstrap and the stock VM build. The source archive is:

```text
https://codeload.github.com/VictoriaMetrics/VictoriaMetrics/tar.gz/cab3d5fbeeedd5d0374c1dabde93939af4c2499c
SHA-256 536d1c62eb98e5a67776a501fc4d0ec8cf6a41b8dc57a7bf4ef69f29456a6e9d
```

TLS and the archive checksum are verified. The stock VM source is extracted into a revision-specific directory under `.deps/`, retaining its original vendor tree and license material for the independent E2E binaries. No upstream implementation files are patched.

vmtier's separate `vendor/` is regenerated with `go mod download`, `go mod verify`, and `go mod vendor` from the committed `go.mod` and `go.sum`. This includes the VM library, SQLite and PostgreSQL drivers, and updated direct and transitive modules used by vmtier. Go verifies downloaded module checksums; `go mod verify` checks cached module contents. Bootstrap does not select newer module versions. `.deps/` and `vendor/` are generated build inputs, not a separately authored implementation.

The SQLite driver pin is recorded in `go.sum`:

```text
github.com/mattn/go-sqlite3 v1.14.53 h1:mnTM0owzUMrKuLo+0w51NUiB0ir2Fo7kFXlPt/l3BGs=
github.com/mattn/go-sqlite3 v1.14.53/go.mod h1:6JTjA44L93a0QCyJef5YvlPoKXntQPjzWv5gtm9sB6w=
```

The generated vendor retains the SQLite driver source used by the build, its MIT
license, and SQLite notices. The catalog builds the bundled
SQLite amalgamation through CGO and requires a C compiler; it does not depend on a
separately installed SQLite service or shared library in the default build.

The PostgreSQL driver pin is also recorded in `go.sum`:

```text
github.com/lib/pq v1.12.3 h1:tTWxr2YLKwIvK90ZXEw8GP7UFHtcbTtty8zsI+YjrfQ=
github.com/lib/pq v1.12.3/go.mod h1:/p+8NSbOcwzAEI7wiMXFlgydTwcgTr3OSKMsD2BitpA=
```

Go module verification and vendoring retain the PostgreSQL driver source used by
the build and its MIT license. The driver is pure Go and requires no installed
`libpq` client library. PostgreSQL server software is an external deployment/test
prerequisite and is not bundled with vmtier.

The gateway uses the public `vmselectapi.API`, `NewServer`, storage and encoding APIs. No copied/modified private vmselect server implementation is maintained. The bounded portable-format parser is vmtier implementation based on the documented pinned source layout, with upstream public codec calls. Compatibility tests are required when updating that layout.

## Transitive dependencies and redistribution

Go vendoring retains license and notice files for the included dependencies. The exact vmtier module/version inventory is generated `vendor/modules.txt`. The stock VM binaries have a separate inventory in `.deps/VictoriaMetrics-cab3d5fbeeedd5d0374c1dabde93939af4c2499c/vendor/modules.txt`; their upstream dependency versions can differ from vmtier's. The downloaded upstream source and its license/NOTICE files remain intact. Do not delete a retained license on the assumption that one selected binary does not link that package.

For a binary distribution, record its actual linked module set with `go version -m <binary>` and package the corresponding full license texts and required notices, including the Go runtime license. For distribution of the entire generated source tree or upstream test binaries, preserve all included licenses and notices. The release tooling below does this for the published binary archive; it does not replace legal review of a new dependency.

`NOTICE` reproduces the direct VM/AWS/Smithy attribution notices and the SQLite driver's copyright notice. Dependency-specific license terms continue to apply in addition to vmtier's project license. The prior-art comparison did not copy vmcoldstorage code (permission unconfirmed), Mimir code (AGPL), or code from other compared projects; see [docs/prior-art.md](docs/prior-art.md).

## Release binaries

[Release archives](docs/releasing.md) contain one static `linux/amd64` binary. `scripts/release.py` reads the linked module set from the binary with `go version -m` and copies every license, notice, and patent file of those modules, nested ones included, into `licenses/<module path>/`. For example, `gozstd` links prebuilt zstd code; the zstd distribution is dual-licensed, and its BSD `LICENSE` and GPL-2.0 `COPYING` are both retained. The archive also contains this file, `LICENSE`, `NOTICE`, and the toolchain notices below. Links in this file point into the source repository.

[`release/licenses.json`](release/licenses.json) records the reviewed SPDX expression and a digest of the license files of every linked module. The build fails when a module is linked or dropped, or its license files change, until the record is updated after review. The same expressions appear in the build record and the CycloneDX SBOM. The version table above is maintained by hand. Each release's build record and SBOM are the authoritative inventory.

| Code compiled into the binary | License and notice |
| --- | --- |
| Go runtime and standard library | BSD-3-Clause, plus MIT for `runtime/memmove_amd64.s`, which derives from Inferno (Lucent Technologies, Vita Nuova). The builder copies `LICENSE`, `PATENTS`, and that file's header notice from its `GOROOT` into `licenses/go/` |
| zstd 1.5.7, prebuilt in `gozstd` | BSD-3-Clause, selected from BSD-3-Clause OR GPL-2.0-only. The linked `divsufsort.c` (MIT, Yuta Mori) and `threading.c` (Tino Reichardt) carry their own headers. [`release/notices/gozstd-v1.27.0-zstd-NOTICE`](release/notices/gozstd-v1.27.0-zstd-NOTICE) reproduces them verbatim, and the build fails if a different `gozstd` version is linked |
| SQLite 3.54.1, the amalgamation in `go-sqlite3` | Public-domain dedication (SPDX `blessing`), recorded with the driver's MIT license |
| musl 1.2.6 (Alpine package) | MIT; [`release/notices/musl-1.2.6-COPYRIGHT`](release/notices/musl-1.2.6-COPYRIGHT) is the `COPYRIGHT` file of `musl-1.2.6.tar.gz` (SHA-256 `d585fd3b613c66151fc3249e8ed44f77020cb5e6c1e635a616d3f9f82460512a`), whose signature verifies with the musl release key `8364 8929 0BB6 B70F 99FF DA05 56BC DB59 3020 450F`. musl also supplies the `crt1.o`, `crti.o`, and `crtn.o` start files |
| GCC support code (`libgcc`, `crtbegin.o`, `crtend.o`) | GPL-3.0-or-later WITH GCC-exception-3.1. The exception permits distributing the combined binary under other terms, because it was produced by an eligible compilation process; it requires no notice |

The builder fails if its musl version differs from the version of the committed notice. glibc is not linked.
