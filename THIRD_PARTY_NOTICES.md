# Third-party source and license record

The repository's existing Apache-2.0 `LICENSE` is preserved. vmtier is an independent, unofficial project and does not imply VictoriaMetrics endorsement. This record describes the development build; it is not a statement that every file in a broad upstream vendor archive is linked into the binary.

## Direct dependencies

| Module | Pinned version | License / notice source |
| --- | --- | --- |
| `github.com/VictoriaMetrics/VictoriaMetrics` | `v1.155.0-cluster`, commit `cab3d5fbeeedd5d0374c1dabde93939af4c2499c` | [Apache-2.0 LICENSE](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/cab3d5fbeeedd5d0374c1dabde93939af4c2499c/LICENSE), including Copyright 2019-2026 VictoriaMetrics, Inc. No root NOTICE file was present in the inspected archive. |
| `github.com/aws/aws-sdk-go-v2` | `v1.47.7` | Generated `vendor/github.com/aws/aws-sdk-go-v2/LICENSE.txt` and `NOTICE.txt`, Apache-2.0. |
| `github.com/aws/aws-sdk-go-v2/credentials` | `v1.20.15` | AWS SDK Apache-2.0 license and notices retained in generated vendor. |
| `github.com/aws/aws-sdk-go-v2/service/s3` | `v1.114.8` | AWS SDK Apache-2.0 license and notices retained in generated vendor. |
| `github.com/aws/smithy-go` | `v1.28.8` | Generated `vendor/github.com/aws/smithy-go/LICENSE` and `NOTICE`, Apache-2.0. |
| `github.com/klauspost/compress` | `v1.20.3` | Generated `vendor/github.com/klauspost/compress/LICENSE`, BSD-style three-clause terms; copyright notices include The Go Authors and Klaus Post. Subdirectories may contain additional licenses/notices, which are retained. |
| `github.com/lib/pq` | `v1.10.9` | Generated `vendor/github.com/lib/pq/LICENSE.md`, MIT, Copyright (c) 2011-2013, 'pq' Contributors; Portions Copyright (C) 2011 Blake Mizerany. |
| `github.com/mattn/go-sqlite3` | `v1.14.53` | Generated `vendor/github.com/mattn/go-sqlite3/LICENSE`, MIT, Copyright (c) 2014 Yasuhiro Matsumoto. The bundled SQLite 3.54.1 amalgamation retains its upstream copyright-disclaimer/blessing notices in `sqlite3-binding.c` and related headers; see [SQLite's copyright statement](https://www.sqlite.org/copyright.html). |
| `golang.org/x/sys` | `v0.49.0` | Generated `vendor/golang.org/x/sys/LICENSE`, BSD-style three-clause terms, Copyright 2009 The Go Authors. |
| Go runtime / standard library | See build toolchain version | Go distribution `LICENSE`, BSD-style three-clause terms, Copyright 2009 The Go Authors. |

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
github.com/lib/pq v1.10.9 h1:YXG7RB+JIjhP29X+OtkiDnYaXQwpS4JEWq7dtCCRUEw=
github.com/lib/pq v1.10.9/go.mod h1:AlVN5x4E4T544tWzH6hKfbfQvm3HdbOxrmggDNAPY9o=
```

Go module verification and vendoring retain the PostgreSQL driver source used by
the build and its MIT license. The driver is pure Go and requires no installed
`libpq` client library. PostgreSQL server software is an external deployment/test
prerequisite and is not bundled with vmtier.

The gateway uses the public `vmselectapi.API`, `NewServer`, storage and encoding APIs. No copied/modified private vmselect server implementation is maintained. The bounded portable-format parser is vmtier implementation based on the documented pinned source layout, with upstream public codec calls. Compatibility tests are required when updating that layout.

## Transitive dependencies and redistribution

Go vendoring retains license and notice files for the included dependencies. The exact vmtier module/version inventory is generated `vendor/modules.txt`. The stock VM binaries have a separate inventory in `.deps/VictoriaMetrics-cab3d5fbeeedd5d0374c1dabde93939af4c2499c/vendor/modules.txt`; their upstream dependency versions can differ from vmtier's. The downloaded upstream source and its license/NOTICE files remain intact. Do not delete a retained license on the assumption that one selected binary does not link that package.

For a binary distribution, record its actual linked module set with `go version -m <binary>` and package the corresponding full license texts and required notices, including the Go runtime license. For distribution of the entire generated source tree or upstream test binaries, preserve all included licenses and notices. This MVP work does not publish a release or represent a completed release-wide legal audit; the local generated source retains attribution material.

`NOTICE` reproduces the direct VM/AWS/Smithy attribution notices and the SQLite driver's copyright notice. Dependency-specific license terms continue to apply in addition to vmtier's project license. The prior-art comparison did not copy vmcoldstorage code (permission unconfirmed), Mimir code (AGPL), or code from other compared projects; see [docs/prior-art.md](docs/prior-art.md).
