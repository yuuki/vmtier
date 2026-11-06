# Third-party source and license record

The repository's existing Apache-2.0 `LICENSE` is preserved. vmtier is an independent, unofficial project and does not imply VictoriaMetrics endorsement. This record describes the development build; it is not a statement that every file in a broad upstream vendor archive is linked into the binary.

## Direct dependencies

| Module | Pinned version | License / notice source |
| --- | --- | --- |
| `github.com/VictoriaMetrics/VictoriaMetrics` | `v1.128.0-cluster`, commit `9a009c491da4c17531893ab4acb6a5f1b5e0eaf2` | [Apache-2.0 LICENSE](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2/LICENSE), including Copyright 2019-2025 VictoriaMetrics, Inc. No root NOTICE file was present in the inspected archive. |
| `github.com/aws/aws-sdk-go-v2` | `v1.39.2` | Generated `vendor/github.com/aws/aws-sdk-go-v2/LICENSE.txt` and `NOTICE.txt`, Apache-2.0. |
| `github.com/aws/aws-sdk-go-v2/credentials` | `v1.18.16` | AWS SDK Apache-2.0 license and notices retained in generated vendor. |
| `github.com/aws/aws-sdk-go-v2/service/s3` | `v1.88.4` | AWS SDK Apache-2.0 license and notices retained in generated vendor. |
| `github.com/aws/smithy-go` | `v1.23.0` | Generated `vendor/github.com/aws/smithy-go/LICENSE` and `NOTICE`, Apache-2.0. |
| `github.com/klauspost/compress` | `v1.18.0` | Generated `vendor/github.com/klauspost/compress/LICENSE`, BSD-style three-clause terms; copyright notices include The Go Authors and Klaus Post. Subdirectories may contain additional licenses/notices, which are retained. |
| `github.com/mattn/go-sqlite3` | `v1.14.32`, commit `8bf7a8a844faf952aa0245b4c0ad0a47e84f4efd` | Generated `vendor/github.com/mattn/go-sqlite3/LICENSE`, MIT, Copyright (c) 2014 Yasuhiro Matsumoto. The bundled SQLite 3.50.4 amalgamation retains its upstream copyright-disclaimer/blessing notices in `sqlite3-binding.c` and related headers; see [SQLite's copyright statement](https://www.sqlite.org/copyright.html). |
| Go runtime / standard library | See build toolchain version | Go distribution `LICENSE`, BSD-style three-clause terms, Copyright 2009 The Go Authors. |

## Reproducible source acquisition

`scripts/bootstrap.sh` retrieves the VM commit archive from:

```text
https://codeload.github.com/VictoriaMetrics/VictoriaMetrics/tar.gz/9a009c491da4c17531893ab4acb6a5f1b5e0eaf2
SHA-256 acf22564ccd69b10eda3a94bf0b03cc6ec28ef3a0d4c8fb43579d563907616a6
```

TLS and the archive checksum are verified. The upstream archive includes its own pinned vendor tree. `scripts/vendor.py` generates a local `vendor/` by retaining that tree and copying the upstream `lib/` plus license files. It adjusts generated `vendor/modules.txt` metadata for vmtier's direct module set; it does not patch upstream implementation files. `.deps/` and `vendor/` are generated build inputs, not a separately authored implementation.

The SQLite driver is an additional direct dependency, outside the VM vendor tree:

```text
https://proxy.golang.org/github.com/mattn/go-sqlite3/@v/v1.14.32.zip
SHA-256 c70101000256656bc0b7286807cf177852c753a001a3aa3c55d5373575fb81aa
Module sum h1:JD12Ag3oLy1zQA+BNn74xRgaBbdhbNIDYvQUEuuErjs=
go.mod sum h1:Uh1q+B4BYcTPb+yiD3kU8Ct7aC0hY9fxUwlHK0RXw+Y=
```

The module sums were verified through the Go checksum database when selecting this
pin and are recorded in `go.sum`. Bootstrap verifies the ZIP's SHA-256 and retains
the complete module source, MIT license, and SQLite notices in generated vendor.
It does not resolve or update unrelated modules. The catalog builds the bundled
SQLite amalgamation through CGO and requires a C compiler; it does not depend on a
separately installed SQLite service or shared library in the default build.

The gateway uses the public `vmselectapi.API`, `NewServer`, storage and encoding APIs. No copied/modified private vmselect server implementation is maintained. The bounded portable-format parser is vmtier implementation based on the documented pinned source layout, with upstream public codec calls. Compatibility tests are required when updating that layout.

## Transitive dependencies and redistribution

All transitive license/NOTICE files in the upstream vendor tree are retained by bootstrap. The exact module/version inventory is generated `vendor/modules.txt`. It contains more packages than vmtier links, because the same pinned source also builds the stock VM cluster binaries for E2E. Do not delete a retained license on the assumption that one selected binary does not link that package.

For a binary distribution, record its actual linked module set with `go version -m <binary>` and package the corresponding full license texts and required notices, including the Go runtime license. For distribution of the entire generated source tree or upstream test binaries, preserve all included licenses and notices. This MVP work does not publish a release or represent a completed release-wide legal audit; the local generated source retains attribution material.

`NOTICE` reproduces the direct VM/AWS/Smithy attribution notices and the SQLite driver's copyright notice. Dependency-specific license terms continue to apply in addition to vmtier's project license. The prior-art comparison did not copy vmcoldstorage code (permission unconfirmed), Mimir code (AGPL), or code from other compared projects; see [docs/prior-art.md](docs/prior-art.md).
