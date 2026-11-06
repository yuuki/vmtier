# Hosted CI evidence packaging validation

[Run 42211699148](https://github.com/yuuki/vmtier/actions/runs/42211699148)
completed successfully on 2026-11-06 at 11:45:45 UTC for commit
[`fe929ae6ec4b3f8258e48946cfd4a2da1cf12cf6`](https://github.com/yuuki/vmtier/commit/fe929ae6ec4b3f8258e48946cfd4a2da1cf12cf6).
Python regression tests, `make bootstrap build test race vet stock`, all four
E2E suites, evidence packaging, and `actions/upload-artifact@v4` succeeded.
This closes the upload gap in [issue #2](https://github.com/yuuki/vmtier/issues/2):
the earlier runs passed test steps but failed to upload tenant paths containing
colons. Historical result files remain unchanged.

Artifact `e2e-evidence` (ID `13499216986`) was downloaded and extracted on
2026-11-06. Its ZIP contains only `e2e-evidence.tar.gz`, so the artifact action
does not validate the nested tenant paths as ZIP member names. Extraction rejected
absolute/traversal paths and non-file/non-directory members before using Python's
`tarfile` data filter. Every extracted file was compared byte-for-byte with its
tar member.

The tar contains 1,299 members, including **806 files**, **136 logs**, and
**24 files with colons in their paths**. Uncompressed file contents total
7,540,733 bytes. For example, the original
`ci-local/objects-offline/tenants/1:2/attempts/...` JSON and pack paths survive.
All four reports say `passed`, with 360 exact query comparisons each (1,440 total):

| Report directory | Logs | `results.json` SHA-256 |
| --- | ---: | --- |
| `ci-local` | 35 | `f7b82ab7b4280a406f4ce386a5e4a248be28a4436ffb90273c5b102baff5fa7d` |
| `ci-s3` | 39 | `8da37f7d35efd5ee3f1acf9bf4ca710745206fd6631ecb8e6fce497aad8a6a11` |
| `ci-serve-local` | 29 | `098455c67c018beb7e5dc4c3b7869bcf0441451cc492cd4c416f273c9536f76e` |
| `ci-serve-s3` | 33 | `092eca7f48a935d9936aaf79acbf30e5a9237af6d641cbee530102a9e61f5dec` |

| Download | Bytes | SHA-256 |
| --- | ---: | --- |
| Artifact ZIP | 514,033 | `8914463fafa227b5575c5f84b4848d0b1966467e9f2280f44453dba35b2cd619` |
| `e2e-evidence.tar.gz` | 513,881 | `2b994d70bd526e05a2d36cea93293adbdc7c71b5a5a14c1deba408a08bfdf0e1` |

This run used the **older E2E harness** at the named commit. It proves successful
hosted evidence packaging and round-trip preservation; it does not validate the
later runtime-provenance, hour-boundary, or stronger blob-outage changes from
issues #3–#5. Their newer runs must be assessed separately. Packaging regression
tests cover partial/empty/missing work directories; this successful hosted run
does not itself exercise a failed E2E run's artifact collection.

To inspect a downloaded artifact, unzip its safe outer member, then run
`tar -xzf e2e-evidence.tar.gz` in an empty directory. The four suite directories
contain their original reports, logs, configuration, and fixture files.
