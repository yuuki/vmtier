# SQLite catalog stage 1 validation

Validated on 2026-11-06 in the same Linux amd64 development environment as the
[MVP evidence](validation.md), using Go 1.27.3 and the pinned stock VM binaries.
This report covers the new offline SQLite foundation and regression of the
existing JSON v2 runtime. **It does not claim SQLite-backed gateway/archive E2E.**

## Build and focused behavior

`make GO=/workspace/tools/go/bin/go build test race vet` completed successfully
after the final implementation edits. All packages passed, including 16 new
top-level SQLite catalog/maintenance/CLI tests and their subtests.

The new tests cover:

- Explicit initialization, exclusive ownership, missing/wrong-ID/corrupt/unsupported
  schema rejection, and pinned blob locator normalization.
- Multi-pack and empty-window publication, independent tenant/source progress,
  idempotent replay after later commits, competing generations, and transaction
  rollback when SQL insertion fails.
- Bounded time selection, plan limits, and a historical fixture with 10,000 rows
  followed by publication and selection of the 10,001st window through the API.
  This is a catalog fixture, not a sustained production workload benchmark.
- V2 index/reference/coverage import parity, empty legacy packs, gaps and bad
  indexes, and absence of a published destination after failed import.
- Consistent backup snapshots, restore under a new ID, further publication after
  restoration, restart, metadata/reference/checksum rejection, and no overwrite.
- CLI initialization, inspection, backup, and restore without S3 credentials;
  import policy matching and bounded selection through the actual command parser.

The SQLite dependency was verified through Go's checksum database and pinned by
module sums and archive SHA-256. An isolated fresh bootstrap generated matching
vendor metadata, was repeatable, and rejected corrupted source archives. Additional
bootstrap checks exercised interrupted downloads and cache reuse. These checks do
not involve a provider account or a deployment.

## Stock-VM regression

After the SQLite changes, the existing E2E suites passed again:

| Suite | Exact query comparisons | Scenario checks | Result |
| --- | ---: | ---: | --- |
| Local blobs / one Hot replica | 360 | 19 | Passed |
| Moto + restrictive S3 proxy / two Hot replicas | 360 | 20 | Passed |

Raw, unchanged reports are [local](results/sqlite-stage1/local.json) and
[S3 replicated](results/sqlite-stage1/s3-replicated.json). Their source, binary, and
harness fingerprints were checked against the final working tree. Earlier MVP
JSON results remain unchanged in their original locations.

The S3 run recorded zero conditional requests, 17 PUTs with valid Content-MD5,
2,089 stripped ETag headers, and 1,641 Range requests. As before, the harness rebuilt
Hot with recent samples and verified the old range was empty. This was not natural
retention expiry. The suites still query the JSON v2 gateway; SQLite is not on that
query path yet. Their performance values are regression-run observations, not
SQLite or real-Sakura benchmarks.

Reproduce with fresh work directories after building the stock binaries and
installing the test requirements:

```sh
python3 scripts/e2e.py --backend local --workdir .cache/sqlite-regression-local
python3 scripts/e2e.py --backend s3 --replicas 2 --workdir .cache/sqlite-regression-s3
```

## CLI smoke test using real MVP archive objects

The freshly generated local E2E archive was imported using the built `vmtier`
binary, inspected for both tenants, backed up, and restored under a new ID.
All six stream windows preserved their references and coverage. SHA-256 and size
were compared with the six actual packs and six indexes. After restore, status and
selection plans matched except for the new catalog ID.

The source v2 generation was 3; SQLite generation was 6 because it commits each
tenant/source/window separately. The [smoke report](results/sqlite-stage1/cli-import-restore.json)
records the executed commands and binary/source-manifest hashes.

The E2E's final failure test moves local blobs to `objects-offline`. The first smoke
attempt correctly failed on the now-empty original path. A copied smoke configuration
was pointed at the preserved directory; the original E2E config, data, and results
were not changed. The corrected smoke test passed. This is an offline migration
check, not proof of runtime query cutover or recovery from VM retention expiry.

## Remaining verification

The next step must connect the repository to the single-owner service and test
SQLite-backed archive/query behavior through stock vmselect. Shared S3 throttling,
streaming multi-pack archive production, memory/load qualification, and real-provider
tests remain unimplemented or unexecuted. No real Sakura/AWS/MinIO account, natural
Hot retention transition, ingress lateness enforcement, power-loss durability test,
hosted CI run, publication, or deployment was performed in this step.
