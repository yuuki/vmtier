# VM compatibility evidence (2026-11-10 UTC)

See [the investigation report](../../vm-version-investigation.md) for conclusions
and limitations. JSON files are retained results, including actual commands,
candidate binary identities and hashes. `downloads.json` contains official
release URLs, archive SHA-256 values and the downloaded upstream checksum lists;
the archive and extracted component checksums were verified before execution.
The v1.155.0 controls were built from checksum-pinned stock sources by `make stock`.

`run-*-*.json` records the existing `scripts/e2e.py` harness; `sqlite-134.json`
records `scripts/e2e_serve.py`. `replica-*-complete.json` records the directed
two-storage-node stale collision, with source and Cold responses, publication
state and raw native captures (`.native`). `frontend-probe.json` isolates RPC
compatibility; `frontend-gateway.log` preserves the `search_v6` rejection.
`source-scan.json` records selected upstream source URLs and hashes.
`release-dates.json` preserves the official release notes' date text and the
separate GitHub release-publication timestamps for every version in the matrix
and the two discussed Enterprise backports.

The drivers are experimental reproduction tools, not production configuration.
They create disposable directories, invoke the unchanged repository harnesses,
and stop their child processes. `run_version.py` substitutes candidate binaries
through an isolated directory of symlinks; it replaces the original harness's
hardcoded VM revision field with actual candidate binary identities. Driver
options evolved during the investigation, so historical per-run driver hashes
need not equal the final reproducer's hash. The repository harness hashes and
binary hashes remain available in the original reports.

From the repository root, with the pinned Go toolchain available:

```sh
make bootstrap build stock

# Extract an official cluster archive into a separate directory, verify its
# upstream checksums, and provide vmstorage/vminsert/vmselect symlinks to the
# corresponding *-prod executables. downloads.json lists the exact assets used.
repo=$(pwd)
driver="$repo/docs/results/vm-compatibility-2026-11-10"
vm_bin=/absolute/path/to/v1.134.0
out=/absolute/path/to/new-results

GO_BIN=$(command -v go) python3 "$driver/run_version.py" \
  --repo "$repo" --vm-bin "$vm_bin" --workdir "$out/json"
GO_BIN=$(command -v go) python3 "$driver/run_version.py" \
  --repo "$repo" --vm-bin "$vm_bin" --workdir "$out/sqlite" \
  --suite sqlite --collision
python3 "$driver/probe_collision.py" \
  --repo "$repo" --vm-bin "$vm_bin" --workdir "$out/replica-collision"
python3 "$driver/probe_frontend.py" \
  --repo "$repo" --archive-run "$out/json" --workdir "$out/frontends" \
  /absolute/path/to/v1.78.1 /absolute/path/to/v1.79.0
```

Run the frontend probe shortly after the archive run so its time-dependent
coverage contract remains current. Reuse the same candidate binaries across
source, oracle and frontend in a full-path run. For historical native probes
v1.78.1/v1.79.0/v1.81.2/v1.82.0, pass `--legacy-flags`; the four omitted flags
are explicitly recorded. `--collision` changes the existing `duplicates` series
to include a number/stale/number sequence at every timestamp. The separate
`probe_collision.py` uses different storage nodes and is necessary to expose the
v1.133.0 incompatibility even when a single-node fixture passes.

Nonzero archive exit status in the old-version directed probes is an expected
observation, recorded in `archive_exit_code`; the probe driver itself can exit
successfully after recording that refusal. Check the report fields, not only the
driver exit code. The complete full-path harness exits nonzero on failure.

The executor used Go 1.27.3, Python 3.12.14, Linux amd64 and GOMAXPROCS=2 for VM
workload children. It supplied `GO_BIN=/workspace/tools/go/bin/go` because the
system `/usr/bin/go` is not the Go language toolchain. Raw working directories
and installed historical VM binaries were left under
`/workspace/scratch/vm-compat`; generated current binaries are in `.bin/`.
