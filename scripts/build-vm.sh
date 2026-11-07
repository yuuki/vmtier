#!/usr/bin/env bash
# Build unmodified OSS VictoriaMetrics, with vendored dependency versions.
set -euo pipefail
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
go_bin=${GO_BIN:-go}
source "$repo_dir/scripts/vm-source.sh"
prepare_vm_source
mkdir -p "$repo_dir/.bin"
cd "$vm_source_dir"
export GOMAXPROCS=${GOMAXPROCS:-3}
for component in vmstorage vminsert vmselect; do
  "$go_bin" build -buildvcs=false -mod=vendor -p "$GOMAXPROCS" \
    -ldflags "-X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=$vm_version-vmtier-e2e-$vm_revision" \
    -o "$repo_dir/.bin/$component" "./app/$component"
done
