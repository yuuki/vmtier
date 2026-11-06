#!/usr/bin/env bash
# Build unmodified OSS VictoriaMetrics, with vendored dependency versions.
set -euo pipefail
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
revision=9a009c491da4c17531893ab4acb6a5f1b5e0eaf2
archive_sha256=acf22564ccd69b10eda3a94bf0b03cc6ec28ef3a0d4c8fb43579d563907616a6
vm_dir=${VM_SOURCE_DIR:-"$repo_dir/.cache/VictoriaMetrics-$revision"}
go_bin=${GO_BIN:-go}
if [[ ! -f "$vm_dir/go.mod" ]]; then
  mkdir -p "$repo_dir/.cache"
  archive=$(mktemp "$repo_dir/.cache/vm-source.XXXXXX.tar.gz")
  trap 'rm -f "$archive"' EXIT
  curl --fail --show-error --location --retry 3 \
    "https://codeload.github.com/VictoriaMetrics/VictoriaMetrics/tar.gz/$revision" -o "$archive"
  printf '%s  %s\n' "$archive_sha256" "$archive" | sha256sum --check --status
  tar -xzf "$archive" -C "$repo_dir/.cache"
fi
mkdir -p "$repo_dir/.bin"
cd "$vm_dir"
export GOMAXPROCS=${GOMAXPROCS:-3}
for component in vmstorage vminsert vmselect; do
  "$go_bin" build -buildvcs=false -mod=vendor -p "$GOMAXPROCS" \
    -ldflags "-X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=v1.128.0-cluster-vmtier-e2e-$revision" \
    -o "$repo_dir/.bin/$component" "./app/$component"
done
