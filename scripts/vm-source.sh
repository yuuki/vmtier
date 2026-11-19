#!/usr/bin/env bash
# Shared pin for the unmodified stock cluster used by the differential tests.
vm_version=v1.156.0-cluster
vm_revision=02bcbd7e56a1d341b31dfb74d96d631e8df323f7
vm_archive_sha256=da08df2db5dd360cf0c3fbc90dc00ffc9f074cb6afbc995be5601cb4ce06db48
vm_source_dir=${VM_SOURCE_DIR:-"$repo_dir/.deps/VictoriaMetrics-$vm_revision"}

prepare_vm_source() (
  set -euo pipefail
  mkdir -p "$repo_dir/.deps"
  archive="$repo_dir/.deps/vm-$vm_revision.tar.gz"
  staging=$(mktemp -d "$repo_dir/.deps/vm-source.XXXXXX")
  trap 'rm -rf -- "$staging"' EXIT
  if [[ ! -f "$archive" ]]; then
    curl --fail --show-error --location --retry 3 \
      "https://codeload.github.com/VictoriaMetrics/VictoriaMetrics/tar.gz/$vm_revision" \
      -o "$staging/source.tar.gz"
    printf '%s  %s\n' "$vm_archive_sha256" "$staging/source.tar.gz" | sha256sum --check --status
    mv -- "$staging/source.tar.gz" "$archive"
  fi
  printf '%s  %s\n' "$vm_archive_sha256" "$archive" | sha256sum --check --status
  marker="$vm_source_dir/.vmtier-source.sha256"
  if [[ -f "$vm_source_dir/go.mod" && -f "$marker" ]] &&
     [[ "$(cat "$marker")" == "$vm_archive_sha256" ]]; then
    exit 0
  fi
  # Never silently label an arbitrary override as the pinned stock source.
  if [[ -n "${VM_SOURCE_DIR:-}" && -e "$vm_source_dir" ]]; then
    printf 'VM_SOURCE_DIR is not a verified pinned source; use a new directory: %s\n' "$vm_source_dir" >&2
    exit 1
  fi
  tar -xzf "$archive" -C "$staging"
  printf '%s\n' "$vm_archive_sha256" > "$staging/VictoriaMetrics-$vm_revision/.vmtier-source.sha256"
  mkdir -p "$(dirname "$vm_source_dir")"
  rm -rf -- "$vm_source_dir"
  mv -- "$staging/VictoriaMetrics-$vm_revision" "$vm_source_dir"
)
