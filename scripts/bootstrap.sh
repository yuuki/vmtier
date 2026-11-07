#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_dir"
go_bin=${GO_BIN:-go}
source "$repo_dir/scripts/vm-source.sh"
prepare_vm_source
mkdir -p .bin
# Resolve only the committed versions and regenerate disposable vendor contents.
# Stock VM retains its own original vendor tree for independent E2E comparisons.
"$go_bin" mod download
"$go_bin" mod verify
"$go_bin" mod vendor
