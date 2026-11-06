#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
revision=9a009c491da4c17531893ab4acb6a5f1b5e0eaf2
checksum=acf22564ccd69b10eda3a94bf0b03cc6ec28ef3a0d4c8fb43579d563907616a6
mkdir -p .deps .bin
if [ ! -d .deps/victoriametrics ]; then
  curl --fail --location --retry 3 "https://codeload.github.com/VictoriaMetrics/VictoriaMetrics/tar.gz/$revision" -o .deps/vm.tar.gz
  printf '%s  %s\n' "$checksum" .deps/vm.tar.gz | sha256sum -c -
  tar -xzf .deps/vm.tar.gz -C .deps
  mv ".deps/VictoriaMetrics-$revision" .deps/victoriametrics
fi
# Use the pinned upstream's checked-in dependencies, without network module resolution.
# Generated vendor is disposable; never modify the upstream sources.
python3 scripts/vendor.py
