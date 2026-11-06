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
# SQLite is an additional direct dependency outside the upstream vendor tree.
sqlite_version=v1.14.32
sqlite_checksum=c70101000256656bc0b7286807cf177852c753a001a3aa3c55d5373575fb81aa
sqlite_zip=".deps/go-sqlite3-$sqlite_version.zip"
if [ ! -f "$sqlite_zip" ]; then
  sqlite_tmp=$(mktemp "${sqlite_zip}.download.XXXXXX")
  trap 'rm -f -- "$sqlite_tmp"' EXIT
  curl --fail --location --retry 3 "https://proxy.golang.org/github.com/mattn/go-sqlite3/@v/$sqlite_version.zip" -o "$sqlite_tmp"
  printf '%s  %s\n' "$sqlite_checksum" "$sqlite_tmp" | sha256sum -c -
  mv -- "$sqlite_tmp" "$sqlite_zip"
  trap - EXIT
fi
printf '%s  %s\n' "$sqlite_checksum" "$sqlite_zip" | sha256sum -c -
# PostgreSQL uses a pure-Go driver outside the upstream vendor tree.
pq_version=v1.10.9
pq_checksum=6791a7af67f846a86335350012c4efadd0850096ebf22350b7b20e01e216e6f2
pq_zip=".deps/pq-$pq_version.zip"
if [ ! -f "$pq_zip" ]; then
  pq_tmp=$(mktemp "${pq_zip}.download.XXXXXX")
  trap 'rm -f -- "$pq_tmp"' EXIT
  curl --fail --location --retry 3 "https://proxy.golang.org/github.com/lib/pq/@v/$pq_version.zip" -o "$pq_tmp"
  printf '%s  %s\n' "$pq_checksum" "$pq_tmp" | sha256sum -c -
  mv -- "$pq_tmp" "$pq_zip"
  trap - EXIT
fi
printf '%s  %s\n' "$pq_checksum" "$pq_zip" | sha256sum -c -
# Use pinned source archives without resolving or updating other modules.
# Generated vendor is disposable; never modify the upstream sources.
python3 scripts/vendor.py
