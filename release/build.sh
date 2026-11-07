#!/bin/sh
# Runs inside the image built from release/Dockerfile; scripts/release.py
# starts it. /src holds `git archive` output of the release commit, /out
# receives the binary and build records, and GOMODCACHE is a fresh directory.
set -eu
: "${VMTIER_VERSION:?}" "${VMTIER_COMMIT:?}"
cd /src

want="go$(sed -n 's/^go \([0-9][0-9.]*\)$/\1/p' go.mod)"
have=$(go env GOVERSION)
if [ "$want" != "$have" ]; then
	echo "release builder has $have; go.mod requires $want" >&2
	exit 1
fi

# Resolve only committed versions. Go checks downloads against go.sum and the
# checksum database; verify re-hashes the cache before vendoring.
go mod download
go mod verify
go mod vendor

tags=netgo,osusergo,musl
ldflags="-s -w -X main.version=$VMTIER_VERSION -X main.commit=$VMTIER_COMMIT -linkmode=external -extldflags=-static"

if [ "${VMTIER_RELEASE_UNIT_TESTS:-0}" = 1 ]; then
	# Exercise SQLite and the codecs against the same libc and linker mode.
	go test -mod=vendor -trimpath -count=1 -p "${GOMAXPROCS:-3}" -tags "$tags" \
		-ldflags "-linkmode=external -extldflags=-static" ./...
fi

go build -mod=vendor -trimpath -buildvcs=false -tags "$tags" -ldflags "$ldflags" \
	-o /out/vmtier ./cmd/vmtier
go version -m /out/vmtier >/out/go-version-m.txt
go env -json GOVERSION GOOS GOARCH GOAMD64 CGO_ENABLED CC CGO_CFLAGS CGO_LDFLAGS \
	GOFLAGS GOPROXY GOSUMDB GOTOOLCHAIN >/out/go-env.json
cp "$(go env GOROOT)/LICENSE" /out/go-LICENSE

{
	printf 'alpine_release=%s\n' "$(cat /etc/alpine-release)"
	printf 'cc=%s\n' "$(gcc --version | head -n 1)"
	printf 'ld=%s\n' "$(ld --version | head -n 1)"
	for package in gcc libgcc musl musl-dev binutils; do
		printf 'apk=%s\n' "$(apk info -e -v "$package")"
	done
	printf 'tags=%s\n' "$tags"
	printf 'ldflags=%s\n' "$ldflags"
	printf 'command=go build -mod=vendor -trimpath -buildvcs=false -tags %s -ldflags "%s" -o vmtier ./cmd/vmtier\n' "$tags" "$ldflags"
} >/out/toolchain.txt
