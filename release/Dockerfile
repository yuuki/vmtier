# Release builder for the static linux/amd64 vmtier binary.
#
# The Go toolchain and Alpine base are pinned by image digest. Alpine package
# versions are resolved when this image is built; release/build.sh records them
# in BUILD.json, and the release workflow compares two independent builds.
# Keep the Go version equal to the `go` directive in go.mod.
FROM docker.io/library/golang:1.27.3-alpine3.24@sha256:fa3d596898d9f126af7c01d14ee7597bab3f39cccf392ef36b69fe77cd681f18

RUN apk add --no-cache gcc musl-dev

ENV CGO_ENABLED=1 \
    GOTOOLCHAIN=local \
    GOFLAGS=-modcacherw \
    GOOS=linux \
    GOARCH=amd64 \
    GOAMD64=v1
