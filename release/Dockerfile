# Release builder for the static linux/amd64 vmtier binary.
#
# The Go toolchain and Alpine base are pinned by image digest. Alpine package
# versions are resolved when this image is built; release/build.sh records them
# in BUILD.json, and the release workflow compares two independent builds.
# Keep the Go version equal to the `go` directive in go.mod.
FROM docker.io/library/golang:1.27.4-alpine3.24@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf

RUN apk add --no-cache gcc musl-dev

ENV CGO_ENABLED=1 \
    GOTOOLCHAIN=local \
    GOFLAGS=-modcacherw \
    GOOS=linux \
    GOARCH=amd64 \
    GOAMD64=v1
