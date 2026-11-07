GO ?= go
GOMAXPROCS ?= 3
GOCACHE ?= $(CURDIR)/.cache/go-build
GOVULNCHECK_VERSION ?= v1.9.0
PYTHON ?= python3
export GOMAXPROCS GOCACHE
.PHONY: bootstrap build test race vet stock e2e e2e-s3 vulncheck release-snapshot
bootstrap:
	GO_BIN="$(GO)" ./scripts/bootstrap.sh
build:
	$(GO) build -mod=vendor -p $(GOMAXPROCS) -o .bin/vmtier ./cmd/vmtier
test:
	$(GO) test -mod=vendor -p $(GOMAXPROCS) ./...
race:
	$(GO) test -race -mod=vendor -p $(GOMAXPROCS) ./...
vet:
	$(GO) vet -mod=vendor -p $(GOMAXPROCS) ./...
stock:
	GO_BIN="$(GO)" ./scripts/build-vm.sh
e2e: build stock
	python3 scripts/e2e.py --backend local
e2e-s3: build stock
	python3 scripts/e2e.py --backend s3 --replicas 2
vulncheck:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
# Static linux/amd64 archive of HEAD from the pinned builder; requires Docker.
release-snapshot:
	rm -rf dist
	$(PYTHON) scripts/release.py build --output dist
	$(PYTHON) scripts/release.py smoke dist
