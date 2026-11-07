GO ?= go
GOMAXPROCS ?= 3
GOCACHE ?= $(CURDIR)/.cache/go-build
export GOMAXPROCS GOCACHE
.PHONY: bootstrap build test race vet stock e2e e2e-s3
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
