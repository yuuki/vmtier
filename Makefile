GO ?= go
GOMAXPROCS ?= 3
GOCACHE ?= $(CURDIR)/.cache/go-build
PYTHON ?= python3
# Tool versions. CI pins the same versions (.github/workflows); bump together.
GOVULNCHECK_VERSION ?= v1.9.0
GOLANGCI_LINT_VERSION ?= v2.15.0
ACTIONLINT_VERSION ?= v1.7.12
ZIZMOR_VERSION ?= 1.32.0
FUZZTIME ?= 30s
# TLA+ tools that model-check spec/ (a release, not the rolling nightly).
TLA_VERSION ?= 1.7.4
TLA_SHA256 ?= 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
TLA_JAR ?= $(CURDIR)/.cache/tla/tla2tools-$(TLA_VERSION).jar
JAVA ?= java
BENCHTIME ?= 1s
BENCH_COUNT ?= 6
# Build go-run tools with this module's toolchain (go.mod), not the one of the
# tool's own go.mod: golangci-lint built with an older Go refuses this module.
GO_TOOLCHAIN = $(shell $(GO) env GOVERSION)
GOLANGCI_LINT ?= GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
# uvx or pipx (preinstalled on GitHub-hosted runners) runs the pinned zizmor.
ZIZMOR ?= $(if $(shell command -v uvx 2>/dev/null),uvx zizmor@$(ZIZMOR_VERSION),pipx run zizmor==$(ZIZMOR_VERSION))
export GOMAXPROCS GOCACHE
.PHONY: bootstrap build test race vet stock e2e e2e-s3 vulncheck release-snapshot
.PHONY: check lint fmt lint-actions test-race test-scripts test-fuzz test-integration test-tla bench vuln
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
	GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
# Static linux/amd64 archive of HEAD from the pinned builder; requires Docker.
release-snapshot:
	rm -rf dist
	$(PYTHON) scripts/release.py build --output dist
	$(PYTHON) scripts/release.py smoke dist
# Local pre-PR gate after bootstrap: the checks of the required CI path that
# need no database, Docker, stock VM binaries or network services.
check: lint lint-actions test-race test-scripts
# golangci-lint (.golangci.yml, reads vendor/) and go.mod/go.sum tidiness.
lint:
	$(GOLANGCI_LINT) run
	$(GO) mod tidy -diff
fmt:
	$(GOLANGCI_LINT) fmt
# Workflow syntax (actionlint) and security (zizmor; online audits with GH_TOKEN).
lint-actions:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	$(ZIZMOR) $(if $(GH_TOKEN),,--offline) .
test-race: race
# Standard-library unit tests of the Python harnesses and release tooling.
test-scripts:
	$(PYTHON) -m unittest discover -s scripts -p 'test_*.py'
# Every Go fuzz target for FUZZTIME each; crashers are saved under testdata/fuzz.
test-fuzz:
	@set -e; for pkg in $$($(GO) list -mod=vendor ./...); do \
		targets=$$($(GO) test -mod=vendor -list '^Fuzz' $$pkg); \
		for f in $$(printf '%s\n' "$$targets" | grep '^Fuzz'); do \
			echo "== $$pkg $$f"; \
			$(GO) test -mod=vendor -run='^$$' -fuzz="^$$f\$$" -fuzztime=$(FUZZTIME) -parallel=$(GOMAXPROCS) $$pkg; \
		done; \
	done
# TLC over the publication model in spec/: every configuration must end as its
# first line expects. Needs Java 11 or later; downloads the pinned jar once.
test-tla:
	$(PYTHON) scripts/tlc.py --java $(JAVA) --jar $(TLA_JAR) --version $(TLA_VERSION) --sha256 $(TLA_SHA256) spec/VmtierPublication.tla
# PostgreSQL repository and CLI tests, which skip without a database. They
# create and drop their own schemas in a disposable database.
test-integration:
	@test -n "$$VMTIER_TEST_POSTGRES_DSN" || { echo 'set VMTIER_TEST_POSTGRES_DSN to a disposable PostgreSQL database' >&2; exit 1; }
	$(GO) test -race -mod=vendor -p $(GOMAXPROCS) -count=1 ./cmd/vmtier ./internal/catalog
# Benchmarks in benchstat format, written to bench.txt.
bench:
	@$(GO) test -mod=vendor -p 1 -run='^$$' -bench=. -benchmem -benchtime=$(BENCHTIME) -count=$(BENCH_COUNT) ./... > bench.txt; \
		status=$$?; cat bench.txt; exit $$status
vuln: vulncheck
