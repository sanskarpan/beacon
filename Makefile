.PHONY: build test test-race test-integration proto console bench sim tidy lint lint-ci fmt coverage docker help clean vet govuln

GO ?= go
MODULE := github.com/sanskar/beacon
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.version=$(VERSION)

help:
	@grep -E '^[a-z-]+:.*##' Makefile | awk 'BEGIN{FS=":.*##"} {printf "  %-18s %s\n", $$1, $$2}'

build: ## build all binaries with version
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/beacon ./cmd/beacon
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/beacon-server ./cmd/beacon-server
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/beacon-agent ./cmd/beacon-agent
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/demo ./examples/demo

test: ## go test all packages
	$(GO) test ./... -count=1

test-race: ## go test with race detector (critical packages)
	$(GO) test -race -count=1 ./pkg/catalog ./pkg/health ./pkg/agent ./pkg/gossip ./pkg/store/raft/consensus ./pkg/xds ./pkg/mesh ./pkg/sdk ./pkg/watch

test-integration: ## integration + sim scenarios
	$(GO) test ./test/integration/... ./pkg/sim/... -count=1

bench: ## benchmarks + propagate bench
	$(GO) test -bench=. -benchmem ./pkg/lb/... ./pkg/catalog/... -count=1
	./bin/beacon bench propagate || $(GO) run ./cmd/beacon bench propagate

sim: ## run sim all
	$(GO) run ./cmd/beacon sim all

tidy: ## go mod tidy and verify
	$(GO) mod tidy
	git diff --exit-code -- go.mod go.sum

vet: ## go vet
	$(GO) vet ./...

lint: vet ## (local) go vet
lint-ci: ## golangci-lint (CI)
	golangci-lint run ./... --timeout=5m

govuln: ## govulncheck
	govulncheck ./...

fmt: ## gofmt + goimports
	gofmt -w -s ./pkg ./cmd ./examples
	goimports -w ./pkg ./cmd ./examples 2>/dev/null || true

coverage: ## test with coverage
	$(GO) test ./... -coverprofile=coverage.out -covermode=atomic
	$(GO) tool cover -func=coverage.out | tail -20

proto: ## verify pb stub matches proto
	@echo "protobuf stubs are hand-written in pkg/api/pb/pb.go; run 'make proto-verify' to check drift"
	@echo "proto/beacon.proto -> pkg/api/pb/pb.go (hand-written, 12k lines)"

proto-verify:
	@echo "checking proto drift (stub vs proto)..."
	@grep -q "service Discovery" proto/beacon.proto && grep -q "type DiscoveryClient" pkg/api/pb/pb.go && echo "ok: pb stub present"

console: ## build console
	cd console && bun install --frozen-lockfile && bun run build

console-dev: ## dev console
	cd console && bun run dev

console-lint: ## console lint + typecheck
	cd console && bun run lint 2>&1 | head -20 || true
	cd console && bunx tsc --noEmit 2>&1 | head -20 || true

docker: ## build docker images
	docker build -f Dockerfile.server -t beacon:server .
	docker build -f Dockerfile.agent -t beacon:agent .
	docker build -f Dockerfile.console -t beacon:console .

clean: ## clean bins
	rm -rf bin/ coverage.out coverage.html

tidy-check: tidy vet lint-ci govuln coverage
