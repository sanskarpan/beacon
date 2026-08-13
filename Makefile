.PHONY: build test test-race test-integration proto console bench sim tidy lint

GO ?= go
MODULE := github.com/sanskar/beacon

build:
	$(GO) build -o bin/beacon ./cmd/beacon
	$(GO) build -o bin/beacon-server ./cmd/beacon-server
	$(GO) build -o bin/beacon-agent ./cmd/beacon-agent
	$(GO) build -o bin/demo ./examples/demo

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	$(GO) test ./test/integration/... ./pkg/sim/...

bench:
	$(GO) test -bench=. -benchmem ./pkg/lb/... ./pkg/catalog/...
	./bin/beacon bench propagate || $(GO) run ./cmd/beacon bench propagate

sim:
	$(GO) run ./cmd/beacon sim all

tidy:
	$(GO) mod tidy

lint:
	$(GO) vet ./...

proto:
	@echo "protobuf stubs are hand-written in pkg/api/grpcapi for now"

console:
	cd console && bun install && bun run build

console-dev:
	cd console && bun run dev
