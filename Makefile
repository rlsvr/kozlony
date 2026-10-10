MODULE  := github.com/rlsvr/kozlony
BINARY  := bin/kozlony
CMD     := ./cmd/kozlony
STRESS_BINARY := bin/stresstest
STRESS_CMD    := ./tools/stress
TUI_BINARY    := bin/kozlony-tui
TUI_CMD       := ./tools/tui

OAPI_CODEGEN  := oapi-codegen
GOLANGCI_LINT := golangci-lint
GO_JSONSCHEMA := go run github.com/atombender/go-jsonschema@v0.22.0

ASYNCAPI_CODEGEN_FLAGS := --only-models --struct-name-from-title --resolve-extension json \
                          --capitalization ID --tags json

# Load local environment variables from .env if present
-include .env
export

.PHONY: all build run test test-verbose fmt lint lint-fix check clean \
	openapi-gen asyncapi-gen gen mocks build-stress stress build-tui tui

## all: generate + build + test
all: gen mocks build test

## mocks: generate test mocks via uber mockgen
mocks:
	go generate ./...

## gen: generate OpenAPI and AsyncAPI stubs
gen: openapi-gen asyncapi-gen

## openapi-gen: generate Echo server interfaces from OpenAPI spec
openapi-gen:
	@mkdir -p internal/api/server
	$(OAPI_CODEGEN) --config api/oapi-codegen.yaml api/openapi.yaml

## asyncapi-gen: generate Go structs from AsyncAPI JSON schemas
asyncapi-gen:
	@mkdir -p internal/messaging/events internal/messaging/commands
	$(GO_JSONSCHEMA) $(ASYNCAPI_CODEGEN_FLAGS) \
		--package events \
		-o internal/messaging/events/interaction_created.go \
		schemas/events/interaction-created/v0.1.0.json
	$(GO_JSONSCHEMA) $(ASYNCAPI_CODEGEN_FLAGS) \
		--package events \
		-o internal/messaging/events/interaction_edited.go \
		schemas/events/interaction-edited/v0.1.0.json
	$(GO_JSONSCHEMA) $(ASYNCAPI_CODEGEN_FLAGS) \
		--package events \
		-o internal/messaging/events/reply_counts_recalculated.go \
		schemas/events/reply-counts-recalculated/v0.1.0.json
	$(GO_JSONSCHEMA) $(ASYNCAPI_CODEGEN_FLAGS) \
		--package commands \
		-o internal/messaging/commands/create_interaction.go \
		schemas/commands/create-interaction/v0.1.0.json

## build: compile the application binary
build:
	@mkdir -p bin
	go build -o $(BINARY) $(CMD)

## build-stress: compile the stress testing CLI binary
build-stress:
	@mkdir -p bin
	go build -o $(STRESS_BINARY) $(STRESS_CMD)

## stress: run the stress test tool (pass ARGS="-users 50 -rps 100")
stress: build-stress
	./$(STRESS_BINARY) $(ARGS)

## build-tui: compile the interactive TUI load monitor
build-tui:
	@mkdir -p bin
	go build -o $(TUI_BINARY) $(TUI_CMD)

## tui: run the interactive TUI load & stack monitor (pass ARGS="..." to customize)
tui: build-tui
	./$(TUI_BINARY) $(ARGS)

## run: compile and execute the application
run: build
	go run $(CMD)

## test: run tests with race detector
test:
	go test -race ./...

## test-verbose: run tests with race detector in verbose mode
test-verbose:
	go test -race -v ./...

## test-blackbox: run end-to-end blackbox tests against running service
test-blackbox:
	go run ./scripts/blackbox


## fmt: apply formatting via golangci-lint
fmt:
	$(GOLANGCI_LINT) fmt

## lint: report lint issues
lint:
	$(GOLANGCI_LINT) run ./...

## lint-fix: apply auto-fixable lint issues
lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

## check: fmt + lint + go mod tidy
check: fmt lint
	go mod tidy

## clean: remove generated binaries
clean:
	rm -rf bin/
