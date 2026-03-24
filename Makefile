# Define variables
GO_BIN = go
DOCKER_BIN = docker
GO_BUILD_FLAGS = -v
GO_TEST_FLAGS = -race -v
BINARY_NAME = tsdb-aggregate-proxy
DOCKER_NAME = ghcr.io/maratoid/tsdb-aggregate-proxy:1.0.0
MAIN_PACKAGE = .
OUTPUT_DIR = bin

.PHONY: all build run test clean fmt lint tidy

all: build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(OUTPUT_DIR)
	$(GO_BIN) build $(GO_BUILD_FLAGS) -o $(OUTPUT_DIR)/$(BINARY_NAME) $(MAIN_PACKAGE)
	@echo "Build complete. Binary located at $(OUTPUT_DIR)/$(BINARY_NAME)"

docker:
	@echo "Building $(DOCKER_NAME)..."
	$(DOCKER_BIN) build . -f ./Dockerfile -t $(DOCKER_NAME)
	@echo "Build complete."

run: build
	@echo "Running $(BINARY_NAME)..."
	@$(OUTPUT_DIR)/$(BINARY_NAME)

test:
	@echo "Running tests..."
	$(GO_BIN) test $(GO_TEST_FLAGS) ./...

clean:
	@echo "Cleaning up..."
	@rm -rf $(OUTPUT_DIR)
	@$(GO_BIN) clean
	@echo "Cleanup complete."

fmt:
	@echo "Formatting Go code..."
	$(GO_BIN) fmt ./...

lint:
	@echo "Running linters"
	golangci-lint run ./...

tidy:
	@echo "Tidying Go modules..."
	$(GO_BIN) mod tidy