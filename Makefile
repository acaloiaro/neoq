SHELL = /bin/bash
GOLANGCI_LINT_VERSION ?= v1.51.1

all: fmt vet mod

.PHONY: mod
mod:
	@go mod tidy -compat=1.20
	@go mod verify
	@go mod vendor

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: vet
vet:
	@go vet ./...

.PHONY: lint
lint:
	@clear
	@golangci-lint run .

.PHONY: lint-watch
lint-watch:
	@reflex -s -r '\.go$$' -R '^vendor/' -- make lint

.PHONY: install-dev-tools
install-dev-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin $(GOLANGCI_LINT_VERSION)
	@go install github.com/cespare/reflex@latest
