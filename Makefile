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
	@go vet -tags testing ./...

.PHONY: test
test: install-gotestsum
	@mkdir -p tmp/output tmp/coverage
	@gotestsum \
		--junitfile tmp/output/gotestsum-report.xml \
		-- \
		-count=5 \
		-race \
		-cover \
		-tags testing \
		-coverprofile=tmp/coverage/coverage.out \
		./...

.PHONY: coverage
coverage:
	@go tool cover -html=tmp/coverage/coverage.out -o tmp/coverage/coverage.html

.PHONY: func-coverage
func-coverage:
	@go tool cover -func=tmp/coverage/coverage.out

.PHONY: update-dependencies
update-dependencies:
	@go get -u -v ./...
	$(MAKE) mod

.PHONY: test-watch
test-watch:
	@gotestsum \
		--watch \
		--junitfile tmp/output/gotestsum-report.xml \
		--post-run-command "make coverage" \
		-- \
		-race \
		-cover \
		-coverprofile=tmp/coverage/coverage.out \
		./...

.PHONY: lint
lint:
	@clear
	@golangci-lint run

.PHONY: lint-watch
lint-watch: install-reflex
	@reflex -s -r '\.go$$' -R '^vendor/' -- make lint

.PHONY: install-dev-tools
install-dev-tools: install-gotestsum install-reflex
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin $(GOLANGCI_LINT_VERSION)

.PHONY: install-gotestsum
install-gotestsum:
	@go install gotest.tools/gotestsum@latest

.PHONY: install-reflex
install-reflex:
	@go install github.com/cespare/reflex@latest
