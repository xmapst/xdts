#!/usr/bin/env make -f

TOPDIR     := $(realpath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SELF       := $(abspath $(lastword $(MAKEFILE_LIST)))
LDFLAGS    := -w -s
GO_TAGS    := codec.notfastpath
BIN_DIR    := $(TOPDIR)/bin
CMD_DIR    := $(TOPDIR)/cmd
GOLANGCI_VERSION := latest

define go-build
	echo "Building $(3) $(2) for $(1)..."; \
	CGO_ENABLED=0 GOOS=$(1) GOARCH=$(2) go build -trimpath -ldflags "$(LDFLAGS)" --tags $(GO_TAGS) -o $(BIN_DIR)/$(3)_$(1)_$(2)$(if $(filter windows,$(1)),.exe,) $(CMD_DIR)/$(3)/
endef

.PHONY: all
all: frontend backend ## Build everything (frontend + backend)

.PHONY: release
export VERSION REVISION
release: ## Build verified Linux amd64/arm64 release (VERSION required; REVISION optional)
	@bash deploy/release/build.sh "$${VERSION}" "$${REVISION}"

# ==============================================================================
# 环境变量展示（调试用）
# ==============================================================================
SHOW_ENV_VARS := TOPDIR SELF LDFLAGS

show-var-%:
	@{ \
	escaped_v="$(subst ",\",$($*))"; \
	if [ -n "$$escaped_v" ]; then v="$$escaped_v"; else v="(undefined)"; fi; \
	printf "%-19s %s\n" "$*" "$$v"; \
	}

.PHONY: show-env
show-env: $(addprefix show-var-, $(SHOW_ENV_VARS)) ## Show environment details

# ==============================================================================
# 帮助
# ==============================================================================
.PHONY: help
help: ## Show help message (list targets)
	@awk 'BEGIN {FS = ":.*##"; printf "\nTargets:\n"} /^[$$()% 0-9a-zA-Z_-]+:.*?##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(SELF)

# ==============================================================================
# 前端构建
# ==============================================================================
WEB_DIR     := $(TOPDIR)/web

.PHONY: frontend
frontend: ## Build Manager UI
	@echo "Building UI..."
	@cd $(WEB_DIR) && npm install --silent && npx vite build

# ==============================================================================
# 本地构建
# ==============================================================================
.PHONY: clean
clean: ## Clean up build artifacts
	@echo "Cleaning up..."
	@rm -rf $(BIN_DIR)

.PHONY: preprocessing
preprocessing: ## Preprocessing code
	@echo "Preprocessing code..."
	@{ \
	set -e; \
	cd $(TOPDIR); \
	go fix ./...; \
	go mod tidy; \
	go generate ./...; \
	go vet ./...; \
	go fmt ./...; \
	}

.PHONY: lint
lint: ## Run golangci-lint (auto-install to GOBIN if missing)
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found, installing $(GOLANGCI_VERSION)..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION); \
	}
	@echo "Running golangci-lint..."
	@golangci-lint run ./...

.PHONY: test
test: ## Run unit tests (race detector on)
	@echo "Running tests..."
	@go test -race ./...

.PHONY: compress
compress: ## Compress binaries with UPX
	@echo "Stripping supported binaries when possible..."
	@command -v strip >/dev/null 2>&1 || { echo "Warning: strip command not found, skipping strip."; }
	@if command -v strip >/dev/null 2>&1; then \
		for f in $(BIN_DIR)/*; do \
			[ -f "$$f" ] || continue; \
			strip --strip-unneeded "$$f" >/dev/null 2>&1 || true; \
		done; \
	fi
	@echo "Compressing binaries with UPX when possible..."
	@command -v upx >/dev/null 2>&1 || { echo "Warning: upx command not found, skipping compression."; }
	@if command -v upx >/dev/null 2>&1; then \
		for f in $(BIN_DIR)/*; do \
			[ -f "$$f" ] || continue; \
			upx --force-overwrite --lzma --quiet --force-macos --best "$$f" >/dev/null 2>&1 || true; \
		done; \
	fi

.PHONY: backend
backend: clean preprocessing ## Build binaries
	@echo "Building binaries..."
	@mkdir -p $(BIN_DIR)
	@\
	pids=""; \
	$(call go-build,linux,amd64,manager) & pids="$$pids $$!"; \
	$(call go-build,linux,amd64,node) & pids="$$pids $$!"; \
	\
	errors=0; \
	for pid in $$pids; do \
		wait $$pid || errors=$$((errors + 1)); \
	done; \
	if [ $$errors -ne 0 ]; then \
		echo "Error: $$errors build task(s) failed."; \
		exit 1; \
	fi
