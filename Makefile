# Ferrite - High-Performance Key-Value Store
# Makefile for common development tasks

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
CYAN := \033[0;36m
NC := \033[0m
BOLD := \033[1m

# Project configuration
PROJECT_NAME := ferrite
DOCKER_IMAGE := ferrite:latest
CARGO := cargo
RUST_LOG ?= info

# Default target
.DEFAULT_GOAL := help

.PHONY: all build release test bench lint fmt check clean doc run help quickstart ping smoke-test docker-ping
.PHONY: docker docker-build docker-run docker-stop
.PHONY: install-tools coverage install docs setup dev dev-full dev-test test-fast

all: check test build

##@ Build Targets

build: ## Build debug version
	@echo "$(BLUE)$(BOLD)Building $(PROJECT_NAME) (debug)...$(NC)"
	@$(CARGO) build
	@echo "$(GREEN)Build complete!$(NC)"

release: ## Build release version with optimizations
	@echo "$(BLUE)$(BOLD)Building $(PROJECT_NAME) (release)...$(NC)"
	@$(CARGO) build --release
	@echo "$(GREEN)Release build complete!$(NC)"

build-all: ## Build with all features
	@echo "$(BLUE)$(BOLD)Building with all features...$(NC)"
	@$(CARGO) build --all-features
	@echo "$(GREEN)Build complete!$(NC)"

build-tui: ## Build with TUI feature
	@echo "$(BLUE)$(BOLD)Building TUI...$(NC)"
	@$(CARGO) build --features tui
	@echo "$(GREEN)TUI build complete!$(NC)"

install: release ## Install the binary locally
	@echo "$(BLUE)$(BOLD)Installing $(PROJECT_NAME)...$(NC)"
	@$(CARGO) install --path .
	@echo "$(GREEN)Installation complete! Binary installed to ~/.cargo/bin/$(NC)"

##@ Testing Targets

test: ## Run all tests
	@echo "$(BLUE)$(BOLD)Running tests...$(NC)"
	@$(CARGO) test
	@echo "$(GREEN)Tests passed!$(NC)"

test-fast: ## Run unit tests only (fast inner-loop feedback)
	@echo "$(BLUE)$(BOLD)Running unit tests...$(NC)"
	@$(CARGO) test --lib
	@echo "$(GREEN)Unit tests passed!$(NC)"

test-release: ## Run tests with optimizations
	@echo "$(BLUE)$(BOLD)Running tests (release mode)...$(NC)"
	@$(CARGO) test --release
	@echo "$(GREEN)Tests passed!$(NC)"

test-all: ## Run tests with all features
	@echo "$(BLUE)$(BOLD)Running tests with all features...$(NC)"
	@$(CARGO) test --all-features
	@echo "$(GREEN)Tests passed!$(NC)"

test-verbose: ## Run tests with output and backtrace
	@echo "$(BLUE)$(BOLD)Running tests (verbose)...$(NC)"
	@RUST_BACKTRACE=1 $(CARGO) test -- --nocapture

##@ Benchmark Targets

bench: ## Run all benchmarks
	@echo "$(BLUE)$(BOLD)Running benchmarks...$(NC)"
	@$(CARGO) bench
	@echo "$(GREEN)Benchmarks complete!$(NC)"

bench-throughput: ## Run throughput benchmark
	@echo "$(BLUE)$(BOLD)Running throughput benchmark...$(NC)"
	@$(CARGO) bench --bench throughput

bench-latency: ## Run latency benchmark
	@echo "$(BLUE)$(BOLD)Running latency benchmark...$(NC)"
	@$(CARGO) bench --bench latency

bench-epoch: ## Run epoch reclamation sweep benchmark
	@echo "$(BLUE)$(BOLD)Running epoch reclamation benchmark...$(NC)"
	@$(CARGO) bench --bench epoch_sweep

##@ Code Quality Targets

lint: ## Run clippy linter
	@echo "$(BLUE)$(BOLD)Running clippy...$(NC)"
	@$(CARGO) clippy --all-targets --all-features -- -D warnings
	@echo "$(GREEN)Clippy checks passed!$(NC)"

fmt: ## Format code with rustfmt
	@echo "$(BLUE)$(BOLD)Formatting code...$(NC)"
	@$(CARGO) fmt --all
	@echo "$(GREEN)Code formatted!$(NC)"

fmt-check: ## Check code formatting without modifying files
	@echo "$(BLUE)$(BOLD)Checking code formatting...$(NC)"
	@$(CARGO) fmt --all -- --check
	@echo "$(GREEN)Format check passed!$(NC)"

check: fmt-check lint test ## Run all checks (fmt, clippy, tests)
	@echo "$(BLUE)$(BOLD)Running final check...$(NC)"
	@$(CARGO) check --all-features
	@echo "$(GREEN)$(BOLD)All checks passed!$(NC)"

##@ Documentation Targets

docs: ## Generate and open documentation
	@echo "$(BLUE)$(BOLD)Generating documentation...$(NC)"
	@$(CARGO) doc --no-deps --all-features --open
	@echo "$(GREEN)Documentation generated!$(NC)"

doc: docs ## Alias for docs

doc-build: ## Generate documentation without opening
	@echo "$(BLUE)$(BOLD)Generating documentation...$(NC)"
	@$(CARGO) doc --no-deps --all-features
	@echo "$(GREEN)Documentation generated at target/doc/$(NC)"

docs-all: ## Generate documentation including dependencies
	@echo "$(BLUE)$(BOLD)Generating documentation (with dependencies)...$(NC)"
	@$(CARGO) doc --all-features --open
	@echo "$(GREEN)Documentation generated!$(NC)"

check-docs: ## Report missing documentation across crates
	@./scripts/check-docs.sh

##@ Cleanup Targets

clean: ## Clean build artifacts
	@echo "$(YELLOW)$(BOLD)Cleaning build artifacts...$(NC)"
	@$(CARGO) clean
	@echo "$(GREEN)Clean complete!$(NC)"

clean-all: ## Clean all artifacts including target directory
	@echo "$(YELLOW)$(BOLD)Cleaning all build artifacts...$(NC)"
	@$(CARGO) clean
	@rm -rf target/
	@echo "$(GREEN)All artifacts cleaned!$(NC)"

##@ Runtime Targets

run: ## Run the server locally (release mode)
	@echo "$(BLUE)$(BOLD)Starting $(PROJECT_NAME) server...$(NC)"
	@echo "$(YELLOW)Log level: $(RUST_LOG)$(NC)"
	@RUST_LOG=$(RUST_LOG) $(CARGO) run --release

run-debug: ## Run the server with debug logging
	@echo "$(BLUE)$(BOLD)Starting $(PROJECT_NAME) server (debug mode)...$(NC)"
	@RUST_LOG=ferrite=debug $(CARGO) run

run-trace: ## Run the server with trace logging
	@echo "$(BLUE)$(BOLD)Starting $(PROJECT_NAME) server (trace mode)...$(NC)"
	@RUST_LOG=ferrite=trace $(CARGO) run

quickstart: ## Build, initialize config, and run locally
	@echo "$(YELLOW)NOTE: quickstart script moved to https://github.com/ferritelabs/ferrite-ops$(NC)"
	@echo "$(BLUE)Building and running locally instead...$(NC)"
	@$(CARGO) build --release && $(CARGO) run --release

smoke-test: ## Build, start, PING, and stop the server
	@echo "$(YELLOW)NOTE: smoke_test script moved to https://github.com/ferritelabs/ferrite-ops$(NC)"
	@$(CARGO) build --release

run-tui: ## Run the TUI interface
	@echo "$(BLUE)$(BOLD)Starting $(PROJECT_NAME) TUI...$(NC)"
	@$(CARGO) run --release --features tui --bin ferrite-tui

run-cli: ## Run the CLI interface
	@echo "$(BLUE)$(BOLD)Starting $(PROJECT_NAME) CLI...$(NC)"
	@$(CARGO) run --release --bin ferrite-cli

ping: ## PING the local server
	@$(CARGO) run --release --bin ferrite-cli -- PING

##@ Docker Targets
##  NOTE: Docker/compose files live in https://github.com/ferritelabs/ferrite-ops
##  These targets assume you have ferrite-ops cloned alongside this repo.

docker-build: ## Build Docker image (requires ../ferrite-ops/Dockerfile)
	@echo "$(BLUE)$(BOLD)Building Docker image: $(DOCKER_IMAGE)...$(NC)"
	@docker build -t $(DOCKER_IMAGE) -f ../ferrite-ops/Dockerfile .
	@echo "$(GREEN)Docker image built successfully!$(NC)"

docker-run: ## Run in Docker container
	@echo "$(BLUE)$(BOLD)Running $(PROJECT_NAME) in Docker...$(NC)"
	@docker run -d --name ferrite -p 6379:6379 -p 9090:9090 $(DOCKER_IMAGE)
	@echo "$(GREEN)Container started! Listening on ports 6379 (Redis) and 9090 (Metrics)$(NC)"

docker-stop: ## Stop and remove Docker container
	@echo "$(YELLOW)$(BOLD)Stopping Docker container...$(NC)"
	@docker stop ferrite && docker rm ferrite
	@echo "$(GREEN)Container stopped and removed$(NC)"

docker-shell: ## Open a shell in Docker container
	@echo "$(BLUE)$(BOLD)Opening shell in Docker container...$(NC)"
	@docker run -it --entrypoint /bin/sh $(DOCKER_IMAGE)

docker-compose-up: ## Start services with docker compose (requires ../ferrite-ops/)
	@echo "$(BLUE)$(BOLD)Starting docker compose services...$(NC)"
	@docker compose -f ../ferrite-ops/docker-compose.yml up -d
	@echo "$(GREEN)Services started!$(NC)"

docker-ping: ## PING ferrite in docker compose
	@docker compose -f ../ferrite-ops/docker-compose.yml exec -T ferrite ferrite-cli PING

docker-compose-down: ## Stop docker compose services
	@echo "$(YELLOW)$(BOLD)Stopping docker compose services...$(NC)"
	@docker compose -f ../ferrite-ops/docker-compose.yml down
	@echo "$(GREEN)Services stopped$(NC)"

docker-compose-logs: ## View docker compose logs
	@echo "$(BLUE)$(BOLD)Following docker compose logs...$(NC)"
	@docker compose logs -f

docker-monitoring: ## Start with monitoring stack
	@echo "$(BLUE)$(BOLD)Starting monitoring stack...$(NC)"
	@docker compose --profile monitoring up -d
	@echo "$(GREEN)Monitoring stack started!$(NC)"

##@ Development Tools

setup: ## Set up complete development environment
	@echo "$(BLUE)$(BOLD)Setting up Ferrite development environment...$(NC)"
	@echo ""
	@echo "$(BOLD)1/5 Checking Rust toolchain...$(NC)"
	@command -v cargo >/dev/null 2>&1 || { echo "$(RED)Rust not found. Install from https://rustup.rs/$(NC)"; exit 1; }
	@echo "  $(GREEN)Rust $$(rustc --version | awk '{print $$2}') found$(NC)"
	@rustup component add clippy rustfmt 2>/dev/null || true
	@echo ""
	@echo "$(BOLD)2/5 Checking system dependencies...$(NC)"
	@if [ "$$(uname -s)" = "Linux" ]; then \
		command -v pkg-config >/dev/null 2>&1 || { echo "$(RED)Missing: pkg-config$(NC)"; echo "  Install: sudo apt-get install -y pkg-config libssl-dev"; exit 1; }; \
		pkg-config --exists openssl 2>/dev/null || { echo "$(RED)Missing: OpenSSL headers$(NC)"; echo "  Install: sudo apt-get install -y libssl-dev"; exit 1; }; \
	fi
	@echo "  $(GREEN)System dependencies OK$(NC)"
	@echo ""
	@echo "$(BOLD)3/5 Installing Cargo tools...$(NC)"
	@$(CARGO) install cargo-watch cargo-audit cargo-outdated cargo-deny cargo-llvm-cov 2>/dev/null || true
	@echo "  $(GREEN)Cargo tools installed$(NC)"
	@echo ""
	@echo "$(BOLD)4/5 Setting up pre-commit hooks...$(NC)"
	@if command -v pip3 >/dev/null 2>&1 || command -v pip >/dev/null 2>&1; then \
		if command -v pre-commit >/dev/null 2>&1; then \
			pre-commit install && echo "  $(GREEN)Pre-commit hooks installed$(NC)"; \
		else \
			echo "  $(YELLOW)pre-commit not found. Install with: pip install pre-commit$(NC)"; \
		fi; \
	else \
		echo "  $(YELLOW)Python not found, skipping pre-commit hooks (optional)$(NC)"; \
	fi
	@echo ""
	@echo "$(BOLD)5/5 Verifying build...$(NC)"
	@$(CARGO) check 2>/dev/null && echo "  $(GREEN)Build verified$(NC)" || { echo "$(RED)Build failed$(NC)"; exit 1; }
	@echo ""
	@echo "$(GREEN)$(BOLD)Setup complete! Next steps:$(NC)"
	@echo "  $(CYAN)make quickstart$(NC)   Build and start the server"
	@echo "  $(CYAN)make dev$(NC)          Start development with auto-reload"
	@echo "  $(CYAN)make check$(NC)        Run all quality checks"
	@echo "  $(CYAN)make test$(NC)         Run the test suite"
	@echo ""

install-tools: ## Install development tools
	@echo "$(BLUE)$(BOLD)Installing development tools...$(NC)"
	@$(CARGO) install cargo-watch cargo-audit cargo-outdated cargo-deny cargo-llvm-cov
	@echo "$(GREEN)Development tools installed!$(NC)"

watch: ## Watch for changes and run checks
	@echo "$(BLUE)$(BOLD)Watching for changes...$(NC)"
	@$(CARGO) watch -x check -x test

watch-run: ## Watch for changes and run the server
	@echo "$(BLUE)$(BOLD)Watching for changes and running server...$(NC)"
	@$(CARGO) watch -x 'run --bin ferrite'

dev: ## Start development server with auto-reload and debug logging
	@echo "$(BLUE)$(BOLD)Starting development server (auto-reload + debug logging)...$(NC)"
	@echo "$(YELLOW)Watching for file changes. Uses --no-default-features for fast rebuilds.$(NC)"
	@echo "$(YELLOW)Use 'make dev-full' to include all default features. Press Ctrl+C to stop.$(NC)"
	@RUST_LOG=ferrite=debug $(CARGO) watch -x 'run --no-default-features --bin ferrite'

dev-full: ## Start development server with all default features
	@echo "$(BLUE)$(BOLD)Starting development server (all features + auto-reload)...$(NC)"
	@echo "$(YELLOW)Watching for file changes. Press Ctrl+C to stop.$(NC)"
	@RUST_LOG=ferrite=debug $(CARGO) watch -x 'run --bin ferrite'

dev-test: ## Watch for changes and run tests continuously
	@echo "$(BLUE)$(BOLD)Watching for changes and running tests...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop.$(NC)"
	@$(CARGO) watch -x 'test --lib'

coverage: ## Generate code coverage report
	@echo "$(BLUE)$(BOLD)Generating coverage report...$(NC)"
	@$(CARGO) llvm-cov --all-features --html
	@echo "$(GREEN)Coverage report generated at target/llvm-cov/html/index.html$(NC)"

coverage-install: ## Install coverage tool
	@echo "$(BLUE)$(BOLD)Installing cargo-llvm-cov...$(NC)"
	@$(CARGO) install cargo-llvm-cov
	@echo "$(GREEN)cargo-llvm-cov installed!$(NC)"

##@ Security and Maintenance

audit: ## Run security audit
	@echo "$(BLUE)$(BOLD)Running security audit...$(NC)"
	@$(CARGO) audit
	@echo "$(GREEN)Security audit complete!$(NC)"

outdated: ## Check for outdated dependencies
	@echo "$(BLUE)$(BOLD)Checking for outdated dependencies...$(NC)"
	@$(CARGO) outdated
	@echo "$(GREEN)Check complete!$(NC)"

deny: ## Run cargo-deny checks
	@echo "$(BLUE)$(BOLD)Running cargo-deny checks...$(NC)"
	@$(CARGO) deny check
	@echo "$(GREEN)Deny checks passed!$(NC)"

update: ## Update dependencies
	@echo "$(BLUE)$(BOLD)Updating dependencies...$(NC)"
	@$(CARGO) update
	@echo "$(GREEN)Dependencies updated!$(NC)"

dead-code-audit: ## List all #[allow(dead_code)] annotations
	@echo "$(BLUE)$(BOLD)Dead code audit — items planned for future versions:$(NC)"
	@grep -rn 'allow(dead_code)' src/ crates/ --include='*.rs' | grep -v test | grep -v '#!' | wc -l | xargs -I{} echo "  Total annotations: {}"
	@echo ""
	@grep -rn 'allow(dead_code)' src/ crates/ --include='*.rs' | grep -v test | grep -v '#!'

##@ Workflow Helpers

pre-commit: fmt-check lint test ## Run pre-commit checks
	@echo "$(GREEN)$(BOLD)All pre-commit checks passed!$(NC)"

ci: fmt-check lint test doc-build ## Simulate CI checks
	@echo "$(GREEN)$(BOLD)CI checks passed!$(NC)"

prepare-release: clean ci bench ## Prepare for release
	@echo "$(GREEN)$(BOLD)Ready for release!$(NC)"

msrv: ## Check Minimum Supported Rust Version
	@echo "$(BLUE)$(BOLD)Checking MSRV (1.88)...$(NC)"
	@$(CARGO) +1.88 check --all-features
	@echo "$(GREEN)MSRV check passed!$(NC)"

completions: ## Generate shell completions for bash, zsh, fish
	@echo "$(BLUE)$(BOLD)Generating shell completions...$(NC)"
	@mkdir -p target/completions
	@$(CARGO) run --release --bin ferrite -- completions bash > target/completions/ferrite.bash 2>/dev/null || echo "$(YELLOW)ferrite completions not yet supported — add clap_complete$(NC)"
	@$(CARGO) run --release --bin ferrite -- completions zsh > target/completions/_ferrite 2>/dev/null || true
	@$(CARGO) run --release --bin ferrite -- completions fish > target/completions/ferrite.fish 2>/dev/null || true
	@$(CARGO) run --release --bin ferrite-cli -- completions bash > target/completions/ferrite-cli.bash 2>/dev/null || true
	@$(CARGO) run --release --bin ferrite-cli -- completions zsh > target/completions/_ferrite-cli 2>/dev/null || true
	@$(CARGO) run --release --bin ferrite-cli -- completions fish > target/completions/ferrite-cli.fish 2>/dev/null || true
	@echo "$(GREEN)Completions written to target/completions/$(NC)"

man-pages: ## Generate man pages (requires clap_mangen)
	@echo "$(BLUE)$(BOLD)Generating man pages...$(NC)"
	@mkdir -p target/man
	@$(CARGO) run --release --bin ferrite -- man > target/man/ferrite.1 2>/dev/null || echo "$(YELLOW)Man page generation not yet supported — add clap_mangen$(NC)"
	@$(CARGO) run --release --bin ferrite-cli -- man > target/man/ferrite-cli.1 2>/dev/null || true
	@echo "$(GREEN)Man pages written to target/man/$(NC)"

##@ Help

help: ## Display this help message
	@echo "$(BOLD)╔══════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BOLD)║  $(BLUE)Ferrite$(NC)$(BOLD) - High-Performance Key-Value Store                 ║$(NC)"
	@echo "$(BOLD)╚══════════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(BLUE)The speed of memory, the capacity of disk, the economics of cloud.$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "$(BOLD)Usage:$(NC)\n  make $(CYAN)<target>$(NC)\n\n"} \
		/^##@/ { printf "\n$(BOLD)%s$(NC)\n", substr($$0, 5) } \
		/^[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-18s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)$(BOLD)Examples:$(NC)"
	@echo "  $(CYAN)make setup$(NC)          Set up development environment (first time)"
	@echo "  $(CYAN)make quickstart$(NC)     Build and start the server"
	@echo "  $(CYAN)make dev$(NC)            Start development with auto-reload"
	@echo "  $(CYAN)make check$(NC)          Run all quality checks"
	@echo "  $(CYAN)make test$(NC)           Run test suite"
	@echo "  $(CYAN)make docker-build$(NC)   Build Docker image"
	@echo ""
	@echo "$(YELLOW)$(BOLD)Environment Variables:$(NC)"
	@echo "  $(CYAN)RUST_LOG$(NC)=<level>    Set log level (trace, debug, info, warn, error)"
	@echo ""
	@echo "$(BOLD)For more information, see CLAUDE.md$(NC)"
	@echo ""
