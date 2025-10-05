# Makefile for My Spark Project
# Provides convenient commands for development workflow

.PHONY: help setup install install-dev install-jupyter clean test test-unit test-integration lint format type-check run-etl shell activate env-info verify-setup verify-spark validate qa

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)My Spark Project - Available Commands$(NC)"
	@echo "=========================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## Set up the development environment
	@echo "$(YELLOW)Setting up development environment...$(NC)"
	@if ! command -v uv > /dev/null; then \
		echo "$(RED)Error: uv is not installed. Please install it first.$(NC)"; \
		echo "Run: curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	fi
	uv venv --python 3.8
	@echo "$(GREEN)Virtual environment created!$(NC)"
	@echo "$(YELLOW)Activate it with: $(NC)$(BLUE)source .venv/bin/activate$(NC)"

activate: ## Show virtual environment activation command
	@echo "$(YELLOW)To activate the virtual environment, run:$(NC)"
	@if [ "$$(uname)" = "Darwin" ] || [ "$$(uname)" = "Linux" ]; then \
		echo "$(BLUE)source .venv/bin/activate$(NC)"; \
	else \
		echo "$(BLUE).venv\\Scripts\\activate$(NC) $(YELLOW)(Windows)$(NC)"; \
	fi

install: ## Install production dependencies
	@echo "$(YELLOW)Installing production dependencies...$(NC)"
	uv sync --no-dev
	@echo "$(GREEN)Dependencies installed successfully!$(NC)"

install-dev: ## Install development dependencies
	@echo "$(YELLOW)Installing development dependencies...$(NC)"
	uv sync --extra dev
	@echo "$(GREEN)Development dependencies installed!$(NC)"

install-jupyter: ## Install Jupyter dependencies
	@echo "$(YELLOW)Installing Jupyter dependencies...$(NC)"
	uv sync --extra jupyter
	@echo "$(GREEN)Jupyter dependencies installed!$(NC)"

clean: ## Clean up generated files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/ .tox/
	rm -rf data/output/* data/processed/* 2>/dev/null || true
	@echo "$(GREEN)Cleanup complete!$(NC)"

test: ## Run all tests
	@echo "$(YELLOW)Running all tests...$(NC)"
	uv run pytest
	@echo "$(GREEN)Tests completed!$(NC)"

test-unit: ## Run unit tests only
	@echo "$(YELLOW)Running unit tests...$(NC)"
	uv run pytest tests/unit/
	@echo "$(GREEN)Unit tests completed!$(NC)"

test-integration: ## Run integration tests only
	@echo "$(YELLOW)Running integration tests...$(NC)"
	uv run pytest tests/integration/
	@echo "$(GREEN)Integration tests completed!$(NC)"

test-coverage: ## Run tests with coverage report
	@echo "$(YELLOW)Running tests with coverage...$(NC)"
	uv run pytest --cov=src --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in htmlcov/$(NC)"

lint: ## Run code linting
	@echo "$(YELLOW)Running code linting...$(NC)"
	uv run flake8 src/ tests/ scripts/
	uv run yamllint .
	@echo "$(GREEN)Linting completed!$(NC)"

format: ## Format code with black and isort
	@echo "$(YELLOW)Formatting code...$(NC)"
	uv run black src/ tests/ scripts/
	uv run isort src/ tests/ scripts/
	@echo "$(GREEN)Code formatting completed!$(NC)"

type-check: ## Run type checking with mypy
	@echo "$(YELLOW)Running type checking...$(NC)"
	uv run mypy src/
	@echo "$(GREEN)Type checking completed!$(NC)"

security-check: ## Run security checks
	@echo "$(YELLOW)Running security checks...$(NC)"
	uv run bandit -r src/
	uv run safety check
	@echo "$(GREEN)Security checks completed!$(NC)"

pre-commit-install: ## Install pre-commit hooks
	@echo "$(YELLOW)Installing pre-commit hooks...$(NC)"
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg
	@echo "$(GREEN)Pre-commit hooks installed!$(NC)"

pre-commit-run: ## Run pre-commit hooks on all files
	@echo "$(YELLOW)Running pre-commit hooks on all files...$(NC)"
	uv run pre-commit run --all-files
	@echo "$(GREEN)Pre-commit checks completed!$(NC)"

pre-commit-update: ## Update pre-commit hooks
	@echo "$(YELLOW)Updating pre-commit hooks...$(NC)"
	uv run pre-commit autoupdate
	@echo "$(GREEN)Pre-commit hooks updated!$(NC)"

run-etl: ## Run the ETL pipeline
	@echo "$(YELLOW)Running ETL pipeline...$(NC)"
	uv run python scripts/run_etl.py
	@echo "$(GREEN)ETL pipeline completed!$(NC)"

shell: ## Start a Python shell with the project environment
	@echo "$(YELLOW)Starting Python shell with project environment...$(NC)"
	@echo "$(BLUE)Type 'exit()' to return to your normal shell$(NC)"
	uv run python

env-info: ## Show virtual environment information
	@echo "$(YELLOW)Virtual Environment Information:$(NC)"
	@echo "Location: $(PWD)/.venv"
	@if [ -d ".venv" ]; then \
		echo "$(GREEN)✓ Virtual environment exists$(NC)"; \
		echo "Python executable: $$(uv run which python)"; \
		echo "Python version: $$(uv run python --version)"; \
		echo "uv version: $$(uv --version)"; \
	else \
		echo "$(RED)✗ Virtual environment not found$(NC)"; \
		echo "Run: $(BLUE)make setup$(NC)"; \
	fi

verify-setup: ## Verify complete development setup
	@echo "$(YELLOW)Verifying development setup...$(NC)"
	@echo "Checking Python version:"
	uv run python --version
	@echo "Checking uv version:"
	uv --version
	@echo "Checking installed packages:"
	uv pip list | head -10
	@echo "$(GREEN)Setup verification completed!$(NC)"

verify-spark: ## Verify Spark installation
	@echo "$(YELLOW)Verifying Spark installation...$(NC)"
	uv run python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('Test').getOrCreate(); print(f'✅ Spark {spark.version} is working!'); spark.stop()"
	@echo "$(GREEN)Spark verification completed!$(NC)"

validate: ## Run all validation checks
	@echo "$(YELLOW)Running all validation checks...$(NC)"
	$(MAKE) lint
	$(MAKE) type-check
	$(MAKE) security-check
	$(MAKE) test
	@echo "$(GREEN)All validation checks passed!$(NC)"

qa: ## Run quality assurance checks
	@echo "$(YELLOW)Running quality assurance...$(NC)"
	$(MAKE) format
	$(MAKE) lint
	$(MAKE) type-check
	$(MAKE) security-check
	$(MAKE) test-coverage
	@echo "$(GREEN)Quality assurance completed!$(NC)"

ci: ## Run CI pipeline (format, lint, type-check, security, test)
	@echo "$(BLUE)Running CI Pipeline...$(NC)"
	$(MAKE) format
	$(MAKE) lint
	$(MAKE) type-check
	$(MAKE) security-check
	$(MAKE) test-coverage
	@echo "$(GREEN)CI Pipeline completed successfully!$(NC)"

# Development workflow shortcuts
dev-setup: ## Complete development setup
	@echo "$(BLUE)Setting up complete development environment...$(NC)"
	$(MAKE) setup
	$(MAKE) install-dev
	$(MAKE) pre-commit-install
	$(MAKE) verify-setup
	@echo "$(GREEN)Development environment ready!$(NC)"
	@echo "$(YELLOW)Don't forget to activate: source .venv/bin/activate$(NC)"

quick-start: ## Quick start for new developers
	@echo "$(BLUE)My Spark Project - Quick Start$(NC)"
	@echo "================================"
	$(MAKE) setup
	$(MAKE) install
	$(MAKE) verify-spark
	@echo "$(YELLOW)To start developing:$(NC)"
	@echo "1. $(BLUE)source .venv/bin/activate$(NC)"
	@echo "2. $(BLUE)make run-etl$(NC) (to test the pipeline)"
	@echo "3. $(BLUE)make test$(NC) (to run tests)"

# Default target
all: setup install-dev verify-setup test ## Run complete setup and validation