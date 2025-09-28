# Makefile for Spark Bootcamp Lesson 1
# Provides convenient commands for common development tasks

.PHONY: help setup install clean test lint format check-format check-lint run-exercises notebook

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Spark Bootcamp Lesson 1 - Available Commands$(NC)"
	@echo "=============================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## Set up the development environment
	@echo "$(YELLOW)Setting up development environment...$(NC)"
	@if ! command -v uv > /dev/null; then \
		echo "$(RED)Error: uv is not installed. Please install it first.$(NC)"; \
		echo "Run: curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	fi
	uv venv --python 3.11
	@echo "$(GREEN)Virtual environment created!$(NC)"
	@echo "$(YELLOW)Activate it with: source .venv/bin/activate$(NC)"

install: ## Install dependencies using uv
	@echo "$(YELLOW)Installing dependencies...$(NC)"
	uv sync
	@echo "$(GREEN)Dependencies installed successfully!$(NC)"

install-dev: ## Install development dependencies
	@echo "$(YELLOW)Installing development dependencies...$(NC)"
	uv sync --extra dev
	@echo "$(GREEN)Development dependencies installed!$(NC)"

clean: ## Clean up generated files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/
	@echo "$(GREEN)Cleanup complete!$(NC)"

test: ## Run all tests
	@echo "$(YELLOW)Running tests...$(NC)"
	uv run pytest tests/ -v
	@echo "$(GREEN)Tests completed!$(NC)"

test-exercises: ## Test exercise implementations
	@echo "$(YELLOW)Testing exercise implementations...$(NC)"
	uv run pytest tests/test_exercises.py -v
	@echo "$(GREEN)Exercise tests completed!$(NC)"

test-basics: ## Test basic Spark concepts
	@echo "$(YELLOW)Testing basic Spark concepts...$(NC)"
	uv run pytest tests/test_basics.py -v
	@echo "$(GREEN)Basic tests completed!$(NC)"

test-coverage: ## Run tests with coverage report
	@echo "$(YELLOW)Running tests with coverage...$(NC)"
	uv run pytest --cov=exercises --cov=solutions --cov-report=html --cov-report=term tests/
	@echo "$(GREEN)Coverage report generated in htmlcov/$(NC)"

lint: ## Run linting checks
	@echo "$(YELLOW)Running linting checks...$(NC)"
	uv run flake8 exercises/ solutions/ tests/
	@echo "$(GREEN)Linting completed!$(NC)"

format: ## Format code with black
	@echo "$(YELLOW)Formatting code...$(NC)"
	uv run black exercises/ solutions/ tests/
	uv run isort exercises/ solutions/ tests/
	@echo "$(GREEN)Code formatting completed!$(NC)"

check-format: ## Check if code is properly formatted
	@echo "$(YELLOW)Checking code formatting...$(NC)"
	uv run black --check exercises/ solutions/ tests/
	uv run isort --check-only exercises/ solutions/ tests/
	@echo "$(GREEN)Format check completed!$(NC)"

check-lint: ## Check linting without fixing
	@echo "$(YELLOW)Checking linting...$(NC)"
	uv run flake8 exercises/ solutions/ tests/
	@echo "$(GREEN)Lint check completed!$(NC)"

type-check: ## Run type checking with mypy
	@echo "$(YELLOW)Running type checks...$(NC)"
	uv run mypy exercises/ solutions/
	@echo "$(GREEN)Type checking completed!$(NC)"

run-exercise-1: ## Run Exercise 1
	@echo "$(YELLOW)Running Exercise 1...$(NC)"
	uv run python exercises/exercise_1.py
	@echo "$(GREEN)Exercise 1 completed!$(NC)"

run-exercise-2: ## Run Exercise 2
	@echo "$(YELLOW)Running Exercise 2...$(NC)"
	uv run python exercises/exercise_2.py
	@echo "$(GREEN)Exercise 2 completed!$(NC)"

run-exercise-3: ## Run Exercise 3
	@echo "$(YELLOW)Running Exercise 3...$(NC)"
	uv run python exercises/exercise_3.py
	@echo "$(GREEN)Exercise 3 completed!$(NC)"

run-exercises: ## Run all exercises
	@echo "$(YELLOW)Running all exercises...$(NC)"
	$(MAKE) run-exercise-1
	$(MAKE) run-exercise-2
	$(MAKE) run-exercise-3
	@echo "$(GREEN)All exercises completed!$(NC)"

run-solutions: ## Run all solution files
	@echo "$(YELLOW)Running solution demonstrations...$(NC)"
	uv run python solutions/exercise_1_solution.py
	uv run python solutions/exercise_2_solution.py
	uv run python solutions/exercise_3_solution.py
	@echo "$(GREEN)All solutions demonstrated!$(NC)"

notebook: ## Start Jupyter notebook
	@echo "$(YELLOW)Starting Jupyter notebook...$(NC)"
	uv run jupyter notebook notebook.ipynb
	@echo "$(GREEN)Jupyter notebook started!$(NC)"

lab: ## Start Jupyter lab
	@echo "$(YELLOW)Starting Jupyter lab...$(NC)"
	uv run jupyter lab
	@echo "$(GREEN)Jupyter lab started!$(NC)"

verify-spark: ## Verify Spark installation
	@echo "$(YELLOW)Verifying Spark installation...$(NC)"
	uv run python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('Test').getOrCreate(); print(f'✅ Spark {spark.version} is working!'); spark.stop()"
	@echo "$(GREEN)Spark verification completed!$(NC)"

check-env: ## Check environment setup
	@echo "$(YELLOW)Checking environment setup...$(NC)"
	@echo "Python version:"
	uv run python --version
	@echo "uv version:"
	uv --version
	@echo "Installed packages:"
	uv pip list | head -10
	@echo "$(GREEN)Environment check completed!$(NC)"

validate: ## Run full validation suite
	@echo "$(YELLOW)Running full validation...$(NC)"
	$(MAKE) check-format
	$(MAKE) check-lint
	$(MAKE) type-check
	$(MAKE) test
	@echo "$(GREEN)Full validation completed!$(NC)"

quick-start: ## Quick start - setup and run basic validation
	@echo "$(BLUE)Spark Bootcamp Lesson 1 - Quick Start$(NC)"
	@echo "======================================"
	$(MAKE) setup
	@echo "$(YELLOW)Please activate the virtual environment:$(NC)"
	@echo "source .venv/bin/activate"
	@echo "$(YELLOW)Then run: make install verify-spark$(NC)"

dev-setup: ## Full development setup
	@echo "$(BLUE)Setting up development environment...$(NC)"
	$(MAKE) setup
	$(MAKE) install-dev
	@echo "$(GREEN)Development environment ready!$(NC)"
	@echo "$(YELLOW)Don't forget to activate: source .venv/bin/activate$(NC)"

# Default target
all: setup install verify-spark test ## Run complete setup and validation