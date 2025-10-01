# Makefile for Spark Bootcamp Lesson 3 - Local Development Setup
# Provides convenient commands for development workflow and project management

.PHONY: help setup install install-dev clean test test-exercises test-structure test-coverage lint format check-format check-lint type-check create-etl-project create-streaming-project create-analysis-project run-exercise-1 run-exercise-2 run-exercise-3 run-exercise-4 run-exercise-5 run-exercises run-solutions notebook lab verify-spark verify-setup validate-structure validate-workflow validate-config validate-tools check-env quick-start dev-setup docker-build docker-up docker-down

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Spark Bootcamp Lesson 3 - Local Development Setup - Available Commands$(NC)"
	@echo "============================================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-25s$(NC) %s\n", $$1, $$2}'

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
	uv sync --extra dev --extra docker --extra ci-cd --extra templates
	@echo "$(GREEN)Development dependencies installed!$(NC)"

clean: ## Clean up generated files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/ .tox/
	rm -rf templates/*/build templates/*/dist
	@echo "$(GREEN)Cleanup complete!$(NC)"

test: ## Run all tests
	@echo "$(YELLOW)Running tests...$(NC)"
	uv run pytest tests/ -v
	@echo "$(GREEN)Tests completed!$(NC)"

test-exercises: ## Test exercise implementations
	@echo "$(YELLOW)Testing exercise implementations...$(NC)"
	uv run pytest tests/test_exercises.py -v
	@echo "$(GREEN)Exercise tests completed!$(NC)"

test-structure: ## Test project structure validation
	@echo "$(YELLOW)Testing project structure concepts...$(NC)"
	uv run pytest tests/test_project_structure.py -v
	@echo "$(GREEN)Structure tests completed!$(NC)"

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

# Project Template Creation Commands
create-etl-project: ## Create ETL project from template
	@echo "$(YELLOW)Creating ETL project from template...$(NC)"
	@read -p "Enter project name: " name; \
	uv run python scripts/create_project.py --template etl --name "$$name"
	@echo "$(GREEN)ETL project created successfully!$(NC)"

create-streaming-project: ## Create streaming project from template
	@echo "$(YELLOW)Creating streaming project from template...$(NC)"
	@read -p "Enter project name: " name; \
	uv run python scripts/create_project.py --template streaming --name "$$name"
	@echo "$(GREEN)Streaming project created successfully!$(NC)"

create-analysis-project: ## Create analysis project from template
	@echo "$(YELLOW)Creating analysis project from template...$(NC)"
	@read -p "Enter project name: " name; \
	uv run python scripts/create_project.py --template analysis --name "$$name"
	@echo "$(GREEN)Analysis project created successfully!$(NC)"

# Exercise Commands
run-exercise-1: ## Run Exercise 1 - Production Project Template
	@echo "$(YELLOW)Running Exercise 1 - Production Project Template...$(NC)"
	uv run python exercises/exercise_1.py
	@echo "$(GREEN)Exercise 1 completed!$(NC)"

run-exercise-2: ## Run Exercise 2 - Modular Refactoring
	@echo "$(YELLOW)Running Exercise 2 - Modular Refactoring...$(NC)"
	uv run python exercises/exercise_2.py
	@echo "$(GREEN)Exercise 2 completed!$(NC)"

run-exercise-3: ## Run Exercise 3 - Multi-Environment Setup
	@echo "$(YELLOW)Running Exercise 3 - Multi-Environment Setup...$(NC)"
	uv run python exercises/exercise_3.py
	@echo "$(GREEN)Exercise 3 completed!$(NC)"

run-exercise-4: ## Run Exercise 4 - Testing Framework
	@echo "$(YELLOW)Running Exercise 4 - Testing Framework...$(NC)"
	uv run python exercises/exercise_4.py
	@echo "$(GREEN)Exercise 4 completed!$(NC)"

run-exercise-5: ## Run Exercise 5 - Development Pipeline
	@echo "$(YELLOW)Running Exercise 5 - Development Pipeline...$(NC)"
	uv run python exercises/exercise_5.py
	@echo "$(GREEN)Exercise 5 completed!$(NC)"

run-exercises: ## Run all exercises
	@echo "$(YELLOW)Running all development setup exercises...$(NC)"
	$(MAKE) run-exercise-1
	$(MAKE) run-exercise-2
	$(MAKE) run-exercise-3
	$(MAKE) run-exercise-4
	$(MAKE) run-exercise-5
	@echo "$(GREEN)All exercises completed!$(NC)"

run-solutions: ## Run all solution files
	@echo "$(YELLOW)Running solution demonstrations...$(NC)"
	uv run python solutions/exercise_1_solution.py
	uv run python solutions/exercise_2_solution.py
	uv run python solutions/exercise_3_solution.py
	uv run python solutions/exercise_4_solution.py
	uv run python solutions/exercise_5_solution.py
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

verify-setup: ## Verify complete development setup
	@echo "$(YELLOW)Verifying development setup...$(NC)"
	@echo "Checking Python version:"
	uv run python --version
	@echo "Checking uv version:"
	uv --version
	@echo "Checking pre-commit:"
	uv run pre-commit --version || echo "$(YELLOW)Pre-commit not installed, run: uv add pre-commit$(NC)"
	@echo "Checking Spark:"
	$(MAKE) verify-spark
	@echo "$(GREEN)Setup verification completed!$(NC)"

# Validation Commands
validate-structure: ## Validate project structure understanding
	@echo "$(YELLOW)Validating project structure knowledge...$(NC)"
	uv run python scripts/validate_structure.py
	@echo "$(GREEN)Structure validation completed!$(NC)"

validate-workflow: ## Validate development workflow setup
	@echo "$(YELLOW)Validating development workflow...$(NC)"
	uv run python scripts/validate_workflow.py
	@echo "$(GREEN)Workflow validation completed!$(NC)"

validate-config: ## Validate configuration management
	@echo "$(YELLOW)Validating configuration management...$(NC)"
	uv run python scripts/validate_config.py
	@echo "$(GREEN)Configuration validation completed!$(NC)"

validate-tools: ## Validate tools integration
	@echo "$(YELLOW)Validating tools integration...$(NC)"
	uv run python scripts/validate_tools.py
	@echo "$(GREEN)Tools validation completed!$(NC)"

check-env: ## Check environment setup
	@echo "$(YELLOW)Checking environment setup...$(NC)"
	@echo "Python version:"
	uv run python --version
	@echo "uv version:"
	uv --version
	@echo "Installed packages:"
	uv pip list | head -15
	@echo "Git status:"
	git --version
	@echo "Docker status:"
	docker --version || echo "$(YELLOW)Docker not installed$(NC)"
	@echo "$(GREEN)Environment check completed!$(NC)"

# Docker Commands
docker-build: ## Build Docker development environment
	@echo "$(YELLOW)Building Docker development environment...$(NC)"
	docker build -t spark-lesson-03:latest .
	@echo "$(GREEN)Docker build completed!$(NC)"

docker-up: ## Start Docker development environment
	@echo "$(YELLOW)Starting Docker development environment...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)Docker environment started!$(NC)"
	@echo "$(BLUE)Access Jupyter at: http://localhost:8888$(NC)"
	@echo "$(BLUE)Access Spark UI at: http://localhost:4040$(NC)"

docker-down: ## Stop Docker development environment
	@echo "$(YELLOW)Stopping Docker development environment...$(NC)"
	docker-compose down
	@echo "$(GREEN)Docker environment stopped!$(NC)"

# Quality Assurance
qa: ## Run complete quality assurance suite
	@echo "$(YELLOW)Running complete quality assurance...$(NC)"
	$(MAKE) check-format
	$(MAKE) check-lint
	$(MAKE) type-check
	$(MAKE) test
	@echo "$(GREEN)Quality assurance completed!$(NC)"

# Pre-commit Setup
setup-pre-commit: ## Set up pre-commit hooks
	@echo "$(YELLOW)Setting up pre-commit hooks...$(NC)"
	uv run pre-commit install
	uv run pre-commit run --all-files
	@echo "$(GREEN)Pre-commit hooks installed!$(NC)"

quick-start: ## Quick start - setup and run basic validation
	@echo "$(BLUE)Spark Bootcamp Lesson 3 - Local Development Setup - Quick Start$(NC)"
	@echo "================================================================="
	$(MAKE) setup
	@echo "$(YELLOW)Please activate the virtual environment:$(NC)"
	@echo "source .venv/bin/activate"
	@echo "$(YELLOW)Then run: make install-dev verify-setup$(NC)"

dev-setup: ## Full development setup
	@echo "$(BLUE)Setting up complete development environment...$(NC)"
	$(MAKE) setup
	$(MAKE) install-dev
	$(MAKE) setup-pre-commit
	$(MAKE) verify-setup
	@echo "$(GREEN)Development environment ready!$(NC)"
	@echo "$(YELLOW)Don't forget to activate: source .venv/bin/activate$(NC)"

# Learning Validation
validate-learning: ## Validate all learning objectives
	@echo "$(YELLOW)Validating all learning objectives...$(NC)"
	$(MAKE) validate-structure
	$(MAKE) validate-workflow
	$(MAKE) validate-config
	$(MAKE) validate-tools
	@echo "$(GREEN)Learning validation completed!$(NC)"

# Default target
all: setup install-dev verify-setup test ## Run complete setup and validation