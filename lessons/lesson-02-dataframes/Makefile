# Makefile for Spark DataFrame Lesson 2
.PHONY: help install install-dev test lint format check-format type-check run-exercises clean setup

# Default target
help:
	@echo "Available commands:"
	@echo "  setup         - Complete setup (install + pre-commit)"
	@echo "  install       - Install dependencies with uv"
	@echo "  install-dev   - Install with dev dependencies"
	@echo "  test          - Run tests"
	@echo "  lint          - Run linting (flake8)"
	@echo "  format        - Format code (black + isort)"
	@echo "  check-format  - Check if code is formatted"
	@echo "  type-check    - Run type checking (mypy)"
	@echo "  run-exercises - Run all exercises"
	@echo "  clean         - Clean up cache files"

# Setup everything
setup: install-dev
	@echo "Setting up pre-commit hooks..."
	uv run pre-commit install
	@echo "✅ Setup complete!"

# Install dependencies
install:
	@echo "Installing dependencies with uv..."
	uv sync

# Install with dev dependencies
install-dev:
	@echo "Installing with dev dependencies..."
	uv sync --extra dev --extra visualization --extra performance

# Run tests
test:
	@echo "Running tests..."
	uv run pytest

# Run linting
lint:
	@echo "Running flake8..."
	uv run flake8 .

# Format code
format:
	@echo "Formatting code with black..."
	uv run black .
	@echo "Sorting imports with isort..."
	uv run isort .

# Check if code is formatted
check-format:
	@echo "Checking code formatting..."
	uv run black --check .
	uv run isort --check-only .

# Type checking
type-check:
	@echo "Running type checks..."
	uv run mypy .

# Run all exercises
run-exercises:
	@echo "Running Exercise 1..."
	uv run python exercises/exercise_1.py
	@echo "Running Exercise 2..."
	uv run python exercises/exercise_2.py
	@echo "Running Exercise 3..."
	uv run python exercises/exercise_3.py
	@echo "Running Exercise 4..."
	uv run python exercises/exercise_4.py
	@echo "Running Exercise 5..."
	uv run python exercises/exercise_5.py
	@echo "Running Exercise 6..."
	uv run python exercises/exercise_6.py
	@echo "Running Exercise 7..."
	uv run python exercises/exercise_7.py

# Run solutions
run-solutions:
	@echo "Running all solutions..."
	uv run python solutions/exercise_1_solution.py
	uv run python solutions/exercise_2_solution.py
	uv run python solutions/exercise_3_solution.py
	uv run python solutions/exercise_4_solution.py
	uv run python solutions/exercise_5_solution.py
	uv run python solutions/exercise_6_solution.py
	uv run python solutions/exercise_7_solution.py

# Start Jupyter notebook
notebook:
	@echo "Starting Jupyter notebook..."
	uv run jupyter notebook notebook.ipynb

# Clean up
clean:
	@echo "Cleaning up cache files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -delete
	find . -type d -name ".mypy_cache" -delete
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/

# Development workflow
dev-check: format lint type-check test
	@echo "✅ All development checks passed!"