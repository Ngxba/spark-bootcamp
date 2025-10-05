# Pre-commit Setup and Code Quality Guide

This document explains the comprehensive pre-commit setup for the My Spark Project, ensuring code quality and consistency across Python, YAML, and XML files.

## 🎯 Overview

Our pre-commit configuration includes:

- **Python**: Code formatting, linting, type checking, security scanning
- **YAML**: Syntax validation and formatting
- **XML**: Syntax validation and formatting
- **General**: File formatting, commit message validation
- **Security**: Dependency vulnerability scanning

## 🚀 Quick Setup

### 1. Install Development Dependencies

```bash
# Setup development environment
make dev-setup

# Or manually:
uv sync --extra dev
make pre-commit-install
```

### 2. Verify Installation

```bash
# Run pre-commit on all files
make pre-commit-run

# Or manually:
uv run pre-commit run --all-files
```

## 🔧 Pre-commit Hooks Configuration

### Python Code Quality

#### **Black (Code Formatting)**
- **Purpose**: Automatic code formatting
- **Configuration**: 100 character line length, Python 3.8+ target
- **Files**: `*.py`

```bash
# Manual run
uv run black src/ tests/ scripts/
```

#### **isort (Import Sorting)**
- **Purpose**: Sorts and organizes imports
- **Configuration**: Black-compatible profile
- **Files**: `*.py`

```bash
# Manual run
uv run isort src/ tests/ scripts/
```

#### **flake8 (Linting)**
- **Purpose**: Code linting and style checking
- **Extensions**: docstrings, import order, bugbear, comprehensions, simplify
- **Configuration**: 100 char line length, complexity limit 10

```bash
# Manual run
uv run flake8 src/ tests/ scripts/
```

#### **mypy (Type Checking)**
- **Purpose**: Static type checking
- **Configuration**: Strict mode, ignore missing imports
- **Files**: `src/*.py` (excludes tests)

```bash
# Manual run
uv run mypy src/
```

#### **bandit (Security Scanning)**
- **Purpose**: Security vulnerability detection
- **Configuration**: Excludes tests, skips false positives
- **Files**: `*.py`

```bash
# Manual run
uv run bandit -r src/
```

#### **safety (Dependency Security)**
- **Purpose**: Check dependencies for known vulnerabilities
- **Configuration**: Default settings

```bash
# Manual run
uv run safety check
```

### YAML Validation

#### **yamllint**
- **Purpose**: YAML syntax and style validation
- **Configuration**: 120 char line length, flexible truthy values
- **Files**: `*.yaml`, `*.yml`

```bash
# Manual run
uv run yamllint .
```

#### **check-yaml (pre-commit-hooks)**
- **Purpose**: Basic YAML syntax validation
- **Files**: `*.yaml`, `*.yml`

### XML Validation

#### **prettier (XML Formatting)**
- **Purpose**: XML formatting and validation
- **Configuration**: 120 char line width, ignore whitespace sensitivity
- **Files**: `*.xml`, `*.xsd`, `*.xsl`

#### **check-xml (pre-commit-hooks)**
- **Purpose**: Basic XML syntax validation
- **Files**: `*.xml`

### General File Checks

#### **File Formatting**
- `trailing-whitespace`: Remove trailing whitespace
- `end-of-file-fixer`: Ensure files end with newline
- `check-merge-conflict`: Detect merge conflict markers
- `check-added-large-files`: Prevent large files (>1MB)

#### **Code Quality**
- `check-ast`: Validate Python syntax
- `check-builtin-literals`: Check builtin literal usage
- `debug-statements`: Detect debug statements (pdb, ipdb)

## 📋 Make Commands

### Pre-commit Management

```bash
# Install pre-commit hooks
make pre-commit-install

# Run hooks on all files
make pre-commit-run

# Update hook versions
make pre-commit-update
```

### Code Quality

```bash
# Format code
make format

# Run linting
make lint

# Type checking
make type-check

# Security checks
make security-check

# Full quality assurance
make qa

# Complete CI pipeline
make ci
```

## 🛠️ Manual Hook Execution

### Run Specific Hooks

```bash
# Python formatting
uv run pre-commit run black --all-files

# Import sorting
uv run pre-commit run isort --all-files

# Python linting
uv run pre-commit run flake8 --all-files

# YAML linting
uv run pre-commit run yamllint --all-files

# Type checking
uv run pre-commit run mypy --all-files

# Security scanning
uv run pre-commit run bandit --all-files
```

### Run on Specific Files

```bash
# Run on staged files only
uv run pre-commit run

# Run on specific files
uv run pre-commit run --files src/jobs/etl_job.py

# Run specific hook on specific files
uv run pre-commit run black --files src/transformations/*.py
```

## 📝 Configuration Files

### `.pre-commit-config.yaml`
Main pre-commit configuration with all hooks and settings.

### `.flake8`
Flake8 linting configuration:
- Line length: 100
- Complexity: 10
- Ignored errors: E203, E501, W503, etc.
- Per-file ignores for `__init__.py`, tests, scripts

### `.yamllint.yaml`
YAML linting configuration:
- Line length: 120
- Indentation: 2 spaces
- Flexible truthy values

### `.markdownlint.yaml`
Markdown linting configuration:
- Line length: 120
- ATX heading style
- Allow inline HTML

### `pyproject.toml`
Contains tool configurations for:
- Black formatting
- isort import sorting
- mypy type checking
- pytest testing
- coverage reporting

## 🔍 Troubleshooting

### Common Issues

#### **Hook Installation Failed**
```bash
# Reinstall pre-commit
uv run pre-commit uninstall
make pre-commit-install
```

#### **Formatting Conflicts**
```bash
# Run formatters in order
make format
uv run pre-commit run --all-files
```

#### **Type Checking Errors**
```bash
# Check specific file
uv run mypy src/path/to/file.py

# Ignore missing imports temporarily
uv run mypy --ignore-missing-imports src/
```

#### **YAML Syntax Errors**
```bash
# Check specific YAML file
uv run yamllint path/to/file.yaml

# Get detailed error info
uv run yamllint -f parsable .
```

### Skipping Hooks

```bash
# Skip all hooks for a commit (not recommended)
git commit --no-verify -m "Emergency commit"

# Skip specific hook
SKIP=flake8 git commit -m "Skip flake8 for this commit"

# Skip multiple hooks
SKIP=flake8,mypy git commit -m "Skip linting for this commit"
```

## 🎯 Best Practices

### Development Workflow

1. **Write code** following project conventions
2. **Run formatters** before committing
3. **Fix linting issues** revealed by pre-commit
4. **Add type hints** to pass mypy checks
5. **Write tests** for new functionality
6. **Commit with descriptive messages**

### Code Quality Guidelines

#### **Python Code**
- Use type hints for all function parameters and returns
- Write docstrings for all public functions and classes
- Keep functions small and focused (max complexity: 10)
- Follow PEP 8 style guidelines (enforced by flake8)

#### **YAML Files**
- Use 2-space indentation
- Keep lines under 120 characters
- Use consistent quoting style
- Validate syntax before committing

#### **XML Files**
- Maintain proper indentation
- Keep lines under 120 characters
- Validate syntax and schema compliance

### Commit Messages

Follow conventional commit format:
```
type(scope): description

feat(etl): add customer segmentation analysis
fix(schemas): correct sales schema date type
docs(readme): update installation instructions
```

## 📊 CI/CD Integration

Pre-commit hooks are also run in CI/CD pipeline:

- **GitHub Actions**: `.github/workflows/ci.yaml`
- **Quality Gates**: All checks must pass before merge
- **Multiple Python Versions**: Tested on 3.8, 3.9, 3.10, 3.11
- **Security Scanning**: Automated vulnerability detection

## 🔗 Resources

- [Pre-commit Documentation](https://pre-commit.com/)
- [Black Code Formatter](https://black.readthedocs.io/)
- [Flake8 Linting](https://flake8.pycqa.org/)
- [MyPy Type Checking](https://mypy.readthedocs.io/)
- [YAML Lint](https://yamllint.readthedocs.io/)
- [Bandit Security](https://bandit.readthedocs.io/)

---

**🎉 Happy coding with consistent, high-quality code!**
