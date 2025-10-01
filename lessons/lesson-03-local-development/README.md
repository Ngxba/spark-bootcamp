# Lesson 3: Local Development Setup

## 🎯 Learning Objectives

By the end of this lesson, you will be able to:

- **Organize production-ready Spark projects** with modular, maintainable structure
- **Implement professional development workflows** for Spark applications
- **Configure environment-specific settings** for dev/staging/production deployments
- **Set up automated code quality tools** and CI/CD pipeline foundations
- **Create reusable project templates** for different types of Spark applications

## 📋 Prerequisites

- ✅ **Lesson 1**: Spark Fundamentals completed
- ✅ **Lesson 2**: DataFrames & Spark SQL completed
- 💻 **Basic Git knowledge**: Understanding of version control concepts
- 🐍 **Python basics**: Familiarity with modules, packages, and virtual environments

## ⏱️ Duration

**Estimated Time**: 2-3 hours
- **Reading & Setup**: 30 minutes
- **Module Walkthroughs**: 90 minutes
- **Hands-on Exercises**: 60-90 minutes

## 🏗️ What You'll Build

In this lesson, you'll create a **professional Spark development environment** including:

1. **Production-ready project structures** for different use cases
2. **Automated testing and code quality pipelines**
3. **Multi-environment configuration management**
4. **Reusable project templates** for future Spark applications

---

## 📚 Module 1: Project Structure Best Practices

### 🎯 Learning Goals
- Understand **modular Spark application architecture**
- Learn **separation of concerns** for data processing projects
- Create **scalable folder structures** for real-world projects
- Implement **reusable components** and utility modules

### 📂 Recommended Project Structure

```
my-spark-project/
├── README.md                   # Project documentation
├── pyproject.toml             # Dependencies and project configuration
├── Makefile                   # Development commands
├── .env.example               # Environment variables template
├── .gitignore                 # Git ignore patterns
├── .pre-commit-config.yaml    # Pre-commit hooks configuration
│
├── src/                       # Source code (production)
│   ├── __init__.py
│   ├── config/                # Configuration management
│   │   ├── __init__.py
│   │   ├── settings.py        # Application settings
│   │   └── environments/      # Environment-specific configs
│   │       ├── dev.yaml
│   │       ├── staging.yaml
│   │       └── prod.yaml
│   │
│   ├── jobs/                  # Spark job definitions
│   │   ├── __init__.py
│   │   ├── base_job.py        # Abstract base job class
│   │   ├── etl_job.py         # ETL job implementation
│   │   └── streaming_job.py   # Streaming job implementation
│   │
│   ├── transformations/       # Data transformation functions
│   │   ├── __init__.py
│   │   ├── cleaning.py        # Data cleaning functions
│   │   ├── aggregations.py    # Aggregation functions
│   │   └── validations.py     # Data validation functions
│   │
│   ├── utils/                 # Utility functions
│   │   ├── __init__.py
│   │   ├── spark_utils.py     # Spark session and utilities
│   │   ├── io_utils.py        # Input/output helpers
│   │   └── logging_utils.py   # Logging configuration
│   │
│   └── schemas/               # Data schemas
│       ├── __init__.py
│       ├── input_schemas.py   # Input data schemas
│       └── output_schemas.py  # Output data schemas
│
├── tests/                     # Test suite
│   ├── __init__.py
│   ├── conftest.py           # Pytest configuration and fixtures
│   ├── unit/                 # Unit tests
│   │   ├── test_transformations.py
│   │   └── test_utils.py
│   ├── integration/          # Integration tests
│   │   └── test_jobs.py
│   └── data/                 # Test data
│       ├── input/
│       └── expected/
│
├── data/                     # Local data directory
│   ├── raw/                  # Raw input data
│   ├── processed/            # Processed data
│   └── output/               # Output data
│
├── docs/                     # Documentation
│   ├── architecture.md
│   ├── deployment.md
│   └── api/
│
├── scripts/                  # Utility scripts
│   ├── setup.sh             # Environment setup
│   ├── run_job.py           # Job runner script
│   └── deploy.sh            # Deployment script
│
└── docker/                  # Docker configurations
    ├── Dockerfile
    ├── docker-compose.yml
    └── spark/
        └── spark-defaults.conf
```

### 🔧 Key Design Principles

#### 1. **Separation of Concerns**
```python
# ❌ BAD: Everything in one file
def process_data():
    # Spark session creation
    # Data loading
    # Business logic
    # Data validation
    # Output writing
    pass

# ✅ GOOD: Separated responsibilities
from src.utils.spark_utils import get_spark_session
from src.transformations.cleaning import clean_customer_data
from src.schemas.input_schemas import CUSTOMER_SCHEMA

def process_customer_data(input_path: str, output_path: str):
    spark = get_spark_session("customer-processing")

    raw_data = spark.read.schema(CUSTOMER_SCHEMA).parquet(input_path)
    cleaned_data = clean_customer_data(raw_data)

    cleaned_data.write.mode("overwrite").parquet(output_path)
```

#### 2. **Configuration-Driven Design**
```python
# src/config/settings.py
from pydantic import BaseSettings
from typing import Dict, Any

class SparkConfig(BaseSettings):
    app_name: str = "my-spark-app"
    master: str = "local[*]"
    sql_shuffle_partitions: int = 200
    serializer: str = "org.apache.spark.serializer.KryoSerializer"

    class Config:
        env_prefix = "SPARK_"

class AppConfig(BaseSettings):
    environment: str = "dev"
    log_level: str = "INFO"
    input_path: str = "data/raw"
    output_path: str = "data/processed"

    spark: SparkConfig = SparkConfig()

    class Config:
        env_file = ".env"
```

#### 3. **Abstract Base Classes**
```python
# src/jobs/base_job.py
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from src.config.settings import AppConfig

class BaseSparkJob(ABC):
    def __init__(self, config: AppConfig):
        self.config = config
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        return (SparkSession.builder
                .appName(self.config.spark.app_name)
                .master(self.config.spark.master)
                .config("spark.sql.shuffle.partitions",
                       self.config.spark.sql_shuffle_partitions)
                .getOrCreate())

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source"""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the data"""
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load data to destination"""
        pass

    def run(self) -> None:
        """Execute the ETL pipeline"""
        try:
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)
        finally:
            self.spark.stop()
```

---

## 🔧 Module 2: Development Workflow

### 🎯 Learning Goals
- Master **local Spark debugging techniques**
- Implement **testing strategies** for Spark applications
- Establish **code quality workflows**
- Create **reproducible development environments**

### 🐛 Local Debugging Strategies

#### 1. **DataFrame Debugging Techniques**
```python
# src/utils/debug_utils.py
from pyspark.sql import DataFrame
from typing import Optional

def debug_dataframe(df: DataFrame,
                   name: str = "DataFrame",
                   show_rows: int = 20,
                   show_schema: bool = True,
                   show_count: bool = True) -> DataFrame:
    """Debug utility for DataFrames"""

    print(f"\n{'='*50}")
    print(f"DEBUG: {name}")
    print(f"{'='*50}")

    if show_schema:
        print(f"\n📋 Schema:")
        df.printSchema()

    if show_count:
        count = df.count()
        print(f"\n📊 Count: {count:,} rows")

    print(f"\n🔍 Sample Data (first {show_rows} rows):")
    df.show(show_rows, truncate=False)

    return df

# Usage in transformations
def clean_customer_data(df: DataFrame) -> DataFrame:
    # Debug input
    df = debug_dataframe(df, "Raw Customer Data")

    # Apply transformations
    cleaned_df = (df
                  .filter(col("age").between(18, 100))
                  .filter(col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$'))
                  .dropDuplicates(["customer_id"]))

    # Debug output
    return debug_dataframe(cleaned_df, "Cleaned Customer Data")
```

#### 2. **Performance Profiling**
```python
# src/utils/profiling_utils.py
import time
from functools import wraps
from typing import Callable, Any

def profile_spark_operation(operation_name: str):
    """Decorator to profile Spark operations"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()

            print(f"\n🚀 Starting: {operation_name}")
            result = func(*args, **kwargs)

            # Force action if result is DataFrame
            if hasattr(result, 'count'):
                count = result.count()
                execution_time = time.time() - start_time
                print(f"✅ Completed: {operation_name}")
                print(f"⏱️  Execution time: {execution_time:.2f} seconds")
                print(f"📊 Result count: {count:,} rows")
            else:
                execution_time = time.time() - start_time
                print(f"✅ Completed: {operation_name}")
                print(f"⏱️  Execution time: {execution_time:.2f} seconds")

            return result
        return wrapper
    return decorator

# Usage
@profile_spark_operation("Customer Data Cleaning")
def clean_customer_data(df: DataFrame) -> DataFrame:
    return (df
            .filter(col("age").between(18, 100))
            .dropDuplicates(["customer_id"]))
```

#### 3. **Testing Strategies**

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
             .appName("test-spark-app")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())

    yield spark
    spark.stop()

@pytest.fixture
def sample_customer_data(spark_session):
    """Create sample customer data for testing"""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
    ])

    data = [
        ("C001", "Alice Johnson", 28, "alice@email.com"),
        ("C002", "Bob Smith", 35, "bob@email.com"),
        ("C003", "Charlie Brown", 22, "charlie@email.com"),
        ("C001", "Alice Johnson", 28, "alice@email.com"),  # Duplicate
        ("C004", "David Wilson", 150, "invalid-email"),    # Invalid data
    ]

    return spark_session.createDataFrame(data, schema)
```

```python
# tests/unit/test_transformations.py
import pytest
from pyspark.sql.functions import col
from src.transformations.cleaning import clean_customer_data

def test_clean_customer_data_removes_duplicates(spark_session, sample_customer_data):
    """Test that duplicate customers are removed"""
    result = clean_customer_data(sample_customer_data)

    # Should have 4 unique customers (C001 duplicate removed)
    assert result.count() == 4

    # Should have unique customer_ids
    unique_ids = result.select("customer_id").distinct().count()
    assert unique_ids == 4

def test_clean_customer_data_filters_invalid_age(spark_session, sample_customer_data):
    """Test that customers with invalid age are filtered out"""
    result = clean_customer_data(sample_customer_data)

    # David Wilson (age 150) should be filtered out
    ages = [row.age for row in result.collect()]
    assert all(18 <= age <= 100 for age in ages)

def test_clean_customer_data_filters_invalid_email(spark_session, sample_customer_data):
    """Test that customers with invalid email are filtered out"""
    result = clean_customer_data(sample_customer_data)

    # Should not contain invalid-email
    emails = [row.email for row in result.collect()]
    assert "invalid-email" not in emails
    assert all("@" in email for email in emails)
```

---

## ⚙️ Module 3: Configuration Management

### 🎯 Learning Goals
- Implement **environment-specific configurations**
- Master **secrets and credentials management**
- Create **flexible, maintainable configuration systems**
- Handle **dependency management** across environments

### 🔐 Environment Configuration Patterns

#### 1. **Hierarchical Configuration with YAML**

```yaml
# config/environments/base.yaml
app:
  name: "spark-etl-pipeline"
  version: "1.0.0"

spark:
  master: "local[*]"
  sql:
    shuffle_partitions: 200
  serializer: "org.apache.spark.serializer.KryoSerializer"

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

data:
  input_format: "parquet"
  output_format: "delta"
  compression: "snappy"
```

```yaml
# config/environments/dev.yaml
app:
  debug: true

spark:
  master: "local[2]"
  sql:
    shuffle_partitions: 4

data:
  input_path: "data/dev/input"
  output_path: "data/dev/output"

database:
  host: "localhost"
  port: 5432
  name: "dev_database"
```

```yaml
# config/environments/prod.yaml
app:
  debug: false

spark:
  master: "yarn"
  sql:
    shuffle_partitions: 1000
  conf:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"

data:
  input_path: "s3a://prod-data-lake/input"
  output_path: "s3a://prod-data-lake/output"

database:
  host: "${DB_HOST}"
  port: "${DB_PORT}"
  name: "${DB_NAME}"
```

#### 2. **Configuration Loading System**

```python
# src/config/config_loader.py
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from omegaconf import OmegaConf, DictConfig

class ConfigLoader:
    """Hierarchical configuration loader"""

    def __init__(self, config_dir: str = "config/environments"):
        self.config_dir = Path(config_dir)

    def load_config(self, environment: str = None) -> DictConfig:
        """Load configuration for specified environment"""
        if environment is None:
            environment = os.getenv("ENVIRONMENT", "dev")

        # Load base configuration
        base_config = self._load_yaml("base.yaml")

        # Load environment-specific configuration
        env_config = self._load_yaml(f"{environment}.yaml")

        # Merge configurations (environment overrides base)
        merged_config = OmegaConf.merge(base_config, env_config)

        # Resolve environment variables
        resolved_config = OmegaConf.to_container(
            merged_config, resolve=True
        )

        return OmegaConf.create(resolved_config)

    def _load_yaml(self, filename: str) -> DictConfig:
        """Load YAML configuration file"""
        file_path = self.config_dir / filename

        if not file_path.exists():
            return OmegaConf.create({})

        with open(file_path, 'r') as file:
            config_dict = yaml.safe_load(file)

        return OmegaConf.create(config_dict)

# Usage
config_loader = ConfigLoader()
config = config_loader.load_config("dev")

print(f"App name: {config.app.name}")
print(f"Spark master: {config.spark.master}")
print(f"Input path: {config.data.input_path}")
```

#### 3. **Secrets Management**

```python
# src/config/secrets.py
import os
from typing import Optional
from pathlib import Path

class SecretsManager:
    """Secure secrets management"""

    @staticmethod
    def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from environment or file"""
        # Try environment variable first
        value = os.getenv(key)
        if value:
            return value

        # Try reading from secrets file
        secrets_file = Path(f"/run/secrets/{key.lower()}")
        if secrets_file.exists():
            return secrets_file.read_text().strip()

        return default

    @staticmethod
    def get_database_url() -> str:
        """Get database connection URL"""
        host = SecretsManager.get_secret("DB_HOST", "localhost")
        port = SecretsManager.get_secret("DB_PORT", "5432")
        username = SecretsManager.get_secret("DB_USERNAME")
        password = SecretsManager.get_secret("DB_PASSWORD")
        database = SecretsManager.get_secret("DB_NAME")

        if not all([username, password, database]):
            raise ValueError("Missing required database credentials")

        return f"postgresql://{username}:{password}@{host}:{port}/{database}"

# .env.example file
"""
# Environment Configuration
ENVIRONMENT=dev

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_NAME=your_database

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_SQL_SHUFFLE_PARTITIONS=200

# AWS Configuration (if using S3)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
"""
```

---

## 🔗 Module 4: Development Tools Integration

### 🎯 Learning Goals
- Establish **Git workflow** for Spark projects
- Implement **automated code quality checks**
- Set up **CI/CD pipeline foundations**
- Create **Docker development environments**

### 🌿 Git Workflow for Data Projects

#### 1. **Git Repository Structure**

```gitignore
# .gitignore for Spark projects

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Jupyter Notebook
.ipynb_checkpoints
*.ipynb

# Environment
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Spark
spark-warehouse/
metastore_db/
derby.log
*.log

# Data files (be selective!)
data/raw/*
data/processed/*
!data/raw/.gitkeep
!data/processed/.gitkeep
!data/sample/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# Docker
.dockerignore
```

#### 2. **Pre-commit Hooks Configuration**

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
        args: ['--maxkb=1000']
      - id: check-merge-conflict
      - id: debug-statements

  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3
        args: [--line-length=100]

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: [--profile=black, --line-length=100]

  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=100, --extend-ignore=E203,W503]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.5.1
    hooks:
      - id: mypy
        additional_dependencies: [types-PyYAML, pydantic]

  - repo: https://github.com/PyCQA/bandit
    rev: 1.7.5
    hooks:
      - id: bandit
        args: [-r, src/, -f, json, -o, bandit-report.json]
        files: ^src/

  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: pytest
        language: python
        pass_filenames: false
        always_run: true
        args: [tests/, --tb=short]
```

#### 3. **Branching Strategy**

```bash
# Feature development workflow
git checkout main
git pull origin main
git checkout -b feature/customer-data-pipeline

# Make changes, commit frequently
git add .
git commit -m "feat: add customer data validation logic"

# Keep feature branch up to date
git checkout main
git pull origin main
git checkout feature/customer-data-pipeline
git rebase main

# Push feature branch
git push origin feature/customer-data-pipeline

# Create pull request (via GitHub/GitLab UI)
# After review and approval, merge to main
```

### 🚀 CI/CD Pipeline Foundation

#### 1. **GitHub Actions Workflow**

```yaml
# .github/workflows/ci.yml
name: CI Pipeline

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.9, 3.10, 3.11]

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install uv
      run: |
        curl -LsSf https://astral.sh/uv/install.sh | sh
        echo "$HOME/.cargo/bin" >> $GITHUB_PATH

    - name: Install dependencies
      run: |
        uv sync --extra dev

    - name: Run linting
      run: |
        uv run flake8 src/ tests/
        uv run black --check src/ tests/
        uv run isort --check-only src/ tests/

    - name: Run type checking
      run: |
        uv run mypy src/

    - name: Run security checks
      run: |
        uv run bandit -r src/

    - name: Run tests
      run: |
        uv run pytest tests/ --cov=src --cov-report=xml

    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml

  docker-build:
    runs-on: ubuntu-latest
    needs: test

    steps:
    - uses: actions/checkout@v3

    - name: Build Docker image
      run: |
        docker build -t spark-app:${{ github.sha }} .

    - name: Test Docker image
      run: |
        docker run --rm spark-app:${{ github.sha }} python -c "import pyspark; print('Spark OK')"
```

### 🐳 Docker Development Environment

#### 1. **Dockerfile for Development**

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --extra dev

# Copy source code
COPY . .

# Set Python path
ENV PYTHONPATH=/app/src

# Default command
CMD ["uv", "run", "python", "-m", "src.jobs.etl_job"]
```

#### 2. **Docker Compose for Development**

```yaml
# docker-compose.yml
version: '3.8'

services:
  spark-app:
    build: .
    volumes:
      - .:/app
      - ./data:/app/data
    environment:
      - ENVIRONMENT=dev
      - PYTHONPATH=/app/src
    ports:
      - "4040:4040"  # Spark UI
    depends_on:
      - postgres
      - minio

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: spark_dev
      POSTGRES_USER: spark_user
      POSTGRES_PASSWORD: spark_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  minio:
    image: minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data

  jupyter:
    build: .
    command: uv run jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
    ports:
      - "8888:8888"
    volumes:
      - .:/app
    environment:
      - PYTHONPATH=/app/src

volumes:
  postgres_data:
  minio_data:
```

---

## 🚀 Getting Started

### 1. **Environment Setup**

```bash
# Clone the lesson repository
cd lesson-03-local-development

# Set up virtual environment with uv
uv venv --python 3.11
source .venv/bin/activate

# Install dependencies
make install-dev

# Set up pre-commit hooks
pre-commit install

# Verify setup
make verify-setup
```

### 2. **Quick Start Commands**

```bash
# Development workflow
make help                    # Show all available commands
make setup                   # Complete development setup
make test                    # Run all tests
make lint                    # Run code quality checks
make format                  # Format code
make run-exercises          # Run all exercises

# Project creation
make create-etl-project     # Create ETL project from template
make create-streaming-project # Create streaming project from template
make create-analysis-project  # Create analysis project from template
```

### 3. **Validation**

```bash
# Verify your learning
make validate-structure     # Check project structure understanding
make validate-workflow      # Check development workflow setup
make validate-config        # Check configuration management
make validate-tools         # Check tools integration
```

---

## 📝 Exercises

Complete these hands-on exercises to master local development setup:

### 🏗️ **Exercise 1: Production Project Template**
Create a comprehensive project structure for a customer analytics ETL pipeline.

### 🔧 **Exercise 2: Modular Refactoring**
Transform a monolithic Spark script into a modular, maintainable application.

### ⚙️ **Exercise 3: Multi-Environment Setup**
Implement configuration management for dev/staging/production environments.

### 🧪 **Exercise 4: Testing Framework**
Build a complete testing infrastructure with unit and integration tests.

### 🔄 **Exercise 5: Development Pipeline**
Set up an end-to-end development workflow with automated quality checks.

---

## 🎓 Learning Outcomes

After completing this lesson, you will have:

✅ **Mastered professional project structure** for Spark applications
✅ **Implemented automated development workflows** with testing and quality checks
✅ **Created flexible configuration management** for multiple environments
✅ **Set up modern development tools integration** with Git, Docker, and CI/CD
✅ **Built reusable project templates** for future Spark development

## 🔗 What's Next?

- **Lesson 4**: File Formats Deep Dive - Optimize data storage and retrieval
- **Lesson 5**: Advanced Data Transformations - Master complex data processing
- **Lesson 6**: Performance Optimization - Scale your applications

---

## 📚 Additional Resources

- [Apache Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
- [Python Project Structure Best Practices](https://docs.python-guide.org/writing/structure/)
- [Pre-commit Hooks Documentation](https://pre-commit.com/)
- [Docker for Data Science](https://www.docker.com/resources/what-container)
- [Git Workflow Strategies](https://www.atlassian.com/git/tutorials/comparing-workflows)

## 🆘 Troubleshooting

**Common Issues and Solutions:**

### Issue: Import errors in tests
```bash
# Solution: Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
# Or use pytest.ini configuration
```

### Issue: Spark session conflicts in tests
```python
# Solution: Use session-scoped fixtures
@pytest.fixture(scope="session")
def spark_session():
    # Implementation
    pass
```

### Issue: Docker container can't access local files
```yaml
# Solution: Proper volume mounting in docker-compose.yml
volumes:
  - .:/app
  - ./data:/app/data
```

---

*Happy coding! 🚀*