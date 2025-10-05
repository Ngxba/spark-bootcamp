# My Spark Project

A production-ready Apache Spark application demonstrating industry best practices for local development setup, modular architecture, and data processing workflows.

## 🎯 Project Overview

This project showcases modern Spark development practices including:

- **Modular Architecture**: Clean separation of concerns with dedicated modules for jobs, transformations, utilities, and configuration
- **Data Processing Pipeline**: Complete ETL workflow with data cleaning, transformation, and analytics
- **Testing Framework**: Comprehensive unit and integration tests
- **Configuration Management**: Environment-specific configuration with YAML and environment variables
- **Industry Standards**: Following Python packaging conventions and Spark optimization patterns

## 📁 Project Structure

```
my-spark-project/
├── README.md                 # Project documentation
├── pyproject.toml           # Project configuration and dependencies
├── Makefile                 # Development workflow automation
├── .env.example             # Environment variables template
├── .gitignore              # Git ignore patterns
├── src/                    # Source code
│   ├── config/             # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py     # Application settings and config loading
│   ├── jobs/               # Spark job implementations
│   │   ├── __init__.py
│   │   ├── base_job.py     # Abstract base job class
│   │   └── etl_job.py      # ETL pipeline implementation
│   ├── transformations/    # Data transformation modules
│   │   ├── __init__.py
│   │   ├── cleaning.py     # Data cleaning utilities
│   │   └── aggregations.py # Data aggregation functions
│   ├── utils/              # Utility modules
│   │   ├── __init__.py
│   │   ├── spark_utils.py  # Spark optimization utilities
│   │   └── file_utils.py   # File operation utilities
│   └── schemas/            # Data schema definitions
│       ├── __init__.py
│       └── sales_schema.py # Schema definitions for sales data
├── tests/                  # Test suite
│   ├── __init__.py
│   ├── conftest.py         # Test configuration and fixtures
│   ├── unit/               # Unit tests
│   │   ├── test_data_cleaning.py
│   │   └── test_data_aggregations.py
│   └── integration/        # Integration tests
│       └── test_etl_job.py
├── data/                   # Data directory
│   ├── raw/                # Raw input data
│   ├── processed/          # Processed data
│   └── output/             # Output data
└── scripts/                # Utility scripts
    └── run_etl.py          # ETL job runner script
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Java 8 or 11 (for Spark)
- uv package manager

### Installation

1. **Clone and setup the project:**
   ```bash
   # Navigate to the project directory
   cd my-spark-project

   # Create virtual environment and install dependencies
   make setup
   make install
   ```

2. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate
   ```

3. **Verify the setup:**
   ```bash
   make verify-setup
   ```

## 🔧 Development Workflow

### Available Make Commands

```bash
# Environment Management
make setup                  # Create virtual environment
make install               # Install dependencies
make activate              # Show activation command
make clean                 # Clean generated files

# Development
make test                  # Run tests
make run-etl              # Run ETL pipeline
make shell                # Start Python shell with environment

# Quality Assurance
make lint                 # Run code linting (if configured)
make format               # Format code (if configured)

# Verification
make verify-setup         # Verify complete setup
make verify-spark         # Verify Spark installation
```

### Running the ETL Pipeline

1. **Prepare sample data:**
   ```bash
   # The project includes sample data in the main lesson directory
   # Copy it to the project data directory
   cp ../data/*.csv data/
   ```

2. **Run the ETL job:**
   ```bash
   python scripts/run_etl.py
   ```

3. **Check the output:**
   ```bash
   ls -la data/output/
   ```

## 📊 Data Processing Pipeline

The ETL pipeline processes sales, customer, and product data through several stages:

### 1. Data Ingestion
- Reads CSV files with schema validation
- Supports multiple input formats
- Configurable input paths

### 2. Data Cleaning
- **Customer Data**: Standardizes gender values, validates age ranges, cleans text fields
- **Product Data**: Validates prices, standardizes categories, cleans product names
- **Sales Data**: Validates quantities and prices, ensures data consistency

### 3. Data Transformation
- **Enrichment**: Joins sales data with customer and product information
- **Aggregations**: Creates customer analytics, product performance metrics, time-based analysis
- **Segmentation**: Implements RFM analysis for customer segmentation

### 4. Data Quality Checks
- Monitors null values and data completeness
- Validates business rules and constraints
- Generates data quality reports

### 5. Output Generation
- Saves results in multiple formats (Parquet, CSV)
- Optimized partitioning for performance
- Comprehensive analytics datasets

## 🧪 Testing

The project includes comprehensive testing:

### Running Tests

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/unit/          # Unit tests only
pytest tests/integration/   # Integration tests only

# Run with coverage
pytest --cov=src
```

### Test Structure

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test complete workflows end-to-end
- **Fixtures**: Shared test data and Spark session setup

## ⚙️ Configuration

### Environment Variables

Create a `.env` file from the template:

```bash
cp .env.example .env
```

Key configuration options:

```bash
# Spark Configuration
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g

# Data Paths
DATA_INPUT_PATH=data
DATA_OUTPUT_PATH=data/output

# Application Settings
ENVIRONMENT=development
LOG_LEVEL=INFO
DEBUG_MODE=true
```

### Configuration Files

The project supports environment-specific configuration:

- `config/base.yaml`: Base configuration
- `config/development.yaml`: Development environment
- `config/production.yaml`: Production environment

## 🏗️ Architecture Patterns

### Job Architecture

All Spark jobs inherit from `BaseSparkJob` which provides:

- Spark session management
- Configuration handling
- Logging setup
- Error handling
- Resource cleanup

### Transformation Modules

Reusable transformation functions organized by purpose:

- **DataCleaner**: Data validation and cleaning
- **DataAggregator**: Business analytics and aggregations
- **SchemaUtils**: Schema validation and management

### Utility Classes

Helper classes for common operations:

- **SparkUtils**: Spark optimization and performance utilities
- **FileUtils**: File system operations and path management
- **ConfigManager**: Configuration loading and validation

## 📈 Performance Optimization

The project includes several performance optimizations:

### Spark Configuration

- Adaptive Query Execution enabled
- Optimized memory settings
- Arrow-based operations for Pandas integration
- Dynamic partition coalescing

### Data Processing

- Broadcast joins for small datasets
- Optimal partitioning strategies
- Caching for reused DataFrames
- Column pruning and predicate pushdown

### Development Best Practices

- Schema enforcement for type safety
- Lazy evaluation optimization
- Resource management and cleanup
- Memory-efficient operations

## 🔍 Monitoring and Debugging

### Spark UI

Access the Spark UI at `http://localhost:4040` when running jobs to monitor:

- Job execution progress
- Stage and task details
- Storage and memory usage
- SQL query plans

### Logging

Comprehensive logging throughout the application:

- Configurable log levels
- Structured logging with timestamps
- Error tracking and debugging information
- Performance metrics

## 🤝 Contributing

1. Follow the existing code structure and patterns
2. Add tests for new functionality
3. Update documentation for changes
4. Run the test suite before submitting changes
5. Follow Python PEP 8 style guidelines

## 📚 Learning Objectives

This project demonstrates:

1. **Production-Ready Architecture**: Modular design patterns for maintainable Spark applications
2. **Testing Strategies**: Unit and integration testing for data pipelines
3. **Configuration Management**: Environment-specific settings and deployment patterns
4. **Performance Optimization**: Spark tuning and optimization techniques
5. **Data Quality**: Validation, cleaning, and monitoring practices
6. **Development Workflow**: Automation, testing, and deployment practices

## 🔗 Related Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Python Packaging User Guide](https://packaging.python.org/)

## 📄 License

This project is part of the Spark Bootcamp educational materials.

---

**Made with ❤️ for learning Apache Spark development best practices**