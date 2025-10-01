# Spark ETL Pipeline Template

A production-ready Apache Spark ETL (Extract, Transform, Load) pipeline template with modern development practices, comprehensive testing, and deployment automation.

## 🚀 Features

- **Modular Architecture**: Clean separation of concerns with dedicated modules for extraction, transformation, and loading
- **Configuration Management**: Environment-specific configurations with validation
- **Data Quality**: Built-in data validation and quality checks
- **Error Handling**: Comprehensive error handling with retry mechanisms
- **Monitoring**: Integrated metrics and logging
- **Testing**: Unit, integration, and data quality tests
- **CI/CD Ready**: GitHub Actions workflows and Docker support
- **Cloud Support**: AWS S3, Azure Blob Storage, Google Cloud Storage

## 📁 Project Structure

```
etl-pipeline/
├── etl_pipeline/              # Main package
│   ├── __init__.py
│   ├── cli.py                 # Command-line interface
│   ├── config/                # Configuration management
│   │   ├── __init__.py
│   │   ├── settings.py        # Settings classes
│   │   └── environments/      # Environment configs
│   ├── extractors/            # Data extraction modules
│   │   ├── __init__.py
│   │   ├── base.py           # Base extractor class
│   │   ├── database.py       # Database extractors
│   │   ├── file.py           # File extractors
│   │   └── api.py            # API extractors
│   ├── transformers/          # Data transformation modules
│   │   ├── __init__.py
│   │   ├── base.py           # Base transformer class
│   │   ├── cleaners.py       # Data cleaning
│   │   ├── validators.py     # Data validation
│   │   └── enrichers.py      # Data enrichment
│   ├── loaders/              # Data loading modules
│   │   ├── __init__.py
│   │   ├── base.py           # Base loader class
│   │   ├── database.py       # Database loaders
│   │   ├── file.py           # File loaders
│   │   └── delta.py          # Delta Lake loader
│   ├── jobs/                 # ETL job definitions
│   │   ├── __init__.py
│   │   ├── base.py           # Base job class
│   │   └── customer_etl.py   # Example ETL job
│   ├── utils/                # Utility modules
│   │   ├── __init__.py
│   │   ├── spark.py          # Spark utilities
│   │   ├── logging.py        # Logging setup
│   │   └── metrics.py        # Metrics collection
│   └── schemas/              # Data schemas
│       ├── __init__.py
│       └── customer.py       # Example schema
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── conftest.py          # Test configuration
│   ├── unit/                # Unit tests
│   ├── integration/         # Integration tests
│   └── data/                # Test data
├── config/                  # Configuration files
│   ├── dev.yaml
│   ├── staging.yaml
│   └── prod.yaml
├── docker/                  # Docker configurations
│   ├── Dockerfile
│   └── docker-compose.yml
├── .github/                 # GitHub Actions
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
├── scripts/                 # Utility scripts
│   ├── setup.sh
│   └── deploy.sh
├── docs/                    # Documentation
├── pyproject.toml          # Project configuration
├── Makefile               # Development commands
└── README.md              # This file
```

## 🛠️ Setup

### Prerequisites

- Python 3.9+
- Java 11+ (for Spark)
- Docker (optional)

### Installation

1. **Clone or copy this template**
2. **Install dependencies:**
   ```bash
   # Install with uv (recommended)
   uv sync --extra dev

   # Or with pip
   pip install -e ".[dev]"
   ```
3. **Setup pre-commit hooks:**
   ```bash
   pre-commit install
   ```
4. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

## 🚦 Usage

### Command Line Interface

```bash
# Run ETL pipeline
etl-pipeline run --job customer_etl --env dev

# Validate configuration
etl-pipeline validate --env prod

# List available jobs
etl-pipeline list-jobs

# Run data quality checks
etl-pipeline quality-check --input data/customers.parquet
```

### Programmatic Usage

```python
from etl_pipeline.jobs.customer_etl import CustomerETLJob
from etl_pipeline.config.settings import load_config

# Load configuration
config = load_config("dev")

# Create and run ETL job
job = CustomerETLJob(config)
job.run()
```

## 🔧 Configuration

Configuration is managed through YAML files in the `config/` directory:

```yaml
# config/dev.yaml
app:
  name: "customer-etl"
  environment: "dev"

spark:
  master: "local[2]"
  app_name: "CustomerETL-Dev"

data:
  input_path: "data/raw/customers"
  output_path: "data/processed/customers"
  format: "parquet"

database:
  host: "localhost"
  port: 5432
  database: "analytics_dev"
```

Environment variables can be used for sensitive data:

```yaml
database:
  username: "${DB_USERNAME}"
  password: "${DB_PASSWORD}"
```

## 🧪 Testing

```bash
# Run all tests
make test

# Run specific test types
make test-unit
make test-integration

# Run with coverage
make test-coverage

# Generate test report
make test-report
```

### Test Structure

- **Unit Tests**: Test individual functions and classes
- **Integration Tests**: Test component interactions
- **Data Quality Tests**: Validate data transformations

Example test:

```python
def test_customer_data_cleaning(spark_session):
    # Given
    raw_data = [
        ("C001", "John Doe", 25, "john@email.com"),
        ("C002", None, 16, "invalid-email"),  # Invalid data
    ]

    # When
    result = clean_customer_data(spark_session.createDataFrame(raw_data, schema))

    # Then
    assert result.count() == 1
    assert result.first().name == "John Doe"
```

## 📊 Monitoring

### Metrics

The pipeline collects metrics for:
- Record counts processed
- Processing time per stage
- Data quality scores
- Error rates

### Logging

Structured logging with different levels:
- INFO: General pipeline progress
- WARN: Data quality issues
- ERROR: Pipeline failures

```python
import structlog

logger = structlog.get_logger(__name__)

logger.info("Processing batch", batch_id=batch_id, record_count=count)
logger.error("Validation failed", error=str(e), records_failed=failed_count)
```

## 🐳 Docker

### Build and Run

```bash
# Build image
docker build -f docker/Dockerfile -t etl-pipeline .

# Run pipeline
docker run --rm \
  -v $(pwd)/data:/app/data \
  -e ENVIRONMENT=dev \
  etl-pipeline run --job customer_etl
```

### Docker Compose

```bash
# Start full stack (pipeline + dependencies)
docker-compose -f docker/docker-compose.yml up
```

## ☁️ Cloud Deployment

### AWS

```bash
# Deploy to EMR
aws emr create-cluster \
  --name "ETL-Pipeline" \
  --applications Name=Spark \
  --ec2-attributes KeyName=my-key \
  --instance-type m5.xlarge \
  --instance-count 3

# Submit job
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps Type=Spark,Name="Customer ETL",ActionOnFailure=TERMINATE_CLUSTER,Args=[--class,CustomerETLJob,s3://my-bucket/etl-pipeline.jar]
```

### Azure

```bash
# Deploy to Azure Synapse
az synapse spark job submit \
  --workspace-name my-workspace \
  --spark-pool-name my-pool \
  --main-definition-file customer_etl.py
```

### Google Cloud

```bash
# Deploy to Dataproc
gcloud dataproc jobs submit pyspark \
  --cluster my-cluster \
  --region us-central1 \
  customer_etl.py
```

## 🔄 CI/CD

GitHub Actions workflows are included:

- **CI Pipeline** (`.github/workflows/ci.yml`):
  - Code quality checks (black, isort, flake8)
  - Type checking (mypy)
  - Unit and integration tests
  - Security scanning

- **CD Pipeline** (`.github/workflows/cd.yml`):
  - Build Docker images
  - Deploy to staging/production
  - Run smoke tests

## 📈 Data Pipeline Best Practices

### Error Handling

```python
from etl_pipeline.utils.retry import retry_on_failure

@retry_on_failure(max_attempts=3, backoff_factor=2)
def extract_data(source_config):
    try:
        return extract_from_source(source_config)
    except ConnectionError as e:
        logger.error("Connection failed", error=str(e))
        raise
```

### Data Quality

```python
from etl_pipeline.transformers.validators import DataValidator

validator = DataValidator()
quality_report = validator.validate(
    df,
    rules={
        "completeness": {"min_threshold": 0.95},
        "uniqueness": {"columns": ["customer_id"]},
        "range": {"age": {"min": 0, "max": 120}}
    }
)
```

### Performance Optimization

```python
# Partition data for optimal performance
df.write \
  .partitionBy("year", "month") \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .parquet(output_path)

# Cache frequently accessed data
df.cache()
```

## 🚨 Troubleshooting

### Common Issues

1. **OutOfMemoryError**
   ```bash
   # Increase driver memory
   export SPARK_DRIVER_MEMORY=4g
   ```

2. **Connection timeouts**
   ```python
   # Increase connection timeout
   spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
   ```

3. **Data skew**
   ```python
   # Use salting for skewed joins
   df.withColumn("salt", (rand() * 10).cast("int"))
   ```

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.