"""
Pytest configuration and shared fixtures for testing.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Generator, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session for testing.

    Yields:
        SparkSession configured for testing
    """
    spark = (SparkSession.builder
             .appName("TestSuite")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.warehouse.dir", str(Path(tempfile.gettempdir()) / "spark-warehouse"))
             .getOrCreate())

    # Reduce logging noise during tests
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    spark.stop()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """
    Create a temporary directory for test files.

    Yields:
        Path to temporary directory
    """
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """
    Sample configuration for testing.

    Returns:
        Dictionary containing test configuration
    """
    return {
        "app": {
            "name": "test-app",
            "version": "1.0.0",
            "environment": "test"
        },
        "spark": {
            "master": "local[2]",
            "app_name": "TestApp",
            "sql": {
                "shuffle": {
                    "partitions": 2
                }
            }
        },
        "data": {
            "input_path": "test/input",
            "output_path": "test/output",
            "temp_path": "test/temp",
            "checkpoint_path": "test/checkpoints"
        },
        "logging": {
            "level": "INFO"
        }
    }


@pytest.fixture
def customer_schema() -> StructType:
    """
    Customer data schema for testing.

    Returns:
        StructType representing customer schema
    """
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("income", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("registration_date", StringType(), True)
    ])


@pytest.fixture
def sample_customer_data():
    """
    Sample customer data for testing.

    Returns:
        List of tuples containing customer data
    """
    return [
        ("C001", "Alice Johnson", 28, "alice@email.com", 75000.0, "New York", "2023-01-15"),
        ("C002", "Bob Smith", 35, "bob@email.com", 65000.0, "Chicago", "2023-02-20"),
        ("C003", "Charlie Brown", 42, "charlie@email.com", 85000.0, "Los Angeles", "2023-01-10"),
        ("C004", "Diana Wilson", 29, "diana@email.com", 70000.0, "Houston", "2023-03-05"),
        ("C005", "Eve Davis", 38, "eve@email.com", 90000.0, "Phoenix", "2023-02-14"),
    ]


@pytest.fixture
def corrupted_customer_data():
    """
    Customer data with quality issues for testing validation.

    Returns:
        List of tuples containing corrupted customer data
    """
    return [
        ("C001", "Alice Johnson", 28, "alice@email.com", 75000.0, "New York", "2023-01-15"),
        ("C002", None, 35, "invalid-email", 65000.0, "Chicago", "2023-02-20"),  # Missing name, invalid email
        ("C003", "Charlie Brown", -5, "charlie@email.com", -1000.0, "", ""),  # Invalid age, negative income
        ("C001", "Alice Duplicate", 28, "alice@email.com", 75000.0, "New York", "2023-01-15"),  # Duplicate ID
        ("C004", "Diana Wilson", None, "diana@email.com", 70000.0, "Houston", "2023-03-05"),  # Missing age
    ]


@pytest.fixture
def product_schema() -> StructType:
    """
    Product data schema for testing.

    Returns:
        StructType representing product schema
    """
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("supplier", StringType(), True)
    ])


@pytest.fixture
def sample_product_data():
    """
    Sample product data for testing.

    Returns:
        List of tuples containing product data
    """
    return [
        ("P001", "Laptop", "Electronics", 999.99, 50, "High-performance laptop", "TechCorp"),
        ("P002", "Smartphone", "Electronics", 599.99, 100, "Latest smartphone model", "MobileTech"),
        ("P003", "Running Shoes", "Sports", 129.99, 75, "Comfortable running shoes", "SportsBrand"),
        ("P004", "Coffee Maker", "Appliances", 89.99, 30, "Automatic coffee maker", "HomeTech"),
        ("P005", "Book", "Books", 19.99, 200, "Best-selling novel", "PublishCorp"),
    ]


@pytest.fixture
def sales_schema() -> StructType:
    """
    Sales data schema for testing.

    Returns:
        StructType representing sales schema
    """
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("transaction_date", StringType(), True)
    ])


@pytest.fixture
def sample_sales_data():
    """
    Sample sales data for testing.

    Returns:
        List of tuples containing sales data
    """
    return [
        ("T001", "C001", "P001", 1, 999.99, "2023-03-01"),
        ("T002", "C002", "P002", 2, 599.99, "2023-03-02"),
        ("T003", "C003", "P003", 1, 129.99, "2023-03-03"),
        ("T004", "C004", "P004", 1, 89.99, "2023-03-04"),
        ("T005", "C005", "P005", 3, 19.99, "2023-03-05"),
        ("T006", "C001", "P002", 1, 599.99, "2023-03-06"),
        ("T007", "C002", "P001", 1, 999.99, "2023-03-07"),
    ]


@pytest.fixture
def mock_config_file(temp_dir: Path) -> Path:
    """
    Create a mock configuration file for testing.

    Args:
        temp_dir: Temporary directory fixture

    Returns:
        Path to mock configuration file
    """
    import yaml

    config = {
        "app": {
            "name": "test-app",
            "version": "1.0.0"
        },
        "spark": {
            "master": "local[2]",
            "app_name": "TestApp"
        },
        "data": {
            "input_path": "test/input",
            "output_path": "test/output"
        }
    }

    config_file = temp_dir / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f)

    return config_file


@pytest.fixture
def mock_spark_session(mocker):
    """
    Mock Spark session for unit testing without Spark dependency.

    Args:
        mocker: pytest-mock fixture

    Returns:
        Mock SparkSession object
    """
    mock_spark = mocker.MagicMock()
    mock_spark.sparkContext.appName = "MockApp"
    mock_spark.sparkContext.master = "local[*]"
    mock_spark.conf.set = mocker.MagicMock()
    mock_spark.stop = mocker.MagicMock()

    return mock_spark


class SparkTestCase:
    """
    Base test case class for Spark-related tests.

    Provides common utilities for testing Spark applications.
    """

    @staticmethod
    def assert_dataframe_equal(df1, df2, check_schema=True):
        """
        Assert two DataFrames are equal.

        Args:
            df1: First DataFrame
            df2: Second DataFrame
            check_schema: Whether to check schema equality
        """
        # Check row counts
        assert df1.count() == df2.count(), f"Row counts differ: {df1.count()} vs {df2.count()}"

        # Check schemas if requested
        if check_schema:
            assert df1.schema == df2.schema, "Schemas differ"

        # Compare data
        df1_sorted = df1.orderBy(*df1.columns)
        df2_sorted = df2.orderBy(*df2.columns)

        rows1 = df1_sorted.collect()
        rows2 = df2_sorted.collect()

        for i, (row1, row2) in enumerate(zip(rows1, rows2)):
            assert row1 == row2, f"Row {i} differs: {row1} vs {row2}"

    @staticmethod
    def assert_column_exists(df, column_name):
        """
        Assert that a column exists in the DataFrame.

        Args:
            df: DataFrame to check
            column_name: Name of column to check for
        """
        assert column_name in df.columns, f"Column '{column_name}' not found in DataFrame"

    @staticmethod
    def assert_no_nulls(df, column_name):
        """
        Assert that a column contains no null values.

        Args:
            df: DataFrame to check
            column_name: Name of column to check for nulls
        """
        null_count = df.filter(df[column_name].isNull()).count()
        assert null_count == 0, f"Column '{column_name}' contains {null_count} null values"

    @staticmethod
    def assert_unique_values(df, column_name):
        """
        Assert that all values in a column are unique.

        Args:
            df: DataFrame to check
            column_name: Name of column to check for uniqueness
        """
        total_count = df.count()
        unique_count = df.select(column_name).distinct().count()
        assert total_count == unique_count, f"Column '{column_name}' contains duplicate values"


@pytest.fixture
def spark_test_case():
    """
    Provide SparkTestCase utilities as a fixture.

    Returns:
        SparkTestCase class with testing utilities
    """
    return SparkTestCase