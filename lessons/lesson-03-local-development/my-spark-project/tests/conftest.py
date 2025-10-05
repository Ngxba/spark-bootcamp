"""
Test configuration and fixtures for the test suite.
This module provides shared fixtures and test utilities.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)
import tempfile
import shutil
from pathlib import Path


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing.

    Returns:
        SparkSession configured for testing
    """
    spark = (
        SparkSession.builder.appName("TestSparkSession")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_directory():
    """
    Create a temporary directory for test files.

    Returns:
        Path to temporary directory
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_sales_data():
    """
    Create sample sales data for testing.

    Returns:
        List of tuples representing sales data
    """
    return [
        ("TXN001", "CUST001", "PROD001", 2, 25.50, 51.00, "2024-01-15"),
        ("TXN002", "CUST002", "PROD002", 1, 15.75, 15.75, "2024-01-16"),
        ("TXN003", "CUST001", "PROD003", 3, 10.25, 30.75, "2024-01-17"),
        ("TXN004", "CUST003", "PROD001", 1, 25.50, 25.50, "2024-01-18"),
        ("TXN005", "CUST002", "PROD004", 2, 8.99, 17.98, "2024-01-19"),
    ]


@pytest.fixture
def sample_customer_data():
    """
    Create sample customer data for testing.

    Returns:
        List of tuples representing customer data
    """
    return [
        ("CUST001", "John Doe", 35, "Male", "New York", "PREMIUM"),
        ("CUST002", "Jane Smith", 28, "Female", "Los Angeles", "STANDARD"),
        ("CUST003", "Bob Johnson", 42, "Male", "Chicago", "PREMIUM"),
        ("CUST004", "Alice Brown", 31, "Female", "Houston", "STANDARD"),
        ("CUST005", "Charlie Davis", 26, "Male", "Phoenix", "BASIC"),
    ]


@pytest.fixture
def sample_product_data():
    """
    Create sample product data for testing.

    Returns:
        List of tuples representing product data
    """
    return [
        ("PROD001", "Laptop Pro", "Electronics", "TechBrand", 999.99, "Supplier A"),
        ("PROD002", "Wireless Mouse", "Electronics", "TechBrand", 29.99, "Supplier B"),
        ("PROD003", "Coffee Mug", "Home", "HomeGoods", 12.50, "Supplier C"),
        ("PROD004", "Notebook", "Office", "OfficeSupply", 4.99, "Supplier A"),
        ("PROD005", "Desk Chair", "Furniture", "FurnitureCo", 199.99, "Supplier D"),
    ]


@pytest.fixture
def sales_schema():
    """
    Get the sales data schema.

    Returns:
        StructType schema for sales data
    """
    return StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("transaction_date", StringType(), False),
        ]
    )


@pytest.fixture
def customer_schema():
    """
    Get the customer data schema.

    Returns:
        StructType schema for customer data
    """
    return StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("city", StringType(), True),
            StructField("customer_segment", StringType(), True),
        ]
    )


@pytest.fixture
def product_schema():
    """
    Get the product data schema.

    Returns:
        StructType schema for product data
    """
    return StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), False),
            StructField("supplier", StringType(), True),
        ]
    )


@pytest.fixture
def sample_sales_df(spark_session, sample_sales_data, sales_schema):
    """
    Create a sample sales DataFrame for testing.

    Returns:
        DataFrame with sample sales data
    """
    return spark_session.createDataFrame(sample_sales_data, sales_schema)


@pytest.fixture
def sample_customer_df(spark_session, sample_customer_data, customer_schema):
    """
    Create a sample customer DataFrame for testing.

    Returns:
        DataFrame with sample customer data
    """
    return spark_session.createDataFrame(sample_customer_data, customer_schema)


@pytest.fixture
def sample_product_df(spark_session, sample_product_data, product_schema):
    """
    Create a sample product DataFrame for testing.

    Returns:
        DataFrame with sample product data
    """
    return spark_session.createDataFrame(sample_product_data, product_schema)


@pytest.fixture
def test_config():
    """
    Create test configuration.

    Returns:
        Dictionary with test configuration
    """
    return {
        "spark_config": {
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
        },
        "input_paths": {
            "customers": "test_data/customers.csv",
            "products": "test_data/products.csv",
            "sales": "test_data/sales.csv",
        },
        "output_path": "test_output",
    }
