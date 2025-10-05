"""
Pytest configuration and shared fixtures for Spark DataFrame tests
=================================================================

This module provides common test configuration and fixtures.
"""

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line(
        "markers", "dataframe: marks tests that require DataFrame operations"
    )
    config.addinivalue_line(
        "markers", "sql: marks tests that require SQL functionality"
    )


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for the entire test session."""
    # Configure Spark for testing
    spark = (
        SparkSession.builder.appName("SparkDataFrameTests")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )

    # Set log level to reduce noise during testing
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    # Clean up
    spark.stop()


@pytest.fixture(scope="function")
def spark(spark_session):
    """Provide a Spark session for individual tests with cleanup."""
    # Clear any cached tables before each test
    spark_session.catalog.clearCache()

    yield spark_session

    # Clean up any temporary views created during the test
    for table in spark_session.catalog.listTables():
        if table.isTemporary:
            spark_session.catalog.dropTempView(table.name)


@pytest.fixture(scope="function")
def temp_dir():
    """Create a temporary directory for tests that need file I/O."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_employee_data():
    """Provide sample employee data for tests."""
    return [
        ("Alice Johnson", 28, "Engineering", 85000),
        ("Bob Chen", 34, "Marketing", 65000),
        ("Charlie Davis", 29, "Engineering", 78000),
        ("Diana Rodriguez", 31, "Sales", 72000),
        ("Eve Wilson", 26, "Engineering", 92000),
    ]


@pytest.fixture
def sample_sales_data():
    """Provide sample sales data for tests."""
    return [
        ("Product A", "Electronics", 1200.00, "North"),
        ("Product B", "Electronics", 800.00, "South"),
        ("Product C", "Furniture", 450.00, "East"),
        ("Product D", "Books", 25.99, "West"),
        ("Product E", "Electronics", 999.99, "North"),
    ]


@pytest.fixture
def sample_customer_data():
    """Provide sample customer data for tests."""
    customers = [
        (1, "Alice Johnson", "New York", 28),
        (2, "Bob Smith", "Los Angeles", 34),
        (3, "Charlie Brown", "Chicago", 29),
        (4, "Diana Wilson", "Houston", 31),
    ]

    orders = [
        (101, 1, 250.0, "Completed"),
        (102, 1, 150.0, "Completed"),
        (103, 2, 300.0, "Pending"),
        (104, 3, 75.0, "Completed"),
    ]

    return customers, orders


class SparkTestUtils:
    """Utility class for Spark testing helpers."""

    @staticmethod
    def assert_dataframes_equal(df1, df2, check_order=False):
        """
        Assert that two DataFrames are equal.

        Args:
            df1: First DataFrame
            df2: Second DataFrame
            check_order: Whether to check row order (default: False)
        """
        # Check column names
        assert (
            df1.columns == df2.columns
        ), f"Columns differ: {df1.columns} != {df2.columns}"

        # Check row count
        assert (
            df1.count() == df2.count()
        ), f"Row counts differ: {df1.count()} != {df2.count()}"

        # Check data content
        if check_order:
            rows1 = df1.collect()
            rows2 = df2.collect()
            assert rows1 == rows2, "DataFrames have different data"
        else:
            # Convert to sets for unordered comparison
            rows1 = set(df1.collect())
            rows2 = set(df2.collect())
            assert rows1 == rows2, "DataFrames have different data"

    @staticmethod
    def assert_schema_equal(df1, df2):
        """Assert that two DataFrames have the same schema."""
        assert (
            df1.schema == df2.schema
        ), f"Schemas differ:\n{df1.schema}\n!=\n{df2.schema}"

    @staticmethod
    def create_test_dataframe(spark, data, columns, schema=None):
        """Create a test DataFrame with optional schema validation."""
        if schema:
            df = spark.createDataFrame(data, schema)
        else:
            df = spark.createDataFrame(data, columns)
        return df


@pytest.fixture
def spark_utils():
    """Provide SparkTestUtils instance."""
    return SparkTestUtils()


# Pytest hooks for custom behavior
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names."""
    for item in items:
        # Add dataframe marker to all tests
        if "test_" in item.name:
            item.add_marker(pytest.mark.dataframe)

        # Add SQL marker to SQL-related tests
        if "sql" in item.name.lower() or "exercise_3" in item.name:
            item.add_marker(pytest.mark.sql)

        # Add integration marker to integration tests
        if "integration" in item.name.lower() or "workflow" in item.name.lower():
            item.add_marker(pytest.mark.integration)

        # Add slow marker to performance tests
        if "performance" in item.name.lower() or "benchmark" in item.name.lower():
            item.add_marker(pytest.mark.slow)


def pytest_runtest_setup(item):
    """Setup before each test run."""
    # Skip slow tests unless explicitly requested
    if "slow" in [mark.name for mark in item.iter_markers()]:
        if not item.config.getoption("-m") or "slow" not in item.config.getoption("-m"):
            if not item.config.getoption("--runslow", default=False):
                pytest.skip("need --runslow option to run slow tests")


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
