"""
Unit tests for data cleaning functionality.
This module tests the DataCleaner class and its methods.
"""

from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from src.transformations.cleaning import DataCleaner


class TestDataCleaner:
    """Test cases for DataCleaner class."""

    def test_remove_nulls(self, spark_session):
        """Test removing null values from DataFrame."""
        # Create test data with nulls
        data = [
            ("1", "John", 25),
            ("2", None, 30),
            ("3", "Alice", None),
            ("4", "Bob", 35),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        # Test removing nulls from all columns
        result = DataCleaner.remove_nulls(df)
        assert result.count() == 2  # Only rows without nulls

        # Test removing nulls from specific columns
        result = DataCleaner.remove_nulls(df, ["name"])
        assert result.count() == 3  # Only remove rows with null names

    def test_fill_nulls(self, spark_session):
        """Test filling null values."""
        # Create test data with nulls
        data = [("1", "John", 25), ("2", None, 30), ("3", "Alice", None)]
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        # Fill nulls
        fill_values = {"name": "Unknown", "age": 0}
        result = DataCleaner.fill_nulls(df, fill_values)

        # Check that nulls are filled
        null_count = result.filter(col("name").isNull()).count()
        assert null_count == 0

        filled_name = (
            result.filter(col("id") == "2").select("name").collect()[0]["name"]
        )
        assert filled_name == "Unknown"

    def test_clean_text_column(self, spark_session):
        """Test text cleaning functionality."""
        # Create test data with messy text
        data = [("1", "  John Doe!@#  "), ("2", "jane_smith$%^"), ("3", "BOB JOHNSON")]
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        # Test cleaning with all options
        result = DataCleaner.clean_text_column(
            df,
            "name",
            remove_special_chars=True,
            trim_whitespace=True,
            to_uppercase=True,
        )

        names = result.select("name").rdd.map(lambda row: row[0]).collect()
        assert "JOHN DOE" in names
        assert "JANESMITH" in names
        assert "BOB JOHNSON" in names

    def test_handle_outliers_clip(self, spark_session):
        """Test outlier handling with clipping method."""
        # Create test data with outliers
        data = [(i, float(i)) for i in range(1, 101)]  # 1 to 100
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("value", DoubleType(), False),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        # Clip outliers (1st and 99th percentile)
        result = DataCleaner.handle_outliers(
            df, "value", method="clip", lower_percentile=0.01, upper_percentile=0.99
        )

        # Check that extreme values are clipped
        min_val = result.select("value").rdd.min()[0]
        max_val = result.select("value").rdd.max()[0]

        assert min_val >= 1.0  # Should be clipped to around 1st percentile
        assert max_val <= 100.0  # Should be clipped to around 99th percentile

    def test_handle_outliers_remove(self, spark_session):
        """Test outlier handling with removal method."""
        # Create test data with clear outliers
        data = [(1, 10.0), (2, 15.0), (3, 12.0), (4, 1000.0), (5, 11.0)]  # Outlier
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("value", DoubleType(), False),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        # Remove outliers
        result = DataCleaner.handle_outliers(
            df, "value", method="remove", lower_percentile=0.1, upper_percentile=0.9
        )

        # Should have fewer rows after removing outliers
        assert result.count() < df.count()

    def test_clean_customer_data(self, spark_session):
        """Test customer data cleaning."""
        # Create messy customer data
        data = [
            ("CUST001", "  John Doe  ", 25, "m", "New York", "premium"),
            ("CUST002", "Jane@Smith", 150, "F", "LA", "STANDARD"),  # Invalid age
            ("CUST003", "Bob", -5, "male", "Chicago", "basic"),  # Invalid age
        ]
        schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("age", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("customer_segment", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        cleaner = DataCleaner()
        result = cleaner.clean_customer_data(df)

        # Check gender standardization
        genders = (
            result.select("gender").distinct().rdd.map(lambda row: row[0]).collect()
        )
        valid_genders = {"Male", "Female", "Other"}
        assert all(gender in valid_genders for gender in genders if gender is not None)

        # Check age validation (invalid ages should be null)
        invalid_age_count = result.filter(col("age").isNull()).count()
        assert invalid_age_count >= 2  # At least the two invalid ages

        # Check customer segment standardization
        segments = (
            result.select("customer_segment")
            .distinct()
            .rdd.map(lambda row: row[0])
            .collect()
        )
        assert all(
            segment and segment.isupper() for segment in segments if segment is not None
        )

    def test_clean_product_data(self, spark_session):
        """Test product data cleaning."""
        # Create messy product data
        data = [
            (
                "PROD001",
                "  Laptop Pro  ",
                "electronics",
                "TechBrand",
                999.99,
                "Supplier A",
            ),
            (
                "PROD002",
                "Mouse@#$",
                "GADGETS",
                "Tech",
                -50.0,
                "Supplier B",
            ),  # Invalid price
            (
                "PROD003",
                "Chair",
                "furniture",
                "FurnCo",
                0.0,
                "Supplier C",
            ),  # Invalid price
        ]
        schema = StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), False),
                StructField("supplier", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        cleaner = DataCleaner()
        result = cleaner.clean_product_data(df)

        # Check category standardization
        categories = (
            result.select("category").distinct().rdd.map(lambda row: row[0]).collect()
        )
        assert all(
            category and category.isupper()
            for category in categories
            if category is not None
        )

        # Check price validation (invalid prices should be null)
        invalid_price_count = result.filter(col("price").isNull()).count()
        assert invalid_price_count >= 2  # At least the two invalid prices

    def test_clean_sales_data(self, spark_session):
        """Test sales data cleaning."""
        # Create sales data with issues
        data = [
            ("TXN001", "CUST001", "PROD001", 2, 25.50, 51.00, "2024-01-15"),
            (
                "TXN002",
                "CUST002",
                "PROD002",
                -1,
                15.75,
                15.75,
                "2024-01-16",
            ),  # Invalid quantity
            (
                "TXN003",
                "CUST003",
                "PROD003",
                3,
                -10.25,
                30.75,
                "2024-01-17",
            ),  # Invalid price
            (
                "TXN004",
                None,
                "PROD001",
                1,
                25.50,
                25.50,
                "2024-01-18",
            ),  # Missing customer
        ]
        schema = StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("total_amount", DoubleType(), False),
                StructField("transaction_date", StringType(), False),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        cleaner = DataCleaner()
        result = cleaner.clean_sales_data(df)

        # Should remove rows with critical null values and invalid data
        assert result.count() < df.count()

        # Check that remaining data has valid quantities and prices
        remaining_data = result.collect()
        for row in remaining_data:
            assert row["quantity"] > 0
            assert row["unit_price"] > 0
            assert row["customer_id"] is not None
            assert row["product_id"] is not None

    def test_get_data_quality_report(self, spark_session):
        """Test data quality report generation."""
        # Create test data with known quality issues
        data = [
            ("1", "John", 25),
            ("2", None, 30),
            ("3", "Alice", None),
            ("4", "Bob", 35),
        ]
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        report = DataCleaner.get_data_quality_report(df, "test_dataset")

        # Check report structure
        assert report["dataset_name"] == "test_dataset"
        assert report["total_rows"] == 4
        assert report["total_columns"] == 3

        # Check null analysis
        assert "name" in report["null_analysis"]
        assert "age" in report["null_analysis"]
        assert report["null_analysis"]["name"]["null_count"] == 1
        assert report["null_analysis"]["age"]["null_count"] == 1

        # Check completeness calculation
        assert 0 <= report["overall_completeness"] <= 100
