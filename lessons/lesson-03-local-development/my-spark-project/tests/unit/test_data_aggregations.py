"""
Unit tests for data aggregation functionality.
This module tests the DataAggregator class and its methods.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

from src.transformations.aggregations import DataAggregator
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


class TestDataAggregator:
    """Test cases for DataAggregator class."""

    @pytest.fixture
    def enriched_sales_data(self, spark_session):
        """Create enriched sales data for testing."""
        data = [
            (
                "TXN001",
                "CUST001",
                "PROD001",
                2,
                25.50,
                51.00,
                "2024-01-15",
                "John Doe",
                35,
                "Male",
                "New York",
                "PREMIUM",
                "Laptop",
                "Electronics",
                "TechBrand",
                "Supplier A",
            ),
            (
                "TXN002",
                "CUST002",
                "PROD002",
                1,
                15.75,
                15.75,
                "2024-01-16",
                "Jane Smith",
                28,
                "Female",
                "Los Angeles",
                "STANDARD",
                "Mouse",
                "Electronics",
                "TechBrand",
                "Supplier B",
            ),
            (
                "TXN003",
                "CUST001",
                "PROD003",
                3,
                10.25,
                30.75,
                "2024-01-17",
                "John Doe",
                35,
                "Male",
                "New York",
                "PREMIUM",
                "Coffee Mug",
                "Home",
                "HomeGoods",
                "Supplier C",
            ),
            (
                "TXN004",
                "CUST003",
                "PROD001",
                1,
                25.50,
                25.50,
                "2024-01-18",
                "Bob Johnson",
                42,
                "Male",
                "Chicago",
                "PREMIUM",
                "Laptop",
                "Electronics",
                "TechBrand",
                "Supplier A",
            ),
            (
                "TXN005",
                "CUST002",
                "PROD004",
                2,
                8.99,
                17.98,
                "2024-02-15",
                "Jane Smith",
                28,
                "Female",
                "Los Angeles",
                "STANDARD",
                "Notebook",
                "Office",
                "OfficeSupply",
                "Supplier A",
            ),
        ]

        schema = StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("total_amount", DoubleType(), False),
                StructField("transaction_date", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("age", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("brand", StringType(), True),
                StructField("supplier", StringType(), True),
            ]
        )

        df = spark_session.createDataFrame(data, schema)
        # Convert transaction_date to DateType
        return df.withColumn(
            "transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")
        )

    def test_sales_by_customer(self, enriched_sales_data):
        """Test customer-level sales aggregation."""
        aggregator = DataAggregator()
        result = aggregator.sales_by_customer(enriched_sales_data)

        # Check that we have the right number of customers
        assert result.count() == 3

        # Check that John Doe (CUST001) has 2 transactions
        john_data = result.filter(col("customer_id") == "CUST001").collect()[0]
        assert john_data["total_transactions"] == 2
        assert john_data["total_spent"] == 81.75  # 51.00 + 30.75
        assert john_data["unique_products_purchased"] == 2

        # Check aggregation columns exist
        expected_columns = [
            "customer_id",
            "customer_name",
            "customer_segment",
            "total_transactions",
            "total_spent",
            "avg_transaction_value",
            "max_transaction_value",
            "min_transaction_value",
            "unique_products_purchased",
            "last_purchase_date",
        ]
        for col_name in expected_columns:
            assert col_name in result.columns

    def test_sales_by_product(self, enriched_sales_data):
        """Test product-level sales aggregation."""
        aggregator = DataAggregator()
        result = aggregator.sales_by_product(enriched_sales_data)

        # Check that we have the right number of products
        assert result.count() == 4

        # Check that Laptop (PROD001) has correct aggregations
        laptop_data = result.filter(col("product_id") == "PROD001").collect()[0]
        assert laptop_data["total_transactions"] == 2
        assert laptop_data["total_quantity_sold"] == 3  # 2 + 1
        assert laptop_data["total_revenue"] == 76.50  # 51.00 + 25.50
        assert laptop_data["unique_customers"] == 2

        # Check aggregation columns exist
        expected_columns = [
            "product_id",
            "product_name",
            "category",
            "brand",
            "total_transactions",
            "total_quantity_sold",
            "total_revenue",
            "avg_unit_price",
            "unique_customers",
        ]
        for col_name in expected_columns:
            assert col_name in result.columns

    def test_sales_by_category(self, enriched_sales_data):
        """Test category-level sales aggregation."""
        aggregator = DataAggregator()
        result = aggregator.sales_by_category(enriched_sales_data)

        # Check that we have the right number of categories
        assert result.count() == 3  # Electronics, Home, Office

        # Check Electronics category aggregations
        electronics_data = result.filter(col("category") == "Electronics").collect()[0]
        assert electronics_data["total_transactions"] == 3
        assert electronics_data["unique_products"] == 2  # Laptop and Mouse
        assert electronics_data["unique_customers"] == 3

        # Check aggregation columns exist
        expected_columns = [
            "category",
            "total_transactions",
            "total_quantity_sold",
            "total_revenue",
            "avg_transaction_value",
            "unique_products",
            "unique_customers",
        ]
        for col_name in expected_columns:
            assert col_name in result.columns

    def test_sales_by_time_period(self, enriched_sales_data):
        """Test time-based sales aggregations."""
        aggregator = DataAggregator()
        result = aggregator.sales_by_time_period(enriched_sales_data)

        # Check that we get all expected time aggregations
        expected_keys = ["daily_sales", "monthly_sales", "day_of_week_sales"]
        for key in expected_keys:
            assert key in result

        # Check daily sales
        daily_sales = result["daily_sales"]
        assert daily_sales.count() > 0

        # Check monthly sales
        monthly_sales = result["monthly_sales"]
        assert monthly_sales.count() == 2  # January and February 2024

        # Check day of week sales
        dow_sales = result["day_of_week_sales"]
        assert dow_sales.count() > 0
        assert "day_name" in dow_sales.columns

    def test_customer_segmentation(self, enriched_sales_data):
        """Test customer segmentation functionality."""
        aggregator = DataAggregator()
        result = aggregator.customer_segmentation(enriched_sales_data)

        # Check that we have the right number of customers
        assert result.count() == 3

        # Check that segmentation columns exist
        expected_columns = [
            "customer_id",
            "customer_name",
            "frequency",
            "monetary",
            "last_purchase",
            "monetary_score",
            "frequency_score",
            "customer_segment_calculated",
        ]
        for col_name in expected_columns:
            assert col_name in result.columns

        # Check that scores are within expected ranges (1-5)
        scores = result.select("monetary_score", "frequency_score").collect()
        for row in scores:
            assert 1 <= row["monetary_score"] <= 5
            assert 1 <= row["frequency_score"] <= 5

        # Check that segments are valid
        valid_segments = {"VIP", "Loyal", "Potential", "New"}
        segments = (
            result.select("customer_segment_calculated")
            .distinct()
            .rdd.map(lambda row: row[0])
            .collect()
        )
        assert all(segment in valid_segments for segment in segments)

    def test_product_performance_ranking(self, enriched_sales_data):
        """Test product performance ranking."""
        aggregator = DataAggregator()
        result = aggregator.product_performance_ranking(enriched_sales_data)

        # Check that we have the right number of products
        assert result.count() == 4

        # Check ranking columns exist
        expected_columns = [
            "product_id",
            "product_name",
            "category",
            "brand",
            "total_revenue",
            "total_quantity",
            "total_transactions",
            "unique_customers",
            "avg_price",
            "revenue_rank",
            "quantity_rank",
            "customer_reach_rank",
        ]
        for col_name in expected_columns:
            assert col_name in result.columns

        # Check that rankings are sequential starting from 1
        revenue_ranks = (
            result.select("revenue_rank").rdd.map(lambda row: row[0]).collect()
        )
        assert min(revenue_ranks) == 1
        assert max(revenue_ranks) == 4
        assert len(set(revenue_ranks)) == 4  # All ranks should be unique

    def test_create_sales_aggregations(self, enriched_sales_data):
        """Test comprehensive sales aggregations creation."""
        aggregator = DataAggregator()
        result = aggregator.create_sales_aggregations(enriched_sales_data)

        # Check that all expected aggregations are created
        expected_keys = [
            "customer_analytics",
            "product_analytics",
            "category_analytics",
            "daily_sales",
            "monthly_sales",
            "day_of_week_sales",
            "customer_segmentation",
            "product_performance",
        ]
        for key in expected_keys:
            assert key in result
            assert result[key].count() > 0

    def test_aggregation_data_types(self, enriched_sales_data):
        """Test that aggregations return correct data types."""
        aggregator = DataAggregator()

        # Test customer analytics
        customer_analytics = aggregator.sales_by_customer(enriched_sales_data)
        schema_dict = {
            field.name: field.dataType for field in customer_analytics.schema.fields
        }

        # Check that numeric aggregations are the right type
        from pyspark.sql.types import LongType, DoubleType

        assert isinstance(schema_dict["total_transactions"], LongType)
        assert isinstance(schema_dict["total_spent"], DoubleType)
        assert isinstance(schema_dict["avg_transaction_value"], DoubleType)

    def test_empty_dataframe_handling(self, spark_session):
        """Test that aggregations handle empty DataFrames gracefully."""
        # Create empty DataFrame with correct schema
        schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("customer_segment", StringType(), True),
                StructField("total_amount", DoubleType(), False),
                StructField("transaction_date", DateType(), False),
            ]
        )
        empty_df = spark_session.createDataFrame([], schema)

        aggregator = DataAggregator()

        # Test that aggregations don't fail on empty data
        result = aggregator.sales_by_customer(empty_df)
        assert result.count() == 0

    def test_null_handling_in_aggregations(self, spark_session):
        """Test that aggregations handle null values correctly."""
        # Create data with null values
        data = [
            (
                "TXN001",
                "CUST001",
                "PROD001",
                2,
                25.50,
                51.00,
                "2024-01-15",
                "John Doe",
                None,
                "Male",
                "New York",
                None,
                "Laptop",
                "Electronics",
                "TechBrand",
                "Supplier A",
            ),
            (
                "TXN002",
                None,
                "PROD002",
                1,
                15.75,
                15.75,
                "2024-01-16",
                None,
                28,
                "Female",
                "Los Angeles",
                "STANDARD",
                "Mouse",
                "Electronics",
                "TechBrand",
                "Supplier B",
            ),
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
                StructField("customer_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("brand", StringType(), True),
                StructField("supplier", StringType(), True),
            ]
        )

        df = spark_session.createDataFrame(data, schema)
        df = df.withColumn(
            "transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")
        )

        aggregator = DataAggregator()

        # Test that aggregations handle nulls without failing
        customer_result = aggregator.sales_by_customer(df)
        # Should only have one customer (CUST001) since other has null customer_id
        assert customer_result.count() == 1

        product_result = aggregator.sales_by_product(df)
        # Should have both products
        assert product_result.count() == 2
