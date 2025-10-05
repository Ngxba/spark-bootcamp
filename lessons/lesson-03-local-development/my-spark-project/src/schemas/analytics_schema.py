"""
Schema definitions for analytics and aggregated data structures.
This module provides Spark SQL schemas for business intelligence and reporting data.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    LongType,
)


class AnalyticsSchema:
    """
    Schema definitions for analytics and aggregated data structures.

    This class provides schema definitions for:
    - Customer analytics
    - Product analytics
    - Time-based aggregations
    - Key performance indicators
    """

    @staticmethod
    def get_customer_analytics_schema() -> StructType:
        """
        Get the schema for customer analytics data.

        Returns:
            StructType schema for customer analytics
        """
        return StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("customer_segment", StringType(), True),
                StructField("total_transactions", LongType(), False),
                StructField("total_spent", DoubleType(), False),
                StructField("avg_transaction_value", DoubleType(), False),
                StructField("max_transaction_value", DoubleType(), False),
                StructField("min_transaction_value", DoubleType(), False),
                StructField("unique_products_purchased", LongType(), False),
                StructField("last_purchase_date", DateType(), True),
            ]
        )

    @staticmethod
    def get_product_analytics_schema() -> StructType:
        """
        Get the schema for product analytics data.

        Returns:
            StructType schema for product analytics
        """
        return StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("brand", StringType(), True),
                StructField("total_transactions", LongType(), False),
                StructField("total_quantity_sold", LongType(), False),
                StructField("total_revenue", DoubleType(), False),
                StructField("avg_unit_price", DoubleType(), False),
                StructField("unique_customers", LongType(), False),
            ]
        )

    @staticmethod
    def get_category_analytics_schema() -> StructType:
        """
        Get the schema for category analytics data.

        Returns:
            StructType schema for category analytics
        """
        return StructType(
            [
                StructField("category", StringType(), False),
                StructField("total_transactions", LongType(), False),
                StructField("total_quantity_sold", LongType(), False),
                StructField("total_revenue", DoubleType(), False),
                StructField("avg_transaction_value", DoubleType(), False),
                StructField("unique_products", LongType(), False),
                StructField("unique_customers", LongType(), False),
            ]
        )

    @staticmethod
    def get_daily_sales_schema() -> StructType:
        """
        Get the schema for daily sales aggregation data.

        Returns:
            StructType schema for daily sales data
        """
        return StructType(
            [
                StructField("transaction_date", DateType(), False),
                StructField("daily_transactions", LongType(), False),
                StructField("daily_revenue", DoubleType(), False),
                StructField("daily_avg_transaction", DoubleType(), False),
                StructField("daily_unique_customers", LongType(), False),
            ]
        )

    @staticmethod
    def get_monthly_sales_schema() -> StructType:
        """
        Get the schema for monthly sales aggregation data.

        Returns:
            StructType schema for monthly sales data
        """
        return StructType(
            [
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("monthly_transactions", LongType(), False),
                StructField("monthly_revenue", DoubleType(), False),
                StructField("monthly_avg_transaction", DoubleType(), False),
                StructField("monthly_unique_customers", LongType(), False),
            ]
        )

    @staticmethod
    def get_customer_segmentation_schema() -> StructType:
        """
        Get the schema for customer segmentation data.

        Returns:
            StructType schema for customer segmentation
        """
        return StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("frequency", LongType(), False),
                StructField("monetary", DoubleType(), False),
                StructField("last_purchase", DateType(), True),
                StructField("monetary_score", IntegerType(), False),
                StructField("frequency_score", IntegerType(), False),
                StructField("customer_segment_calculated", StringType(), False),
            ]
        )

    @staticmethod
    def get_product_performance_schema() -> StructType:
        """
        Get the schema for product performance ranking data.

        Returns:
            StructType schema for product performance data
        """
        return StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("brand", StringType(), True),
                StructField("total_revenue", DoubleType(), False),
                StructField("total_quantity", LongType(), False),
                StructField("total_transactions", LongType(), False),
                StructField("unique_customers", LongType(), False),
                StructField("avg_price", DoubleType(), False),
                StructField("revenue_rank", LongType(), False),
                StructField("quantity_rank", LongType(), False),
                StructField("customer_reach_rank", LongType(), False),
            ]
        )
