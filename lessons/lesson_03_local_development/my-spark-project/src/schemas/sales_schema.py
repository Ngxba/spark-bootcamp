"""
Schema definitions for sales transaction data.
This module provides Spark SQL schemas for sales-related data validation and type safety.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)


class SalesSchema:
    """
    Schema definitions for sales transaction data.

    This class provides schema definitions for:
    - Raw sales transactions
    - Enriched sales data with customer and product details
    """

    @staticmethod
    def get_schema() -> StructType:
        """
        Get the schema for raw sales transaction data.

        Returns:
            StructType schema for sales data
        """
        return StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField(
                    "transaction_date", StringType(), True
                ),  # String initially, converted to DateType in cleaning
            ]
        )

    @staticmethod
    def get_enriched_sales_schema() -> StructType:
        """
        Get the schema for enriched sales data (with customer and product details).

        Returns:
            StructType schema for enriched sales data
        """
        return StructType(
            [
                # Sales fields
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("transaction_date", DateType(), True),
                # Customer fields
                StructField("customer_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("customer_segment", StringType(), True),
                # Product fields
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("supplier", StringType(), True),
            ]
        )
