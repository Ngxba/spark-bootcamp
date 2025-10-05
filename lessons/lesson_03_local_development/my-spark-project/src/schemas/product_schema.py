"""
Schema definitions for product data.
This module provides Spark SQL schemas for product-related data validation and type safety.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)


class ProductSchema:
    """
    Schema definitions for product data.

    This class provides schema definitions for:
    - Raw product data
    - Product catalog information
    - Product inventory data
    """

    @staticmethod
    def get_schema() -> StructType:
        """
        Get the schema for basic product data.

        Returns:
            StructType schema for product data
        """
        return StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("supplier", StringType(), True),
            ]
        )

    @staticmethod
    def get_product_catalog_schema() -> StructType:
        """
        Get the schema for extended product catalog data.

        Returns:
            StructType schema for product catalog data
        """
        return StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("category", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("cost", DoubleType(), True),
                StructField("supplier", StringType(), True),
                StructField("supplier_contact", StringType(), True),
                StructField("weight", DoubleType(), True),
                StructField("dimensions", StringType(), True),
                StructField("color", StringType(), True),
                StructField("material", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("created_date", StringType(), True),
                StructField("last_updated", StringType(), True),
            ]
        )

    @staticmethod
    def get_product_inventory_schema() -> StructType:
        """
        Get the schema for product inventory data.

        Returns:
            StructType schema for product inventory data
        """
        return StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("warehouse_id", StringType(), True),
                StructField("stock_quantity", IntegerType(), True),
                StructField("reserved_quantity", IntegerType(), True),
                StructField("available_quantity", IntegerType(), True),
                StructField("reorder_level", IntegerType(), True),
                StructField("max_stock_level", IntegerType(), True),
                StructField("last_restocked", StringType(), True),
                StructField("expiry_date", StringType(), True),
            ]
        )
