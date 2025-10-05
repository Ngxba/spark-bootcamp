"""
Schema definitions for customer data.
This module provides Spark SQL schemas for customer-related data validation and type safety.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class CustomerSchema:
    """
    Schema definitions for customer data.

    This class provides schema definitions for:
    - Raw customer data
    - Customer profile information
    """

    @staticmethod
    def get_schema() -> StructType:
        """
        Get the schema for customer data.

        Returns:
            StructType schema for customer data
        """
        return StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("customer_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("customer_segment", StringType(), True),
            ]
        )

    @staticmethod
    def get_customer_profile_schema() -> StructType:
        """
        Get the schema for extended customer profile data.

        Returns:
            StructType schema for customer profile data
        """
        return StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("customer_name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("registration_date", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
            ]
        )
