"""
Customer-specific data transformations for business logic.
This module provides customer domain transformations including segmentation, analytics, and business rules.
"""

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    countDistinct,
    desc,
    round as spark_round,
)

from .base_cleaning import BaseDataCleaner
from .base_aggregations import BaseDataAggregator


class CustomerTransformations:
    """
    Customer domain-specific transformations.

    This class provides methods for:
    - Customer data cleaning with business rules
    - Customer segmentation and analytics
    - Customer behavior analysis
    - Customer lifetime value calculations
    """

    def __init__(self):
        self.base_cleaner = BaseDataCleaner()
        self.base_aggregator = BaseDataAggregator()

    def clean_customer_data(self, df: DataFrame) -> DataFrame:
        """
        Clean customer data with customer-specific business rules.

        Args:
            df: Raw customer DataFrame

        Returns:
            Cleaned customer DataFrame
        """
        # Clean customer names
        df = self.base_cleaner.clean_text_column(
            df, "customer_name", remove_special_chars=False
        )

        # Standardize gender values using business mapping
        gender_mappings = {
            "M": "Male",
            "MALE": "Male",
            "MAN": "Male",
            "F": "Female",
            "FEMALE": "Female",
            "WOMAN": "Female",
        }
        df = self.base_cleaner.standardize_categorical_values(
            df, "gender", gender_mappings
        )

        # Set default for unmapped gender values
        df = df.withColumn(
            "gender",
            when(
                col("gender").isNull() | (~col("gender").isin(["Male", "Female"])),
                "Other",
            ).otherwise(col("gender")),
        )

        # Clean city names
        df = self.base_cleaner.clean_text_column(df, "city", remove_special_chars=False)

        # Validate age ranges (business rule: customers must be 13-120 years old)
        df = self.base_cleaner.validate_numeric_range(
            df, "age", min_value=13, max_value=120
        )

        # Standardize customer segment
        segment_mappings = {
            "PREMIUM": "PREMIUM",
            "STANDARD": "STANDARD",
            "BASIC": "BASIC",
            "VIP": "PREMIUM",  # Business rule: VIP customers are treated as PREMIUM
            "GOLD": "PREMIUM",
            "SILVER": "STANDARD",
            "BRONZE": "BASIC",
        }
        df = self.base_cleaner.standardize_categorical_values(
            df, "customer_segment", segment_mappings
        )

        return df

    def calculate_customer_analytics(self, sales_df: DataFrame) -> DataFrame:
        """
        Calculate comprehensive customer analytics from sales data.

        Args:
            sales_df: Sales DataFrame with customer information

        Returns:
            DataFrame with customer analytics
        """
        return (
            sales_df.groupBy("customer_id", "customer_name", "customer_segment")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_transaction_value"),
                spark_max("total_amount").alias("max_transaction_value"),
                spark_sum("quantity").alias("total_items_purchased"),
                countDistinct("product_id").alias("unique_products_purchased"),
                countDistinct("category").alias("unique_categories_purchased"),
                spark_max("transaction_date").alias("last_purchase_date"),
                avg("unit_price").alias("avg_unit_price_paid"),
            )
            .withColumn("total_spent", spark_round(col("total_spent"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .withColumn(
                "avg_unit_price_paid", spark_round(col("avg_unit_price_paid"), 2)
            )
            .orderBy(desc("total_spent"))
        )

    def create_rfm_segmentation(self, sales_df: DataFrame) -> DataFrame:
        """
        Create RFM (Recency, Frequency, Monetary) customer segmentation.

        Args:
            sales_df: Sales DataFrame with customer and transaction data

        Returns:
            DataFrame with RFM scores and segments
        """
        # Calculate RFM metrics
        customer_rfm = sales_df.groupBy("customer_id", "customer_name").agg(
            count("transaction_id").alias("frequency"),
            spark_sum("total_amount").alias("monetary"),
            spark_max("transaction_date").alias("last_purchase"),
        )

        # Calculate RFM scores using quintiles
        # Monetary score (higher is better)
        customer_rfm = customer_rfm.withColumn(
            "monetary_score",
            when(col("monetary") >= 1500, 5)
            .when(col("monetary") >= 1000, 4)
            .when(col("monetary") >= 500, 3)
            .when(col("monetary") >= 200, 2)
            .otherwise(1),
        )

        # Frequency score (higher is better)
        customer_rfm = customer_rfm.withColumn(
            "frequency_score",
            when(col("frequency") >= 15, 5)
            .when(col("frequency") >= 10, 4)
            .when(col("frequency") >= 7, 3)
            .when(col("frequency") >= 4, 2)
            .otherwise(1),
        )

        # Create customer segments based on RFM scores
        customer_segments = customer_rfm.withColumn(
            "rfm_segment",
            when(
                (col("monetary_score") >= 4) & (col("frequency_score") >= 4),
                "Champions",
            )
            .when(
                (col("monetary_score") >= 3) & (col("frequency_score") >= 4),
                "Loyal Customers",
            )
            .when(
                (col("monetary_score") >= 4) & (col("frequency_score") >= 2),
                "Potential Loyalists",
            )
            .when(
                (col("monetary_score") >= 3) & (col("frequency_score") >= 3),
                "New Customers",
            )
            .when(
                (col("monetary_score") >= 2) & (col("frequency_score") >= 2),
                "Promising",
            )
            .when(
                (col("monetary_score") >= 2) & (col("frequency_score") == 1),
                "Need Attention",
            )
            .when(
                (col("monetary_score") == 1) & (col("frequency_score") >= 2),
                "Price Sensitive",
            )
            .otherwise("At Risk"),
        )

        # Add business value indicator
        customer_segments = customer_segments.withColumn(
            "customer_value",
            when(col("rfm_segment").isin(["Champions", "Loyal Customers"]), "High")
            .when(
                col("rfm_segment").isin(["Potential Loyalists", "New Customers"]),
                "Medium",
            )
            .otherwise("Low"),
        )

        return customer_segments.select(
            "customer_id",
            "customer_name",
            "frequency",
            "monetary",
            "last_purchase",
            "monetary_score",
            "frequency_score",
            "rfm_segment",
            "customer_value",
        ).orderBy(desc("monetary"))

    def analyze_customer_behavior_by_segment(
        self, enriched_sales_df: DataFrame
    ) -> DataFrame:
        """
        Analyze customer behavior patterns by segment.

        Args:
            enriched_sales_df: Enriched sales DataFrame

        Returns:
            DataFrame with segment behavior analysis
        """
        return (
            enriched_sales_df.groupBy("customer_segment")
            .agg(
                count("transaction_id").alias("total_transactions"),
                countDistinct("customer_id").alias("total_customers"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_transaction_value"),
                avg("quantity").alias("avg_items_per_transaction"),
                countDistinct("product_id").alias("unique_products_purchased"),
                countDistinct("category").alias("categories_purchased"),
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .withColumn(
                "avg_items_per_transaction",
                spark_round(col("avg_items_per_transaction"), 2),
            )
            .withColumn(
                "revenue_per_customer",
                spark_round(col("total_revenue") / col("total_customers"), 2),
            )
            .orderBy(desc("total_revenue"))
        )

    def identify_high_value_customers(
        self,
        customer_analytics_df: DataFrame,
        revenue_threshold: float = 1000,
        transaction_threshold: int = 5,
    ) -> DataFrame:
        """
        Identify high-value customers based on business criteria.

        Args:
            customer_analytics_df: Customer analytics DataFrame
            revenue_threshold: Minimum revenue to be considered high-value
            transaction_threshold: Minimum transactions to be considered high-value

        Returns:
            DataFrame with high-value customers
        """
        high_value_customers = customer_analytics_df.filter(
            (col("total_spent") >= revenue_threshold)
            & (col("total_transactions") >= transaction_threshold)
        )

        # Add value tier classification
        high_value_customers = high_value_customers.withColumn(
            "value_tier",
            when(col("total_spent") >= 5000, "Platinum")
            .when(col("total_spent") >= 2500, "Gold")
            .when(col("total_spent") >= 1000, "Silver")
            .otherwise("Bronze"),
        )

        # Calculate customer lifetime value score
        high_value_customers = high_value_customers.withColumn(
            "clv_score",
            spark_round(
                (col("total_spent") * 0.6)
                + (col("total_transactions") * 10)
                + (col("unique_products_purchased") * 5),
                2,
            ),
        )

        return high_value_customers.orderBy(desc("clv_score"))

    def calculate_customer_retention_metrics(
        self, sales_df: DataFrame
    ) -> Dict[str, DataFrame]:
        """
        Calculate customer retention and churn metrics.

        Args:
            sales_df: Sales DataFrame

        Returns:
            Dictionary of retention analysis DataFrames
        """
        # Customer purchase frequency analysis
        purchase_frequency = (
            sales_df.groupBy("customer_id")
            .agg(
                count("transaction_id").alias("purchase_count"),
                countDistinct("transaction_date").alias("unique_purchase_days"),
            )
            .withColumn(
                "avg_purchases_per_day",
                spark_round(col("purchase_count") / col("unique_purchase_days"), 2),
            )
        )

        # Customer lifecycle analysis
        customer_lifecycle = sales_df.groupBy("customer_id").agg(
            spark_max("transaction_date").alias("last_purchase"),
            count("transaction_id").alias("total_purchases"),
            spark_sum("total_amount").alias("total_value"),
        )

        return {
            "purchase_frequency": purchase_frequency,
            "customer_lifecycle": customer_lifecycle,
        }
