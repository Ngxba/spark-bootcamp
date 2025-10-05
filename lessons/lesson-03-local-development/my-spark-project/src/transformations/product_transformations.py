"""
Product-specific data transformations for business logic.
This module provides product domain transformations including analytics, performance metrics, and business rules.
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
    min as spark_min,
    countDistinct,
    desc,
    round as spark_round,
    row_number,
)
from pyspark.sql.window import Window

from .base_cleaning import BaseDataCleaner
from .base_aggregations import BaseDataAggregator


class ProductTransformations:
    """
    Product domain-specific transformations.

    This class provides methods for:
    - Product data cleaning with business rules
    - Product performance analytics
    - Category analysis
    - Inventory and pricing insights
    """

    def __init__(self):
        self.base_cleaner = BaseDataCleaner()
        self.base_aggregator = BaseDataAggregator()

    def clean_product_data(self, df: DataFrame) -> DataFrame:
        """
        Clean product data with product-specific business rules.

        Args:
            df: Raw product DataFrame

        Returns:
            Cleaned product DataFrame
        """
        # Clean product names
        df = self.base_cleaner.clean_text_column(
            df, "product_name", remove_special_chars=False
        )

        # Standardize category names using business taxonomy
        category_mappings = {
            "ELECTRONICS": "Electronics",
            "ELECTRONIC": "Electronics",
            "TECH": "Electronics",
            "TECHNOLOGY": "Electronics",
            "HOME": "Home & Garden",
            "GARDEN": "Home & Garden",
            "HOUSE": "Home & Garden",
            "OFFICE": "Office Supplies",
            "STATIONERY": "Office Supplies",
            "FURNITURE": "Furniture",
            "FURNISHING": "Furniture",
            "CLOTHING": "Apparel",
            "CLOTHES": "Apparel",
            "FASHION": "Apparel",
        }
        df = self.base_cleaner.standardize_categorical_values(
            df, "category", category_mappings
        )

        # Clean brand names
        df = self.base_cleaner.clean_text_column(
            df, "brand", remove_special_chars=False
        )

        # Validate price (business rule: prices must be positive and reasonable)
        df = self.base_cleaner.validate_numeric_range(
            df, "price", min_value=0.01, max_value=50000
        )

        # Round prices to 2 decimal places
        df = df.withColumn("price", spark_round(col("price"), 2))

        # Clean supplier names
        df = self.base_cleaner.clean_text_column(
            df, "supplier", remove_special_chars=False
        )

        # Add price tier classification
        df = df.withColumn(
            "price_tier",
            when(col("price") >= 1000, "Premium")
            .when(col("price") >= 100, "Mid-Range")
            .when(col("price") >= 20, "Budget")
            .otherwise("Economy"),
        )

        return df

    def calculate_product_analytics(self, sales_df: DataFrame) -> DataFrame:
        """
        Calculate comprehensive product analytics from sales data.

        Args:
            sales_df: Sales DataFrame with product information

        Returns:
            DataFrame with product analytics
        """
        return (
            sales_df.groupBy("product_id", "product_name", "category", "brand")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("unit_price").alias("avg_unit_price"),
                spark_min("unit_price").alias("min_unit_price"),
                spark_max("unit_price").alias("max_unit_price"),
                countDistinct("customer_id").alias("unique_customers"),
                avg("quantity").alias("avg_quantity_per_transaction"),
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn("avg_unit_price", spark_round(col("avg_unit_price"), 2))
            .withColumn(
                "avg_quantity_per_transaction",
                spark_round(col("avg_quantity_per_transaction"), 2),
            )
            .withColumn(
                "revenue_per_customer",
                spark_round(col("total_revenue") / col("unique_customers"), 2),
            )
            .orderBy(desc("total_revenue"))
        )

    def create_product_performance_ranking(
        self, product_analytics_df: DataFrame
    ) -> DataFrame:
        """
        Create comprehensive product performance rankings.

        Args:
            product_analytics_df: Product analytics DataFrame

        Returns:
            DataFrame with product performance rankings
        """
        # Add ranking columns using window functions
        revenue_window = Window.orderBy(desc("total_revenue"))
        quantity_window = Window.orderBy(desc("total_quantity_sold"))
        customer_window = Window.orderBy(desc("unique_customers"))
        transaction_window = Window.orderBy(desc("total_transactions"))

        product_rankings = (
            product_analytics_df.withColumn(
                "revenue_rank", row_number().over(revenue_window)
            )
            .withColumn("quantity_rank", row_number().over(quantity_window))
            .withColumn("customer_reach_rank", row_number().over(customer_window))
            .withColumn("transaction_rank", row_number().over(transaction_window))
        )

        # Calculate overall performance score
        product_rankings = product_rankings.withColumn(
            "performance_score",
            spark_round(
                (100 - col("revenue_rank")) * 0.4
                + (100 - col("quantity_rank")) * 0.3
                + (100 - col("customer_reach_rank")) * 0.2
                + (100 - col("transaction_rank")) * 0.1,
                2,
            ),
        )

        # Add performance tier
        product_rankings = product_rankings.withColumn(
            "performance_tier",
            when(col("performance_score") >= 80, "Top Performer")
            .when(col("performance_score") >= 60, "Strong Performer")
            .when(col("performance_score") >= 40, "Average Performer")
            .when(col("performance_score") >= 20, "Below Average")
            .otherwise("Poor Performer"),
        )

        return product_rankings.orderBy("revenue_rank")

    def analyze_category_performance(self, enriched_sales_df: DataFrame) -> DataFrame:
        """
        Analyze product category performance.

        Args:
            enriched_sales_df: Enriched sales DataFrame

        Returns:
            DataFrame with category performance analysis
        """
        category_analysis = (
            enriched_sales_df.groupBy("category")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_transaction_value"),
                avg("unit_price").alias("avg_unit_price"),
                countDistinct("product_id").alias("unique_products"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("brand").alias("unique_brands"),
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .withColumn("avg_unit_price", spark_round(col("avg_unit_price"), 2))
        )

        # Add market share calculation
        total_revenue = category_analysis.agg(spark_sum("total_revenue")).collect()[0][
            0
        ]

        category_analysis = category_analysis.withColumn(
            "market_share_pct",
            spark_round((col("total_revenue") / total_revenue) * 100, 2),
        )

        # Add category performance indicators
        category_analysis = category_analysis.withColumn(
            "revenue_per_product",
            spark_round(col("total_revenue") / col("unique_products"), 2),
        ).withColumn(
            "revenue_per_customer",
            spark_round(col("total_revenue") / col("unique_customers"), 2),
        )

        return category_analysis.orderBy(desc("total_revenue"))

    def identify_bestselling_products(
        self, product_analytics_df: DataFrame, top_n: int = 10
    ) -> Dict[str, DataFrame]:
        """
        Identify bestselling products by different metrics.

        Args:
            product_analytics_df: Product analytics DataFrame
            top_n: Number of top products to return

        Returns:
            Dictionary of DataFrames with top products by different metrics
        """
        # Top by revenue
        top_revenue = product_analytics_df.orderBy(desc("total_revenue")).limit(top_n)

        # Top by quantity sold
        top_quantity = product_analytics_df.orderBy(desc("total_quantity_sold")).limit(
            top_n
        )

        # Top by customer reach
        top_customers = product_analytics_df.orderBy(desc("unique_customers")).limit(
            top_n
        )

        # Top by transaction frequency
        top_transactions = product_analytics_df.orderBy(
            desc("total_transactions")
        ).limit(top_n)

        return {
            "top_revenue": top_revenue,
            "top_quantity": top_quantity,
            "top_customer_reach": top_customers,
            "top_transactions": top_transactions,
        }

    def analyze_pricing_trends(self, sales_df: DataFrame) -> DataFrame:
        """
        Analyze pricing trends and price elasticity.

        Args:
            sales_df: Sales DataFrame

        Returns:
            DataFrame with pricing analysis
        """
        # Group by product and analyze price variations
        pricing_analysis = sales_df.groupBy(
            "product_id", "product_name", "category"
        ).agg(
            avg("unit_price").alias("avg_price"),
            spark_min("unit_price").alias("min_price"),
            spark_max("unit_price").alias("max_price"),
            count("transaction_id").alias("total_sales"),
            spark_sum("quantity").alias("total_quantity"),
        )

        # Calculate price range and stability
        pricing_analysis = pricing_analysis.withColumn(
            "price_range", spark_round(col("max_price") - col("min_price"), 2)
        ).withColumn(
            "price_stability",
            when(col("price_range") == 0, "Stable")
            .when(col("price_range") <= col("avg_price") * 0.1, "Low Variance")
            .when(col("price_range") <= col("avg_price") * 0.3, "Medium Variance")
            .otherwise("High Variance"),
        )

        # Add price competitiveness within category
        category_window = Window.partitionBy("category")

        pricing_analysis = pricing_analysis.withColumn(
            "category_avg_price", avg("avg_price").over(category_window)
        ).withColumn(
            "price_position",
            when(col("avg_price") > col("category_avg_price") * 1.2, "Premium")
            .when(col("avg_price") > col("category_avg_price") * 0.8, "Market")
            .otherwise("Budget"),
        )

        return pricing_analysis.orderBy("category", desc("total_sales"))

    def calculate_product_lifecycle_metrics(self, sales_df: DataFrame) -> DataFrame:
        """
        Calculate product lifecycle and adoption metrics.

        Args:
            sales_df: Sales DataFrame

        Returns:
            DataFrame with lifecycle metrics
        """
        # Calculate product introduction and performance over time
        lifecycle_metrics = sales_df.groupBy(
            "product_id", "product_name", "category"
        ).agg(
            spark_min("transaction_date").alias("first_sale_date"),
            spark_max("transaction_date").alias("last_sale_date"),
            count("transaction_id").alias("total_transactions"),
            countDistinct("transaction_date").alias("active_days"),
            spark_sum("total_amount").alias("total_revenue"),
        )

        # Calculate average daily performance
        lifecycle_metrics = lifecycle_metrics.withColumn(
            "avg_daily_revenue",
            spark_round(col("total_revenue") / col("active_days"), 2),
        ).withColumn(
            "avg_daily_transactions",
            spark_round(col("total_transactions") / col("active_days"), 2),
        )

        # Classify product maturity
        lifecycle_metrics = lifecycle_metrics.withColumn(
            "maturity_stage",
            when(col("active_days") <= 30, "Introduction")
            .when(col("active_days") <= 90, "Growth")
            .when(col("active_days") <= 180, "Maturity")
            .otherwise("Decline/Stable"),
        )

        return lifecycle_metrics.orderBy(desc("total_revenue"))
