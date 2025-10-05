"""
Data aggregation transformations for Spark DataFrames.
This module provides reusable aggregation functions for analytics and reporting.
"""

from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    count,
    max as spark_max,
    min as spark_min,
    countDistinct,
    when,
    desc,
    asc,
    round as spark_round,
    date_format,
    year,
    month,
    dayofmonth,
    dayofweek,
    quarter,
    to_date,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


class DataAggregator:
    """
    Data aggregation utility class for creating business intelligence reports.

    This class provides methods for:
    - Sales analytics
    - Customer analytics
    - Product performance
    - Time-based aggregations
    - Window functions
    """

    @staticmethod
    def sales_by_customer(df: DataFrame) -> DataFrame:
        """
        Aggregate sales metrics by customer.

        Args:
            df: Sales DataFrame with customer information

        Returns:
            DataFrame with customer-level aggregations
        """
        return (
            df.groupBy("customer_id", "customer_name", "customer_segment")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_transaction_value"),
                spark_max("total_amount").alias("max_transaction_value"),
                spark_min("total_amount").alias("min_transaction_value"),
                countDistinct("product_id").alias("unique_products_purchased"),
                spark_max("transaction_date").alias("last_purchase_date"),
            )
            .withColumn("total_spent", spark_round(col("total_spent"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .orderBy(desc("total_spent"))
        )

    @staticmethod
    def sales_by_product(df: DataFrame) -> DataFrame:
        """
        Aggregate sales metrics by product.

        Args:
            df: Sales DataFrame with product information

        Returns:
            DataFrame with product-level aggregations
        """
        return (
            df.groupBy("product_id", "product_name", "category", "brand")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("unit_price").alias("avg_unit_price"),
                countDistinct("customer_id").alias("unique_customers"),
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn("avg_unit_price", spark_round(col("avg_unit_price"), 2))
            .orderBy(desc("total_revenue"))
        )

    @staticmethod
    def sales_by_category(df: DataFrame) -> DataFrame:
        """
        Aggregate sales metrics by product category.

        Args:
            df: Sales DataFrame with category information

        Returns:
            DataFrame with category-level aggregations
        """
        return (
            df.groupBy("category")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_transaction_value"),
                countDistinct("product_id").alias("unique_products"),
                countDistinct("customer_id").alias("unique_customers"),
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .orderBy(desc("total_revenue"))
        )

    @staticmethod
    def sales_by_time_period(df: DataFrame) -> Dict[str, DataFrame]:
        """
        Create time-based sales aggregations.

        Args:
            df: Sales DataFrame with transaction_date

        Returns:
            Dictionary of DataFrames with different time period aggregations
        """
        # Add time period columns
        df_with_time = (
            df.withColumn("year", year(col("transaction_date")))
            .withColumn("month", month(col("transaction_date")))
            .withColumn("quarter", quarter(col("transaction_date")))
            .withColumn("day_of_week", dayofweek(col("transaction_date")))
        )

        # Daily sales
        daily_sales = (
            df_with_time.groupBy("transaction_date")
            .agg(
                count("transaction_id").alias("daily_transactions"),
                spark_sum("total_amount").alias("daily_revenue"),
                avg("total_amount").alias("daily_avg_transaction"),
                countDistinct("customer_id").alias("daily_unique_customers"),
            )
            .withColumn("daily_revenue", spark_round(col("daily_revenue"), 2))
            .withColumn(
                "daily_avg_transaction", spark_round(col("daily_avg_transaction"), 2)
            )
            .orderBy("transaction_date")
        )

        # Monthly sales
        monthly_sales = (
            df_with_time.groupBy("year", "month")
            .agg(
                count("transaction_id").alias("monthly_transactions"),
                spark_sum("total_amount").alias("monthly_revenue"),
                avg("total_amount").alias("monthly_avg_transaction"),
                countDistinct("customer_id").alias("monthly_unique_customers"),
            )
            .withColumn("monthly_revenue", spark_round(col("monthly_revenue"), 2))
            .withColumn(
                "monthly_avg_transaction",
                spark_round(col("monthly_avg_transaction"), 2),
            )
            .orderBy("year", "month")
        )

        # Day of week analysis
        dow_sales = (
            df_with_time.groupBy("day_of_week")
            .agg(
                count("transaction_id").alias("transactions"),
                spark_sum("total_amount").alias("revenue"),
                avg("total_amount").alias("avg_transaction_value"),
            )
            .withColumn("revenue", spark_round(col("revenue"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .withColumn(
                "day_name",
                when(col("day_of_week") == 1, "Sunday")
                .when(col("day_of_week") == 2, "Monday")
                .when(col("day_of_week") == 3, "Tuesday")
                .when(col("day_of_week") == 4, "Wednesday")
                .when(col("day_of_week") == 5, "Thursday")
                .when(col("day_of_week") == 6, "Friday")
                .when(col("day_of_week") == 7, "Saturday")
                .otherwise("Unknown"),
            )
            .orderBy("day_of_week")
        )

        return {
            "daily_sales": daily_sales,
            "monthly_sales": monthly_sales,
            "day_of_week_sales": dow_sales,
        }

    @staticmethod
    def customer_segmentation(df: DataFrame) -> DataFrame:
        """
        Create customer segmentation based on purchase behavior.

        Args:
            df: Sales DataFrame with customer information

        Returns:
            DataFrame with customer segments
        """
        # Calculate customer metrics
        customer_metrics = df.groupBy("customer_id", "customer_name").agg(
            count("transaction_id").alias("frequency"),
            spark_sum("total_amount").alias("monetary"),
            spark_max("transaction_date").alias("last_purchase"),
        )

        # Define window for percentile calculations
        window = Window.orderBy("monetary")

        # Add RFM scores (simplified version)
        customer_rfm = customer_metrics.withColumn(
            "monetary_score",
            when(col("monetary") >= 1000, 5)
            .when(col("monetary") >= 500, 4)
            .when(col("monetary") >= 200, 3)
            .when(col("monetary") >= 100, 2)
            .otherwise(1),
        ).withColumn(
            "frequency_score",
            when(col("frequency") >= 10, 5)
            .when(col("frequency") >= 7, 4)
            .when(col("frequency") >= 4, 3)
            .when(col("frequency") >= 2, 2)
            .otherwise(1),
        )

        # Create customer segments
        customer_segments = customer_rfm.withColumn(
            "customer_segment_calculated",
            when((col("monetary_score") >= 4) & (col("frequency_score") >= 4), "VIP")
            .when((col("monetary_score") >= 3) & (col("frequency_score") >= 3), "Loyal")
            .when(
                (col("monetary_score") >= 3) | (col("frequency_score") >= 3),
                "Potential",
            )
            .otherwise("New"),
        )

        return customer_segments.select(
            "customer_id",
            "customer_name",
            "frequency",
            "monetary",
            "last_purchase",
            "monetary_score",
            "frequency_score",
            "customer_segment_calculated",
        ).orderBy(desc("monetary"))

    @staticmethod
    def product_performance_ranking(df: DataFrame) -> DataFrame:
        """
        Rank products by performance metrics.

        Args:
            df: Sales DataFrame with product information

        Returns:
            DataFrame with product performance rankings
        """
        # Calculate product metrics
        product_metrics = df.groupBy(
            "product_id", "product_name", "category", "brand"
        ).agg(
            spark_sum("total_amount").alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity"),
            count("transaction_id").alias("total_transactions"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("unit_price").alias("avg_price"),
        )

        # Add ranking columns
        revenue_window = Window.orderBy(desc("total_revenue"))
        quantity_window = Window.orderBy(desc("total_quantity"))
        customer_window = Window.orderBy(desc("unique_customers"))

        product_rankings = (
            product_metrics.withColumn(
                "revenue_rank", row_number().over(revenue_window)
            )
            .withColumn("quantity_rank", row_number().over(quantity_window))
            .withColumn("customer_reach_rank", row_number().over(customer_window))
        )

        return (
            product_rankings.withColumn(
                "total_revenue", spark_round(col("total_revenue"), 2)
            )
            .withColumn("avg_price", spark_round(col("avg_price"), 2))
            .orderBy("revenue_rank")
        )

    @staticmethod
    def cohort_analysis(df: DataFrame) -> DataFrame:
        """
        Perform cohort analysis based on customer first purchase month.

        Args:
            df: Sales DataFrame with customer and date information

        Returns:
            DataFrame with cohort analysis
        """
        # Find first purchase date for each customer
        first_purchase = df.groupBy("customer_id").agg(
            spark_min("transaction_date").alias("first_purchase_date")
        )

        # Join back to get cohort information
        cohort_data = (
            df.join(first_purchase, "customer_id")
            .withColumn(
                "first_purchase_month",
                date_format(col("first_purchase_date"), "yyyy-MM"),
            )
            .withColumn(
                "transaction_month", date_format(col("transaction_date"), "yyyy-MM")
            )
        )

        # Calculate cohort metrics
        cohort_metrics = (
            cohort_data.groupBy("first_purchase_month", "transaction_month")
            .agg(
                countDistinct("customer_id").alias("customers"),
                count("transaction_id").alias("transactions"),
                spark_sum("total_amount").alias("revenue"),
            )
            .withColumn("revenue", spark_round(col("revenue"), 2))
        )

        return cohort_metrics.orderBy("first_purchase_month", "transaction_month")

    def create_sales_aggregations(
        self, enriched_sales_df: DataFrame
    ) -> Dict[str, DataFrame]:
        """
        Create comprehensive sales aggregations.

        Args:
            enriched_sales_df: Enriched sales DataFrame

        Returns:
            Dictionary of aggregated DataFrames
        """
        aggregations = {}

        # Customer analytics
        aggregations["customer_analytics"] = self.sales_by_customer(enriched_sales_df)

        # Product analytics
        aggregations["product_analytics"] = self.sales_by_product(enriched_sales_df)

        # Category analytics
        aggregations["category_analytics"] = self.sales_by_category(enriched_sales_df)

        # Time-based analytics
        time_aggregations = self.sales_by_time_period(enriched_sales_df)
        aggregations.update(time_aggregations)

        # Customer segmentation
        aggregations["customer_segmentation"] = self.customer_segmentation(
            enriched_sales_df
        )

        # Product performance ranking
        aggregations["product_performance"] = self.product_performance_ranking(
            enriched_sales_df
        )

        return aggregations
