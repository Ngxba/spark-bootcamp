"""
Sales-specific data transformations for business logic.
This module provides sales domain transformations including analytics, trends, and business rules.
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
    year,
    month,
    quarter,
    dayofweek,
    date_format,
    lit,
    coalesce,
    abs,
    stddev,
    months_between,
)
from pyspark.sql.window import Window

from .base_cleaning import BaseDataCleaner
from .base_aggregations import BaseDataAggregator


class SalesTransformations:
    """
    Sales domain-specific transformations.

    This class provides methods for:
    - Sales data cleaning with business rules
    - Sales analytics and KPIs
    - Time-based sales analysis
    - Revenue and growth calculations
    """

    def __init__(self):
        self.base_cleaner = BaseDataCleaner()
        self.base_aggregator = BaseDataAggregator()

    def clean_sales_data(self, df: DataFrame) -> DataFrame:
        """
        Clean sales data with sales-specific business rules.

        Args:
            df: Raw sales DataFrame

        Returns:
            Cleaned sales DataFrame
        """
        # Validate quantity (business rule: must be positive)
        df = self.base_cleaner.validate_numeric_range(
            df, "quantity", min_value=1, max_value=1000
        )

        # Validate unit price (business rule: must be positive and reasonable)
        df = self.base_cleaner.validate_numeric_range(
            df, "unit_price", min_value=0.01, max_value=50000
        )

        # Round monetary values to 2 decimal places
        df = df.withColumn("unit_price", spark_round(col("unit_price"), 2))

        # Calculate total amount and validate consistency
        df = df.withColumn(
            "calculated_total", spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Use calculated total if total_amount is null or inconsistent (tolerance: $0.01)
        df = df.withColumn(
            "total_amount",
            coalesce(
                when(
                    abs(col("total_amount") - col("calculated_total")) < 0.01,
                    col("total_amount"),
                ),
                col("calculated_total"),
            ),
        ).drop("calculated_total")

        # Convert transaction_date to proper date format
        df = self.base_cleaner.convert_date_column(df, "transaction_date", "yyyy-MM-dd")

        # Add business date fields
        df = (
            df.withColumn("year", year(col("transaction_date")))
            .withColumn("month", month(col("transaction_date")))
            .withColumn("quarter", quarter(col("transaction_date")))
            .withColumn("day_of_week", dayofweek(col("transaction_date")))
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
        )

        # Add transaction value tier
        df = df.withColumn(
            "transaction_tier",
            when(col("total_amount") >= 1000, "High Value")
            .when(col("total_amount") >= 100, "Medium Value")
            .when(col("total_amount") >= 20, "Low Value")
            .otherwise("Micro Transaction"),
        )

        # Remove rows with critical null values (business rule)
        df = df.filter(
            col("customer_id").isNotNull()
            & col("product_id").isNotNull()
            & col("quantity").isNotNull()
            & col("unit_price").isNotNull()
            & col("transaction_date").isNotNull()
            & col("total_amount").isNotNull()
        )

        return df

    def calculate_sales_kpis(self, sales_df: DataFrame) -> Dict[str, any]:
        """
        Calculate key sales performance indicators.

        Args:
            sales_df: Cleaned sales DataFrame

        Returns:
            Dictionary with KPI values
        """
        kpi_results = sales_df.agg(
            count("transaction_id").alias("total_transactions"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_value"),
            spark_max("total_amount").alias("max_transaction_value"),
            spark_min("total_amount").alias("min_transaction_value"),
            spark_sum("quantity").alias("total_items_sold"),
            avg("quantity").alias("avg_items_per_transaction"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            countDistinct("transaction_date").alias("active_days"),
        ).collect()[0]

        return {
            "total_transactions": kpi_results["total_transactions"],
            "total_revenue": round(kpi_results["total_revenue"], 2),
            "avg_transaction_value": round(kpi_results["avg_transaction_value"], 2),
            "max_transaction_value": kpi_results["max_transaction_value"],
            "min_transaction_value": kpi_results["min_transaction_value"],
            "total_items_sold": kpi_results["total_items_sold"],
            "avg_items_per_transaction": round(
                kpi_results["avg_items_per_transaction"], 2
            ),
            "unique_customers": kpi_results["unique_customers"],
            "unique_products": kpi_results["unique_products"],
            "active_days": kpi_results["active_days"],
            "avg_daily_revenue": (
                round(kpi_results["total_revenue"] / kpi_results["active_days"], 2)
                if kpi_results["active_days"] > 0
                else 0
            ),
        }

    def create_time_based_sales_analysis(
        self, sales_df: DataFrame
    ) -> Dict[str, DataFrame]:
        """
        Create comprehensive time-based sales analysis.

        Args:
            sales_df: Sales DataFrame with date fields

        Returns:
            Dictionary of time-based analysis DataFrames
        """
        # Daily sales analysis
        daily_sales = (
            sales_df.groupBy("transaction_date")
            .agg(
                count("transaction_id").alias("daily_transactions"),
                spark_sum("total_amount").alias("daily_revenue"),
                avg("total_amount").alias("daily_avg_transaction"),
                spark_sum("quantity").alias("daily_items_sold"),
                countDistinct("customer_id").alias("daily_unique_customers"),
            )
            .withColumn("daily_revenue", spark_round(col("daily_revenue"), 2))
            .withColumn(
                "daily_avg_transaction", spark_round(col("daily_avg_transaction"), 2)
            )
            .orderBy("transaction_date")
        )

        # Monthly sales analysis
        monthly_sales = (
            sales_df.groupBy("year", "month")
            .agg(
                count("transaction_id").alias("monthly_transactions"),
                spark_sum("total_amount").alias("monthly_revenue"),
                avg("total_amount").alias("monthly_avg_transaction"),
                countDistinct("customer_id").alias("monthly_unique_customers"),
                countDistinct("transaction_date").alias("active_days_in_month"),
            )
            .withColumn("monthly_revenue", spark_round(col("monthly_revenue"), 2))
            .withColumn(
                "monthly_avg_transaction",
                spark_round(col("monthly_avg_transaction"), 2),
            )
            .withColumn(
                "avg_daily_revenue",
                spark_round(col("monthly_revenue") / col("active_days_in_month"), 2),
            )
            .orderBy("year", "month")
        )

        # Day of week analysis
        dow_sales = (
            sales_df.groupBy("day_of_week", "day_name")
            .agg(
                count("transaction_id").alias("transactions"),
                spark_sum("total_amount").alias("revenue"),
                avg("total_amount").alias("avg_transaction_value"),
                avg("quantity").alias("avg_items_per_transaction"),
            )
            .withColumn("revenue", spark_round(col("revenue"), 2))
            .withColumn(
                "avg_transaction_value", spark_round(col("avg_transaction_value"), 2)
            )
            .withColumn(
                "avg_items_per_transaction",
                spark_round(col("avg_items_per_transaction"), 2),
            )
            .orderBy("day_of_week")
        )

        # Quarterly analysis
        quarterly_sales = (
            sales_df.groupBy("year", "quarter")
            .agg(
                count("transaction_id").alias("quarterly_transactions"),
                spark_sum("total_amount").alias("quarterly_revenue"),
                avg("total_amount").alias("quarterly_avg_transaction"),
                countDistinct("customer_id").alias("quarterly_unique_customers"),
            )
            .withColumn("quarterly_revenue", spark_round(col("quarterly_revenue"), 2))
            .withColumn(
                "quarterly_avg_transaction",
                spark_round(col("quarterly_avg_transaction"), 2),
            )
            .orderBy("year", "quarter")
        )

        return {
            "daily_sales": daily_sales,
            "monthly_sales": monthly_sales,
            "day_of_week_sales": dow_sales,
            "quarterly_sales": quarterly_sales,
        }

    def analyze_transaction_patterns(self, sales_df: DataFrame) -> DataFrame:
        """
        Analyze transaction patterns and behaviors.

        Args:
            sales_df: Sales DataFrame

        Returns:
            DataFrame with transaction pattern analysis
        """
        return (
            sales_df.groupBy("transaction_tier")
            .agg(
                count("transaction_id").alias("transaction_count"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_transaction_value"),
                avg("quantity").alias("avg_items_per_transaction"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("product_id").alias("unique_products"),
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
                "revenue_contribution_pct",
                spark_round(
                    (
                        col("total_revenue")
                        / sales_df.agg(spark_sum("total_amount")).collect()[0][0]
                    )
                    * 100,
                    2,
                ),
            )
            .orderBy(desc("total_revenue"))
        )

    def calculate_sales_growth_metrics(self, sales_df: DataFrame) -> DataFrame:
        """
        Calculate sales growth metrics over time.

        Args:
            sales_df: Sales DataFrame

        Returns:
            DataFrame with growth metrics
        """
        # Monthly growth analysis
        monthly_data = sales_df.groupBy("year", "month").agg(
            spark_sum("total_amount").alias("monthly_revenue")
        )

        # Add previous month revenue for growth calculation
        window_spec = Window.orderBy("year", "month")

        from pyspark.sql.functions import lag

        growth_analysis = (
            monthly_data.withColumn(
                "prev_month_revenue", lag("monthly_revenue").over(window_spec)
            )
            .withColumn(
                "month_over_month_growth",
                when(
                    col("prev_month_revenue").isNotNull(),
                    spark_round(
                        (
                            (col("monthly_revenue") - col("prev_month_revenue"))
                            / col("prev_month_revenue")
                        )
                        * 100,
                        2,
                    ),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "growth_trend",
                when(col("month_over_month_growth") > 10, "High Growth")
                .when(col("month_over_month_growth") > 0, "Positive Growth")
                .when(col("month_over_month_growth") > -10, "Slight Decline")
                .when(col("month_over_month_growth").isNotNull(), "Significant Decline")
                .otherwise("No Data"),
            )
        )

        return growth_analysis.orderBy("year", "month")

    def identify_sales_anomalies(self, daily_sales_df: DataFrame) -> DataFrame:
        """
        Identify sales anomalies and outliers.

        Args:
            daily_sales_df: Daily sales DataFrame

        Returns:
            DataFrame with anomaly indicators
        """
        # Calculate statistical measures
        stats = daily_sales_df.select(
            avg("daily_revenue").alias("avg_revenue"),
            stddev("daily_revenue").alias("stddev_revenue"),
        ).collect()[0]

        avg_revenue = stats["avg_revenue"]
        stddev_revenue = stats["stddev_revenue"]

        # Identify anomalies (beyond 2 standard deviations)
        anomaly_threshold_high = avg_revenue + (2 * stddev_revenue)
        anomaly_threshold_low = avg_revenue - (2 * stddev_revenue)

        anomalies = (
            daily_sales_df.withColumn(
                "is_anomaly",
                when(
                    (col("daily_revenue") > anomaly_threshold_high)
                    | (col("daily_revenue") < anomaly_threshold_low),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "anomaly_type",
                when(col("daily_revenue") > anomaly_threshold_high, "High Revenue Day")
                .when(col("daily_revenue") < anomaly_threshold_low, "Low Revenue Day")
                .otherwise("Normal"),
            )
            .withColumn(
                "deviation_from_avg",
                spark_round(
                    (col("daily_revenue") - avg_revenue) / avg_revenue * 100, 2
                ),
            )
        )

        return anomalies.orderBy("transaction_date")

    def create_cohort_analysis(self, sales_df: DataFrame) -> DataFrame:
        """
        Create cohort analysis based on customer first purchase month.

        Args:
            sales_df: Sales DataFrame

        Returns:
            DataFrame with cohort analysis
        """
        # Find first purchase date for each customer
        first_purchase = sales_df.groupBy("customer_id").agg(
            spark_min("transaction_date").alias("first_purchase_date")
        )

        # Join back to get cohort information
        cohort_data = (
            sales_df.join(first_purchase, "customer_id")
            .withColumn(
                "first_purchase_month",
                date_format(col("first_purchase_date"), "yyyy-MM"),
            )
            .withColumn(
                "transaction_month", date_format(col("transaction_date"), "yyyy-MM")
            )
            .withColumn(
                "months_since_first_purchase",
                months_between(col("transaction_date"), col("first_purchase_date")),
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
