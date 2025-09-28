"""
Exercise 6 Solutions: Analytics and Aggregations
================================================

This file contains the complete solutions for Exercise 6.
Study these solutions to understand advanced analytics, aggregations, and window functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, variance, collect_list, collect_set,
    row_number, rank, dense_rank, lag, lead, first, last,
    percentile_approx, desc, asc, datediff, round as spark_round,
    percent_rank, expr, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from typing import List, Tuple


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise6-Solutions") \
        .master("local[*]") \
        .getOrCreate()


def exercise_6a(spark: SparkSession, sales_data: List[tuple]):
    """Perform basic statistical analysis on sales data."""
    # Create DataFrame
    df = spark.createDataFrame(sales_data, ["product_category", "region", "sales_amount", "quantity"])

    # Statistical analysis by product category
    result = df.groupBy("product_category") \
               .agg(
                   spark_sum("sales_amount").alias("total_sales"),
                   spark_round(avg("sales_amount"), 2).alias("avg_sales"),
                   spark_max("sales_amount").alias("max_sales"),
                   spark_min("sales_amount").alias("min_sales"),
                   spark_round(stddev("sales_amount"), 2).alias("std_sales"),
                   spark_sum("quantity").alias("total_quantity"),
                   count("*").alias("sales_count")
               ) \
               .orderBy(desc("total_sales"))

    return result


def exercise_6b(spark: SparkSession, employee_data: List[tuple]):
    """Use window functions for ranking and comparative analysis."""
    # Create DataFrame
    df = spark.createDataFrame(employee_data, ["name", "department", "salary", "years_experience"])

    # Define window specifications
    window_overall = Window.orderBy(desc("salary"))
    window_dept = Window.partitionBy("department").orderBy(desc("salary"))
    window_exp_dept = Window.partitionBy("department").orderBy(desc("years_experience"))

    # Apply window functions
    result = df.withColumn("salary_rank_overall", rank().over(window_overall)) \
               .withColumn("salary_rank_dept", rank().over(window_dept)) \
               .withColumn("experience_rank_dept", rank().over(window_exp_dept)) \
               .withColumn("salary_percentile", percent_rank().over(window_overall)) \
               .orderBy("department", desc("salary"))

    return result


def exercise_6c(spark: SparkSession, sales_data: List[tuple]):
    """Create a pivot table analysis."""
    # Create DataFrame
    df = spark.createDataFrame(sales_data, ["month", "region", "product", "sales_amount"])

    # Create pivot table
    pivot_result = df.groupBy("month") \
                     .pivot("region") \
                     .sum("sales_amount") \
                     .fillna(0)

    # Add total_sales column (sum across all regions)
    region_columns = [col_name for col_name in pivot_result.columns if col_name != "month"]
    total_expr = sum([col(region_col) for region_col in region_columns])

    result = pivot_result.withColumn("total_sales", total_expr) \
                        .orderBy("month")

    return result


def exercise_6d(spark: SparkSession, customer_orders: List[tuple]):
    """Perform customer cohort analysis using window functions."""
    # Create DataFrame
    df = spark.createDataFrame(customer_orders, ["customer_id", "order_date", "order_amount"])

    # Convert order_date to date type
    df = df.withColumn("order_date", col("order_date").cast(DateType()))

    # Define window specifications
    customer_window = Window.partitionBy("customer_id").orderBy("order_date")
    customer_window_unbounded = Window.partitionBy("customer_id").orderBy("order_date") \
                                     .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Apply cohort analysis
    result = df.withColumn("order_number", row_number().over(customer_window)) \
               .withColumn("first_order_date", first("order_date").over(customer_window_unbounded)) \
               .withColumn("days_since_first_order",
                          datediff(col("order_date"), col("first_order_date"))) \
               .withColumn("cumulative_spending",
                          spark_sum("order_amount").over(customer_window_unbounded)) \
               .withColumn("previous_order_amount", lag("order_amount", 1).over(customer_window)) \
               .orderBy("customer_id", "order_date")

    return result


def exercise_6e(spark: SparkSession, product_reviews: List[tuple]):
    """Analyze product reviews with advanced aggregations."""
    # Create DataFrame
    df = spark.createDataFrame(product_reviews, ["product_id", "rating", "review_text", "helpful_votes"])

    # Advanced review analysis
    result = df.groupBy("product_id") \
               .agg(
                   count("*").alias("review_count"),
                   spark_round(avg("rating"), 1).alias("avg_rating"),
                   spark_round(stddev("rating"), 2).alias("rating_stddev"),
                   spark_sum("helpful_votes").alias("total_helpful_votes"),
                   collect_list("rating").alias("rating_distribution"),
                   spark_max("rating").alias("top_rating"),
                   spark_sum(when(col("rating") <= 2, 1).otherwise(0)).alias("low_rating_count"),
                   spark_sum(when(col("rating") >= 4, 1).otherwise(0)).alias("high_rating_count")
               ) \
               .orderBy(desc("avg_rating"), desc("review_count"))

    return result


def exercise_6f(spark: SparkSession, time_series_data: List[tuple]):
    """Perform time series analysis with moving averages and trends."""
    # Create DataFrame
    df = spark.createDataFrame(time_series_data, ["date", "metric_value"])

    # Convert date column to date type
    df = df.withColumn("date", col("date").cast(DateType()))

    # Define window specifications for moving averages
    window_3day = Window.orderBy("date").rowsBetween(-2, 0)  # Current + 2 previous
    window_7day = Window.orderBy("date").rowsBetween(-6, 0)  # Current + 6 previous
    window_prev = Window.orderBy("date")
    window_all = Window.orderBy(desc("metric_value"))
    window_cumulative = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

    # Time series analysis
    result = df.withColumn("moving_avg_3", spark_round(avg("metric_value").over(window_3day), 2)) \
               .withColumn("moving_avg_7", spark_round(avg("metric_value").over(window_7day), 2)) \
               .withColumn("value_vs_prev",
                          col("metric_value") - lag("metric_value", 1).over(window_prev)) \
               .withColumn("trend_3day",
                          when(col("metric_value") > col("moving_avg_3"), "Up")
                          .when(col("metric_value") < col("moving_avg_3"), "Down")
                          .otherwise("Stable")) \
               .withColumn("cumulative_sum", spark_sum("metric_value").over(window_cumulative)) \
               .withColumn("rank_by_value", rank().over(window_all)) \
               .orderBy("date")

    return result


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 6 Solutions")
    print("=" * 30)

    # Solution 6a
    print("\n6a. Statistical Analysis")
    sales_data = [
        ("Electronics", "North", 1500.00, 3),
        ("Electronics", "South", 2200.00, 4),
        ("Clothing", "North", 800.00, 8),
        ("Clothing", "South", 1200.00, 12),
        ("Electronics", "East", 1800.00, 2),
        ("Books", "North", 300.00, 15),
        ("Books", "South", 450.00, 20)
    ]
    result_6a = exercise_6a(spark, sales_data)
    result_6a.show()

    # Solution 6b
    print("\n6b. Window Functions and Ranking")
    employee_data = [
        ("Alice", "Engineering", 95000, 8),
        ("Bob", "Engineering", 87000, 5),
        ("Charlie", "Marketing", 75000, 6),
        ("Diana", "Marketing", 82000, 7),
        ("Eve", "Engineering", 92000, 4),
        ("Frank", "Sales", 68000, 3)
    ]
    result_6b = exercise_6b(spark, employee_data)
    result_6b.show()

    # Solution 6c
    print("\n6c. Pivot Table Analysis")
    sales_data = [
        ("Jan", "North", "Product A", 1000),
        ("Jan", "South", "Product A", 1500),
        ("Feb", "North", "Product A", 1200),
        ("Feb", "South", "Product A", 1300),
        ("Jan", "East", "Product A", 800),
        ("Feb", "East", "Product A", 900)
    ]
    result_6c = exercise_6c(spark, sales_data)
    result_6c.show()

    # Solution 6d
    print("\n6d. Customer Cohort Analysis")
    customer_orders = [
        (1, "2023-01-15", 100.00),
        (1, "2023-02-20", 150.00),
        (1, "2023-03-10", 200.00),
        (2, "2023-01-20", 75.00),
        (2, "2023-03-15", 125.00),
        (3, "2023-02-01", 300.00)
    ]
    result_6d = exercise_6d(spark, customer_orders)
    result_6d.show()

    # Solution 6e
    print("\n6e. Product Review Analysis")
    product_reviews = [
        ("PROD001", 5, "Great product!", 10),
        ("PROD001", 4, "Good quality", 5),
        ("PROD001", 2, "Poor quality", 2),
        ("PROD002", 5, "Excellent", 8),
        ("PROD002", 5, "Love it", 6),
        ("PROD003", 3, "Average", 3),
        ("PROD003", 1, "Terrible", 1)
    ]
    result_6e = exercise_6e(spark, product_reviews)
    result_6e.show(truncate=False)

    # Solution 6f
    print("\n6f. Time Series Analysis")
    time_series_data = [
        ("2023-01-01", 100),
        ("2023-01-02", 110),
        ("2023-01-03", 105),
        ("2023-01-04", 120),
        ("2023-01-05", 115),
        ("2023-01-06", 130),
        ("2023-01-07", 125),
        ("2023-01-08", 140)
    ]
    result_6f = exercise_6f(spark, time_series_data)
    result_6f.show()

    spark.stop()
    print("\n🎓 All Exercise 6 solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()