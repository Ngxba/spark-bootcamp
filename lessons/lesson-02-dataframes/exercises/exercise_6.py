"""
Exercise 6: Analytics and Aggregations
======================================

This exercise focuses on advanced analytics, aggregations, and statistical
operations using DataFrames. You'll work with window functions, pivot tables,
and complex analytical queries.

Instructions:
1. Complete each function according to the docstring requirements
2. Use advanced aggregation functions and window operations
3. Focus on business analytics patterns

Learning Goals:
- Master advanced aggregation functions
- Use window functions for analytics
- Create pivot tables and cross-tabulations
- Perform statistical analysis and ranking
- Build analytical reports

Run this file: python exercise_6.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, variance, collect_list, collect_set,
    row_number, rank, dense_rank, lag, lead,
    percentile_approx, desc, asc
)
from pyspark.sql.window import Window
from typing import List, Tuple


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise6-Analytics") \
        .master("local[*]") \
        .getOrCreate()


def exercise_6a(spark: SparkSession, sales_data: List[tuple]):
    """
    Perform basic statistical analysis on sales data.

    Args:
        spark: SparkSession instance
        sales_data: List of tuples (product_category, region, sales_amount, quantity)

    Returns:
        DataFrame with statistical summary by product_category:
        - product_category
        - total_sales: sum of sales_amount
        - avg_sales: average sales_amount (rounded to 2 decimals)
        - max_sales: maximum sales_amount
        - min_sales: minimum sales_amount
        - std_sales: standard deviation of sales_amount (rounded to 2 decimals)
        - total_quantity: sum of quantity
        - sales_count: number of sales records

    Order by total_sales descending.
    """
    # TODO: Implement this function
    # Use groupBy with multiple aggregation functions
    pass


def exercise_6b(spark: SparkSession, employee_data: List[tuple]):
    """
    Use window functions for ranking and comparative analysis.

    Args:
        spark: SparkSession instance
        employee_data: List of tuples (name, department, salary, years_experience)

    Returns:
        DataFrame with ranking columns:
        - name, department, salary, years_experience (original columns)
        - salary_rank_overall: rank by salary across all employees (highest = 1)
        - salary_rank_dept: rank by salary within each department (highest = 1)
        - experience_rank_dept: rank by experience within each department (highest = 1)
        - salary_percentile: percentile rank of salary (0-1 scale)

    Order by department, then by salary descending.
    """
    # TODO: Implement this function
    # Use Window functions with PARTITION BY and ORDER BY
    # Use rank(), row_number(), and percent_rank() functions
    pass


def exercise_6c(spark: SparkSession, sales_data: List[tuple]):
    """
    Create a pivot table analysis.

    Args:
        spark: SparkSession instance
        sales_data: List of tuples (month, region, product, sales_amount)

    Returns:
        DataFrame pivoted by region showing sales_amount:
        - month: month value
        - [region columns]: one column for each region with sales amounts
        - total_sales: sum across all regions for each month

    Regions should be columns, months should be rows.
    """
    # TODO: Implement this function
    # Use groupBy().pivot().sum() for pivot table
    # Add total_sales column summing across all region columns
    pass


def exercise_6d(spark: SparkSession, customer_orders: List[tuple]):
    """
    Perform customer cohort analysis using window functions.

    Args:
        spark: SparkSession instance
        customer_orders: List of tuples (customer_id, order_date, order_amount)

    Returns:
        DataFrame with cohort analysis:
        - customer_id
        - order_date
        - order_amount
        - order_number: sequence number of orders for each customer (1, 2, 3...)
        - first_order_date: date of customer's first order
        - days_since_first_order: days between current order and first order
        - cumulative_spending: cumulative order_amount for each customer
        - previous_order_amount: amount of previous order (null for first order)

    Order by customer_id, then by order_date.
    """
    # TODO: Implement this function
    # Use window functions with row_number(), first(), lag(), sum()
    # Calculate date differences and cumulative values
    pass


def exercise_6e(spark: SparkSession, product_reviews: List[tuple]):
    """
    Analyze product reviews with advanced aggregations.

    Args:
        spark: SparkSession instance
        product_reviews: List of tuples (product_id, rating, review_text, helpful_votes)

    Returns:
        DataFrame with review analysis by product:
        - product_id
        - review_count: number of reviews
        - avg_rating: average rating (rounded to 1 decimal)
        - rating_stddev: standard deviation of ratings (rounded to 2 decimals)
        - total_helpful_votes: sum of helpful_votes
        - rating_distribution: list of all ratings (use collect_list)
        - top_rating: highest rating
        - low_rating_count: count of ratings <= 2
        - high_rating_count: count of ratings >= 4

    Order by avg_rating descending, then by review_count descending.
    """
    # TODO: Implement this function
    # Use complex aggregations with conditional counting
    pass


def exercise_6f(spark: SparkSession, time_series_data: List[tuple]):
    """
    Perform time series analysis with moving averages and trends.

    Args:
        spark: SparkSession instance
        time_series_data: List of tuples (date, metric_value)

    Returns:
        DataFrame with time series analysis:
        - date
        - metric_value
        - moving_avg_3: 3-day moving average (current + 2 previous days)
        - moving_avg_7: 7-day moving average (current + 6 previous days)
        - value_vs_prev: difference from previous day's value
        - trend_3day: "Up" if current > 3-day avg, "Down" if <, "Stable" if equal
        - cumulative_sum: running total of metric_value
        - rank_by_value: rank of this day's value among all days (highest = 1)

    Order by date ascending.
    """
    # TODO: Implement this function
    # Use window functions with frame specifications for moving averages
    # Use lag() for previous day comparisons
    pass


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 6: Analytics and Aggregations")
    print("=" * 50)

    # Test Exercise 6a
    print("\n📝 Exercise 6a: Statistical Analysis")
    try:
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

        if result_6a is not None:
            print("Statistical analysis by product category:")
            result_6a.show()
            expected_cols = ["product_category", "total_sales", "avg_sales", "max_sales", "min_sales", "std_sales", "total_quantity", "sales_count"]
            actual_cols = result_6a.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 6b
    print("\n📝 Exercise 6b: Window Functions and Ranking")
    try:
        employee_data = [
            ("Alice", "Engineering", 95000, 8),
            ("Bob", "Engineering", 87000, 5),
            ("Charlie", "Marketing", 75000, 6),
            ("Diana", "Marketing", 82000, 7),
            ("Eve", "Engineering", 92000, 4),
            ("Frank", "Sales", 68000, 3)
        ]
        
        result_6b = exercise_6b(spark, employee_data)
        
        if result_6b is not None:
            print("Employee ranking analysis:")
            result_6b.show()
            expected_cols = ["name", "department", "salary", "years_experience", "salary_rank_overall", "salary_rank_dept", "experience_rank_dept", "salary_percentile"]
            actual_cols = result_6b.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 6c
    print("\n📝 Exercise 6c: Pivot Table Analysis")
    try:
        sales_data = [
            ("Jan", "North", "Product A", 1000),
            ("Jan", "South", "Product A", 1500),
            ("Feb", "North", "Product A", 1200),
            ("Feb", "South", "Product A", 1300),
            ("Jan", "East", "Product A", 800),
            ("Feb", "East", "Product A", 900)
        ]
        
        result_6c = exercise_6c(spark, sales_data)
        
        if result_6c is not None:
            print("Pivot table (months vs regions):")
            result_6c.show()
            print(f"Columns: {result_6c.columns}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 6d
    print("\n📝 Exercise 6d: Customer Cohort Analysis")
    try:
        customer_orders = [
            (1, "2023-01-15", 100.00),
            (1, "2023-02-20", 150.00),
            (1, "2023-03-10", 200.00),
            (2, "2023-01-20", 75.00),
            (2, "2023-03-15", 125.00),
            (3, "2023-02-01", 300.00)
        ]
        
        result_6d = exercise_6d(spark, customer_orders)
        
        if result_6d is not None:
            print("Customer cohort analysis:")
            result_6d.show()
            expected_cols = ["customer_id", "order_date", "order_amount", "order_number", "first_order_date", "days_since_first_order", "cumulative_spending", "previous_order_amount"]
            actual_cols = result_6d.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 6e
    print("\n📝 Exercise 6e: Product Review Analysis")
    try:
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
        
        if result_6e is not None:
            print("Product review analysis:")
            result_6e.show(truncate=False)
            expected_cols = ["product_id", "review_count", "avg_rating", "rating_stddev", "total_helpful_votes", "rating_distribution", "top_rating", "low_rating_count", "high_rating_count"]
            actual_cols = result_6e.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 6f
    print("\n📝 Exercise 6f: Time Series Analysis")
    try:
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
        
        if result_6f is not None:
            print("Time series analysis:")
            result_6f.show()
            expected_cols = ["date", "metric_value", "moving_avg_3", "moving_avg_7", "value_vs_prev", "trend_3day", "cumulative_sum", "rank_by_value"]
            actual_cols = result_6f.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 6 completed!")
    print("💡 Fantastic work with analytics and aggregations!")
    print("📊 You've mastered window functions, pivot tables, and statistical analysis!")
    print("🚀 Ready for the final exercise - Performance Comparison!")


if __name__ == "__main__":
    run_exercises()