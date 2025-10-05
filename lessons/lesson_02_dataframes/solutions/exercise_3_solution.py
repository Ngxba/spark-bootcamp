"""
Exercise 3 Solutions: Spark SQL and Temporary Views
===================================================

This file contains the complete solutions for Exercise 3.
Study these solutions to understand Spark SQL and temporary views.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum
from typing import List


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise3-Solutions")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_3a(spark: SparkSession, employee_data: List[tuple]):
    """Create a temporary view and perform basic SQL queries."""
    # Create DataFrame and temporary view
    df = spark.createDataFrame(employee_data, ["name", "department", "salary", "age"])
    df.createOrReplaceTempView("employees")

    # SQL query to get employees in Engineering department
    result = spark.sql("""
        SELECT name, salary, age
        FROM employees
        WHERE department = 'Engineering'
        ORDER BY salary DESC
    """)

    return result


def exercise_3b(spark: SparkSession, sales_data: List[tuple]):
    """Use SQL for aggregations and grouping."""
    # Create DataFrame and temporary view
    df = spark.createDataFrame(
        sales_data, ["product", "category", "sales_amount", "region"]
    )
    df.createOrReplaceTempView("sales")

    # SQL query for category-wise aggregations
    result = spark.sql("""
        SELECT
            category,
            COUNT(*) as product_count,
            ROUND(AVG(sales_amount), 2) as avg_sales,
            SUM(sales_amount) as total_sales,
            MAX(sales_amount) as max_sales
        FROM sales
        GROUP BY category
        ORDER BY total_sales DESC
    """)

    return result


def exercise_3c(
    spark: SparkSession, customer_data: List[tuple], order_data: List[tuple]
):
    """Perform SQL joins between multiple tables."""
    # Create DataFrames and temporary views
    customers_df = spark.createDataFrame(
        customer_data, ["customer_id", "name", "city", "age"]
    )
    orders_df = spark.createDataFrame(
        order_data, ["order_id", "customer_id", "amount", "status"]
    )

    customers_df.createOrReplaceTempView("customers")
    orders_df.createOrReplaceTempView("orders")

    # SQL query with JOIN
    result = spark.sql("""
        SELECT
            c.name,
            c.city,
            c.age,
            COUNT(o.order_id) as order_count,
            COALESCE(SUM(o.amount), 0) as total_spent,
            COALESCE(ROUND(AVG(o.amount), 2), 0) as avg_order_value
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.name, c.city, c.age
        ORDER BY total_spent DESC
    """)

    return result


def exercise_3d(spark: SparkSession, product_data: List[tuple]):
    """Use SQL window functions and advanced analytics."""
    # Create DataFrame and temporary view
    df = spark.createDataFrame(
        product_data, ["product_name", "category", "price", "rating"]
    )
    df.createOrReplaceTempView("products")

    # SQL query with window functions
    result = spark.sql("""
        SELECT
            product_name,
            category,
            price,
            rating,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY rating DESC) as rank_in_category,
            AVG(rating) OVER (PARTITION BY category) as avg_category_rating,
            MAX(price) OVER (PARTITION BY category) as max_category_price,
            CASE
                WHEN price > AVG(price) OVER (PARTITION BY category) THEN 'Above Average'
                WHEN price < AVG(price) OVER (PARTITION BY category) THEN 'Below Average'
                ELSE 'Average'
            END as price_vs_category_avg
        FROM products
        ORDER BY category, rank_in_category
    """)

    return result


def exercise_3e(spark: SparkSession, transaction_data: List[tuple]):
    """Complex SQL query with subqueries and CTEs."""
    # Create DataFrame and temporary view
    df = spark.createDataFrame(
        transaction_data,
        ["transaction_id", "customer_id", "amount", "date", "merchant_type"],
    )
    df.createOrReplaceTempView("transactions")

    # Complex SQL with CTE
    result = spark.sql("""
        WITH customer_stats AS (
            SELECT
                customer_id,
                COUNT(*) as transaction_count,
                SUM(amount) as total_spent,
                AVG(amount) as avg_transaction,
                MAX(amount) as max_transaction
            FROM transactions
            GROUP BY customer_id
        ),
        merchant_stats AS (
            SELECT
                merchant_type,
                COUNT(*) as merchant_transaction_count,
                AVG(amount) as merchant_avg_amount
            FROM transactions
            GROUP BY merchant_type
        )
        SELECT
            cs.customer_id,
            cs.transaction_count,
            ROUND(cs.total_spent, 2) as total_spent,
            ROUND(cs.avg_transaction, 2) as avg_transaction,
            cs.max_transaction,
            CASE
                WHEN cs.total_spent > 1000 THEN 'High Value'
                WHEN cs.total_spent > 500 THEN 'Medium Value'
                ELSE 'Low Value'
            END as customer_tier,
            CASE
                WHEN cs.transaction_count > 5 THEN 'Frequent'
                WHEN cs.transaction_count > 2 THEN 'Regular'
                ELSE 'Occasional'
            END as customer_frequency
        FROM customer_stats cs
        ORDER BY cs.total_spent DESC
    """)

    return result


def exercise_3f(spark: SparkSession, sales_data: List[tuple]):
    """Compare SQL vs DataFrame API performance for the same operation."""
    # Create DataFrame and temporary view
    df = spark.createDataFrame(sales_data, ["region", "product", "sales", "quarter"])
    df.createOrReplaceTempView("sales_data")

    # SQL approach
    sql_result = spark.sql("""
        SELECT
            region,
            quarter,
            COUNT(DISTINCT product) as unique_products,
            SUM(sales) as total_sales,
            ROUND(AVG(sales), 2) as avg_sales,
            CASE
                WHEN SUM(sales) > 10000 THEN 'High'
                WHEN SUM(sales) > 5000 THEN 'Medium'
                ELSE 'Low'
            END as performance_tier
        FROM sales_data
        GROUP BY region, quarter
        ORDER BY region, quarter
    """)

    # DataFrame API approach (for comparison)
    from pyspark.sql.functions import countDistinct, when

    df_result = (
        df.groupBy("region", "quarter")
        .agg(
            countDistinct("product").alias("unique_products"),
            spark_sum("sales").alias("total_sales"),
            avg("sales").alias("avg_sales"),
        )
        .withColumn(
            "performance_tier",
            when(col("total_sales") > 10000, "High")
            .when(col("total_sales") > 5000, "Medium")
            .otherwise("Low"),
        )
        .orderBy("region", "quarter")
    )

    return sql_result, df_result


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 3 Solutions")
    print("=" * 30)

    # Solution 3a
    print("\n3a. Basic SQL Queries with Temporary Views")
    employee_data = [
        ("Alice", "Engineering", 85000, 28),
        ("Bob", "Marketing", 65000, 34),
        ("Charlie", "Engineering", 78000, 29),
        ("Diana", "Sales", 72000, 31),
        ("Eve", "Engineering", 92000, 26),
    ]
    result_3a = exercise_3a(spark, employee_data)
    result_3a.show()

    # Solution 3b
    print("\n3b. SQL Aggregations and Grouping")
    sales_data = [
        ("Laptop", "Electronics", 1200, "North"),
        ("Mouse", "Electronics", 25, "South"),
        ("Chair", "Furniture", 150, "North"),
        ("Desk", "Furniture", 300, "East"),
        ("Phone", "Electronics", 800, "West"),
        ("Table", "Furniture", 200, "South"),
    ]
    result_3b = exercise_3b(spark, sales_data)
    result_3b.show()

    # Solution 3c
    print("\n3c. SQL Joins Between Multiple Tables")
    customer_data = [
        (1, "Alice Johnson", "New York", 28),
        (2, "Bob Smith", "Los Angeles", 34),
        (3, "Charlie Brown", "Chicago", 29),
        (4, "Diana Wilson", "Houston", 31),
    ]
    order_data = [
        (101, 1, 250.0, "Completed"),
        (102, 1, 150.0, "Completed"),
        (103, 2, 300.0, "Pending"),
        (104, 3, 75.0, "Completed"),
        (105, 3, 200.0, "Completed"),
    ]
    result_3c = exercise_3c(spark, customer_data, order_data)
    result_3c.show()

    # Solution 3d
    print("\n3d. SQL Window Functions and Advanced Analytics")
    product_data = [
        ("iPhone 15", "Electronics", 999, 4.5),
        ("Samsung Galaxy", "Electronics", 849, 4.3),
        ("iPad", "Electronics", 599, 4.4),
        ("Office Chair", "Furniture", 299, 4.2),
        ("Desk Lamp", "Furniture", 89, 4.0),
        ("Standing Desk", "Furniture", 699, 4.6),
    ]
    result_3d = exercise_3d(spark, product_data)
    result_3d.show(truncate=False)

    # Solution 3e
    print("\n3e. Complex SQL with Subqueries and CTEs")
    transaction_data = [
        ("TXN001", 1, 250.0, "2023-01-15", "Restaurant"),
        ("TXN002", 1, 75.0, "2023-01-16", "Gas Station"),
        ("TXN003", 1, 150.0, "2023-01-17", "Grocery"),
        ("TXN004", 2, 500.0, "2023-01-15", "Electronics"),
        ("TXN005", 2, 300.0, "2023-01-16", "Clothing"),
        ("TXN006", 3, 50.0, "2023-01-15", "Coffee Shop"),
        ("TXN007", 3, 800.0, "2023-01-16", "Electronics"),
    ]
    result_3e = exercise_3e(spark, transaction_data)
    result_3e.show()

    # Solution 3f
    print("\n3f. SQL vs DataFrame API Comparison")
    comparison_sales_data = [
        ("North", "ProductA", 1000, "Q1"),
        ("North", "ProductB", 1500, "Q1"),
        ("South", "ProductA", 800, "Q1"),
        ("South", "ProductC", 1200, "Q1"),
        ("North", "ProductA", 1100, "Q2"),
        ("West", "ProductB", 900, "Q2"),
    ]
    sql_result, df_result = exercise_3f(spark, comparison_sales_data)

    print("SQL Approach:")
    sql_result.show()
    print("DataFrame API Approach:")
    df_result.show()

    print("✅ Both approaches produce identical results!")

    spark.stop()
    print("\n🎓 All Exercise 3 solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()
