"""
Exercise 3: SQL Queries with Temporary Views
============================================

This exercise focuses on using Spark SQL with temporary views to perform
data analysis using SQL syntax.

Instructions:
1. Complete each function according to the docstring requirements
2. Use SQL queries with spark.sql() method
3. Create temporary views when needed

Learning Goals:
- Create and use temporary views
- Write SQL queries for data analysis
- Use SQL functions and aggregations
- Combine SQL with DataFrame operations

Run this file: python exercise_3.py
"""

from pyspark.sql import SparkSession
from typing import List


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise3-SQLQueries")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_3a(spark: SparkSession, employees_data: List[tuple]):
    """
    Create a temporary view and write a simple SQL query.

    Args:
        spark: SparkSession instance
        employees_data: List of tuples (name, age, department, salary)

    Returns:
        DataFrame with employees older than 30, ordered by salary descending

    Requirements:
    - Create DataFrame from employees_data
    - Create temporary view named 'employees'
    - Write SQL query to select employees with age > 30
    - Order by salary descending
    """
    # TODO: Implement this function
    # 1. Create DataFrame from employees_data with appropriate column names
    # 2. Create temporary view using createOrReplaceTempView()
    # 3. Write SQL query using spark.sql()
    # 4. Return the result DataFrame
    pass


def exercise_3b(spark: SparkSession, df):
    """
    Write SQL query with aggregations.

    Args:
        spark: SparkSession instance
        df: DataFrame with employee data (should have 'employees' temp view)

    Returns:
        DataFrame with department statistics:
        - department
        - employee_count
        - avg_salary (rounded to 2 decimal places)
        - max_salary
        - min_salary

    Order by avg_salary descending.
    """
    # TODO: Implement this function
    # Write SQL query with GROUP BY and aggregation functions
    # Use ROUND() for avg_salary
    pass


def exercise_3c(
    spark: SparkSession, sales_data: List[tuple], products_data: List[tuple]
):
    """
    Write SQL query with JOIN operations.

    Args:
        spark: SparkSession instance
        sales_data: List of tuples (sale_id, product_id, quantity, sale_date)
        products_data: List of tuples (product_id, product_name, category, unit_price)

    Returns:
        DataFrame with sales information joined with product details:
        - product_name
        - category
        - quantity
        - unit_price
        - total_amount (quantity * unit_price)

    Requirements:
    - Create temporary views for both datasets
    - Use INNER JOIN
    - Calculate total_amount in SQL
    - Order by total_amount descending
    """
    # TODO: Implement this function
    # 1. Create DataFrames and temporary views for both datasets
    # 2. Write SQL query with INNER JOIN
    # 3. Calculate total_amount in the SELECT clause
    # 4. Order by total_amount descending
    pass


def exercise_3d(spark: SparkSession):
    """
    Write SQL query with WHERE conditions and CASE statements.

    Assumes 'employees' temporary view exists with columns: name, age, salary, department

    Returns:
        DataFrame with:
        - name
        - age
        - salary
        - salary_category: 'High' if salary >= 80000, 'Medium' if >= 60000, 'Low' otherwise
        - age_group: 'Young' if age < 30, 'Middle' if < 45, 'Senior' otherwise

    Filter for employees with salary > 50000.
    """
    # TODO: Implement this function
    # Write SQL query using CASE WHEN statements for categories
    # Include WHERE clause for salary filter
    pass


def exercise_3e(spark: SparkSession, orders_data: List[tuple]):
    """
    Write SQL query with date functions and string operations.

    Args:
        spark: SparkSession instance
        orders_data: List of tuples (order_id, customer_name, order_date, amount)

    Returns:
        DataFrame with:
        - customer_name (uppercase)
        - order_month (extracted from order_date)
        - order_year (extracted from order_date)
        - amount
        - name_length (length of customer_name)

    Filter for orders in 2023 only.
    """
    # TODO: Implement this function
    # 1. Create DataFrame and temporary view
    # 2. Use UPPER(), MONTH(), YEAR(), LENGTH() functions
    # 3. Filter for year 2023
    pass


def exercise_3f(spark: SparkSession):
    """
    Write SQL query with subqueries.

    Assumes 'employees' temporary view exists.

    Returns:
        DataFrame with employees who earn more than the average salary:
        - name
        - salary
        - salary_diff (difference from average salary)

    Requirements:
    - Use subquery to calculate average salary
    - Show difference from average
    - Order by salary_diff descending
    """
    # TODO: Implement this function
    # Use subquery in WHERE clause and SELECT clause
    pass


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 3: SQL Queries with Temporary Views")
    print("=" * 55)

    # Test Exercise 3a
    print("\n📝 Exercise 3a: Simple SQL Query with Temporary View")
    try:
        employees_data = [
            ("Alice", 28, "Engineering", 85000),
            ("Bob", 34, "Marketing", 75000),
            ("Charlie", 29, "Engineering", 90000),
            ("Diana", 31, "Sales", 70000),
            ("Eve", 26, "Engineering", 88000),
        ]

        result_3a = exercise_3a(spark, employees_data)

        if result_3a is not None:
            print("Employees older than 30, ordered by salary:")
            result_3a.show()
            print(f"Row count: {result_3a.count()}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3b
    print("\n📝 Exercise 3b: SQL Aggregations")
    try:
        # Use the same employees data, should have temp view from 3a
        result_3b = exercise_3b(spark, None)

        if result_3b is not None:
            print("Department statistics:")
            result_3b.show()
            expected_cols = [
                "department",
                "employee_count",
                "avg_salary",
                "max_salary",
                "min_salary",
            ]
            actual_cols = result_3b.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3c
    print("\n📝 Exercise 3c: SQL JOIN Operations")
    try:
        sales_data = [
            (1, 101, 2, "2023-01-15"),
            (2, 102, 1, "2023-01-16"),
            (3, 101, 3, "2023-01-17"),
            (4, 103, 1, "2023-01-18"),
        ]

        products_data = [
            (101, "Laptop", "Electronics", 999.99),
            (102, "Book", "Education", 29.99),
            (103, "Phone", "Electronics", 699.99),
        ]

        result_3c = exercise_3c(spark, sales_data, products_data)

        if result_3c is not None:
            print("Sales with product information:")
            result_3c.show()
            expected_cols = [
                "product_name",
                "category",
                "quantity",
                "unit_price",
                "total_amount",
            ]
            actual_cols = result_3c.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3d
    print("\n📝 Exercise 3d: CASE Statements and WHERE Conditions")
    try:
        result_3d = exercise_3d(spark)

        if result_3d is not None:
            print("Employees with categories (salary > 50000):")
            result_3d.show()
            expected_cols = ["name", "age", "salary", "salary_category", "age_group"]
            actual_cols = result_3d.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3e
    print("\n📝 Exercise 3e: Date Functions and String Operations")
    try:
        orders_data = [
            ("ORD001", "Alice Johnson", "2023-03-15", 150.00),
            ("ORD002", "Bob Smith", "2023-06-20", 75.50),
            ("ORD003", "Charlie Brown", "2022-12-10", 200.00),
            ("ORD004", "Diana Ross", "2023-09-05", 99.99),
        ]

        result_3e = exercise_3e(spark, orders_data)

        if result_3e is not None:
            print("Orders with date/string functions (2023 only):")
            result_3e.show()
            expected_cols = [
                "customer_name",
                "order_month",
                "order_year",
                "amount",
                "name_length",
            ]
            actual_cols = result_3e.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3f
    print("\n📝 Exercise 3f: SQL Subqueries")
    try:
        result_3f = exercise_3f(spark)

        if result_3f is not None:
            print("Employees earning above average:")
            result_3f.show()
            expected_cols = ["name", "salary", "salary_diff"]
            actual_cols = result_3f.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 3 completed!")
    print("💡 Excellent work with SQL queries and temporary views!")
    print("📚 Ready to move on to Exercise 4!")


if __name__ == "__main__":
    run_exercises()
