"""
Exercise 2: Basic Operations and Transformations
================================================

This exercise focuses on DataFrame operations like select, filter, groupBy,
and working with different column functions and transformations.

Instructions:
1. Complete each function according to the docstring requirements
2. Run the script to test your implementations
3. Focus on using DataFrame API methods

Learning Goals:
- Master basic DataFrame operations (select, filter, orderBy)
- Work with column functions (string, numeric, conditional)
- Perform groupBy operations and aggregations
- Handle data transformations and calculations

Run this file: python exercise_2.py
"""

from pyspark.sql import SparkSession
from typing import List


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise2-BasicOperations")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_2a(spark: SparkSession, products_data: List[tuple]):
    """
    Create a DataFrame from products data and perform basic operations.

    Args:
        spark: SparkSession instance
        products_data: List of tuples (product_name, category, price, rating)

    Returns:
        DataFrame with products where price > 50, ordered by rating descending,
        showing only product_name, price, and rating columns

    Requirements:
    - Filter products with price > 50
    - Select only product_name, price, and rating columns
    - Order by rating in descending order
    """
    # TODO: Implement this function
    pass


def exercise_2b(spark: SparkSession, df):
    """
    Add string transformation columns to the DataFrame.

    Args:
        spark: SparkSession instance
        df: DataFrame with 'name' column

    Returns:
        DataFrame with additional columns:
        - name_upper: name in uppercase
        - name_lower: name in lowercase
        - name_length: length of the name
        - first_char: first character of the name

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use upper(), lower(), length(), and substring() functions
    pass


def exercise_2c(spark: SparkSession, df):
    """
    Create conditional columns based on existing data.

    Args:
        spark: SparkSession instance
        df: DataFrame with columns: name, age, salary

    Returns:
        DataFrame with additional columns:
        - salary_level: "High" if salary >= 80000, "Medium" if >= 60000, "Low" otherwise
        - generation: "Millennial" if age <= 35, "Gen X" if <= 50, "Boomer" otherwise
        - bonus: 10% of salary if salary_level is "High", 5% if "Medium", 0 otherwise

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use when().when().otherwise() for multiple conditions
    pass


def exercise_2d(spark: SparkSession, sales_data: List[tuple]):
    """
    Analyze sales data with groupBy operations.

    Args:
        spark: SparkSession instance
        sales_data: List of tuples (region, product, quantity, unit_price)

    Returns:
        DataFrame with sales analysis by region:
        - region: region name
        - total_products: count of products sold
        - total_quantity: sum of quantities
        - total_revenue: sum of (quantity * unit_price)
        - avg_unit_price: average unit price

    Results should be ordered by total_revenue descending.
    """
    # TODO: Implement this function
    # 1. Create DataFrame from sales_data
    # 2. Calculate revenue as quantity * unit_price
    # 3. Group by region and calculate aggregations
    # 4. Order by total_revenue descending
    pass


def exercise_2e(spark: SparkSession, df):
    """
    Perform numeric operations and rounding.

    Args:
        spark: SparkSession instance
        df: DataFrame with numeric columns: price, tax_rate

    Returns:
        DataFrame with additional columns:
        - tax_amount: price * tax_rate (rounded to 2 decimal places)
        - total_price: price + tax_amount (rounded to 2 decimal places)
        - price_category: "Expensive" if total_price > 100, "Moderate" if > 50, "Cheap" otherwise

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use round() function and conditional logic
    pass


def exercise_2f(spark: SparkSession, employees_df, departments_df):
    """
    Perform a simple join between employees and departments.

    Args:
        spark: SparkSession instance
        employees_df: DataFrame with columns: name, dept_id, salary
        departments_df: DataFrame with columns: dept_id, dept_name, location

    Returns:
        DataFrame with employee information joined with department details:
        - name: employee name
        - salary: employee salary
        - dept_name: department name
        - location: department location

    Only include employees who have a matching department.
    """
    # TODO: Implement this function
    # Perform inner join on dept_id and select required columns
    pass


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 2: Basic Operations and Transformations")
    print("=" * 60)

    # Test Exercise 2a
    print("\n📝 Exercise 2a: Filter and Order Products")
    try:
        products_data = [
            ("Laptop", "Electronics", 999.99, 4.5),
            ("Book", "Education", 29.99, 4.8),
            ("Phone", "Electronics", 699.99, 4.2),
            ("Pen", "Office", 2.99, 3.9),
            ("Tablet", "Electronics", 399.99, 4.6),
        ]
        result_2a = exercise_2a(spark, products_data)

        if result_2a is not None:
            print("Filtered and ordered products:")
            result_2a.show()
            print(f"Row count: {result_2a.count()}")
            print(f"Columns: {result_2a.columns}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2b
    print("\n📝 Exercise 2b: String Transformations")
    try:
        test_data = [("Alice",), ("Bob",), ("Charlie",)]
        test_df = spark.createDataFrame(test_data, ["name"])

        result_2b = exercise_2b(spark, test_df)

        if result_2b is not None:
            print("DataFrame with string transformations:")
            result_2b.show()
            expected_cols = [
                "name",
                "name_upper",
                "name_lower",
                "name_length",
                "first_char",
            ]
            actual_cols = result_2b.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2c
    print("\n📝 Exercise 2c: Conditional Columns")
    try:
        test_data = [("Alice", 28, 85000), ("Bob", 45, 65000), ("Charlie", 55, 45000)]
        test_df = spark.createDataFrame(test_data, ["name", "age", "salary"])

        result_2c = exercise_2c(spark, test_df)

        if result_2c is not None:
            print("DataFrame with conditional columns:")
            result_2c.show()
            expected_cols = [
                "name",
                "age",
                "salary",
                "salary_level",
                "generation",
                "bonus",
            ]
            actual_cols = result_2c.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2d
    print("\n📝 Exercise 2d: Sales Analysis with GroupBy")
    try:
        sales_data = [
            ("North", "Laptop", 10, 999.99),
            ("North", "Phone", 15, 699.99),
            ("South", "Laptop", 8, 999.99),
            ("South", "Tablet", 12, 399.99),
            ("East", "Phone", 20, 699.99),
        ]

        result_2d = exercise_2d(spark, sales_data)

        if result_2d is not None:
            print("Sales analysis by region:")
            result_2d.show()
            expected_cols = [
                "region",
                "total_products",
                "total_quantity",
                "total_revenue",
                "avg_unit_price",
            ]
            actual_cols = result_2d.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2e
    print("\n📝 Exercise 2e: Numeric Operations and Rounding")
    try:
        test_data = [(99.99, 0.08), (45.50, 0.06), (150.00, 0.10)]
        test_df = spark.createDataFrame(test_data, ["price", "tax_rate"])

        result_2e = exercise_2e(spark, test_df)

        if result_2e is not None:
            print("DataFrame with numeric calculations:")
            result_2e.show()
            expected_cols = [
                "price",
                "tax_rate",
                "tax_amount",
                "total_price",
                "price_category",
            ]
            actual_cols = result_2e.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2f
    print("\n📝 Exercise 2f: Simple Join Operations")
    try:
        employees_data = [("Alice", 1, 80000), ("Bob", 2, 75000), ("Charlie", 1, 85000)]
        employees_df = spark.createDataFrame(
            employees_data, ["name", "dept_id", "salary"]
        )

        departments_data = [
            (1, "Engineering", "Building A"),
            (2, "Marketing", "Building B"),
            (3, "Sales", "Building C"),
        ]
        departments_df = spark.createDataFrame(
            departments_data, ["dept_id", "dept_name", "location"]
        )

        result_2f = exercise_2f(spark, employees_df, departments_df)

        if result_2f is not None:
            print("Employee-Department join result:")
            result_2f.show()
            expected_cols = ["name", "salary", "dept_name", "location"]
            actual_cols = result_2f.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 2 completed!")
    print("💡 Great job working with DataFrame operations!")
    print("📚 Ready to move on to Exercise 3!")


if __name__ == "__main__":
    run_exercises()
