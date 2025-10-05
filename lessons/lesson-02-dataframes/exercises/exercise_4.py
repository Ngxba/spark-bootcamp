"""
Exercise 4: Joins and Data Combining
====================================

This exercise focuses on different types of joins and combining DataFrames
using various join strategies and union operations.

Instructions:
1. Complete each function according to the docstring requirements
2. Focus on understanding different join types and their results
3. Practice combining data from multiple sources

Learning Goals:
- Master different join types (inner, left, right, full outer)
- Understand when to use each join type
- Work with union operations
- Handle null values in joined data

Run this file: python exercise_4.py
"""

from typing import List

from pyspark.sql import SparkSession


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise4-JoinsAndCombining")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_4a(
    spark: SparkSession, customers_data: List[tuple], orders_data: List[tuple]
):
    """
    Perform an inner join between customers and orders.

    Args:
        spark: SparkSession instance
        customers_data: List of tuples (customer_id, customer_name, city)
        orders_data: List of tuples (order_id, customer_id, order_amount)

    Returns:
        DataFrame with inner join result showing:
        - customer_name
        - city
        - order_id
        - order_amount

    Only customers with orders should be included.
    """
    # TODO: Implement this function
    # 1. Create DataFrames from both datasets
    # 2. Perform inner join on customer_id
    # 3. Select and return required columns


def exercise_4b(
    spark: SparkSession, employees_data: List[tuple], departments_data: List[tuple]
):
    """
    Perform a left join to show all employees with their department info.

    Args:
        spark: SparkSession instance
        employees_data: List of tuples (emp_id, emp_name, dept_id, salary)
        departments_data: List of tuples (dept_id, dept_name, manager)

    Returns:
        DataFrame with left join result showing:
        - emp_name
        - salary
        - dept_name (may be null for employees without departments)
        - manager (may be null for employees without departments)

    All employees should be included, even if they don't have a department.
    """
    # TODO: Implement this function
    # 1. Create DataFrames from both datasets
    # 2. Perform left join on dept_id
    # 3. Select and return required columns


def exercise_4c(
    spark: SparkSession, products_data: List[tuple], inventory_data: List[tuple]
):
    """
    Perform a right join to show all inventory items with product details.

    Args:
        spark: SparkSession instance
        products_data: List of tuples (product_id, product_name, category)
        inventory_data: List of tuples (product_id, warehouse, quantity)

    Returns:
        DataFrame with right join result showing:
        - product_name (may be null for inventory without product details)
        - category (may be null)
        - warehouse
        - quantity

    All inventory items should be included, even if product details are missing.
    """
    # TODO: Implement this function
    # 1. Create DataFrames from both datasets
    # 2. Perform right join on product_id
    # 3. Select and return required columns


def exercise_4d(
    spark: SparkSession, students_data: List[tuple], courses_data: List[tuple]
):
    """
    Perform a full outer join between students and courses.

    Args:
        spark: SparkSession instance
        students_data: List of tuples (student_id, student_name, major)
        courses_data: List of tuples (student_id, course_name, grade)

    Returns:
        DataFrame with full outer join result showing:
        - student_name (may be null)
        - major (may be null)
        - course_name (may be null)
        - grade (may be null)

    Include all students and all course records, whether they match or not.
    """
    # TODO: Implement this function
    # 1. Create DataFrames from both datasets
    # 2. Perform full outer join on student_id
    # 3. Select and return required columns


def exercise_4e(spark: SparkSession, df_joined):
    """
    Handle null values in a joined DataFrame.

    Args:
        spark: SparkSession instance
        df_joined: DataFrame with potential null values

    Returns:
        DataFrame with null values handled:
        - Fill null string columns with "Unknown"
        - Fill null numeric columns with 0
        - Add a column 'has_missing_data' (True if any original nulls, False otherwise)

    Note: You should check for nulls before filling them.
    """
    # TODO: Implement this function
    # 1. Create a column indicating if row has missing data
    # 2. Fill null values appropriately
    # 3. Return the cleaned DataFrame


def exercise_4f(
    spark: SparkSession, current_employees: List[tuple], new_hires: List[tuple]
):
    """
    Combine current employees with new hires using union operation.

    Args:
        spark: SparkSession instance
        current_employees: List of tuples (emp_id, name, department, start_date)
        new_hires: List of tuples (emp_id, name, department, start_date)

    Returns:
        DataFrame with all employees (current + new hires):
        - emp_id
        - name
        - department
        - start_date
        - employee_type: "Current" for current employees, "New Hire" for new hires

    Remove any duplicate employees (same emp_id).
    """
    # TODO: Implement this function
    # 1. Create DataFrames and add employee_type column to each
    # 2. Union the DataFrames
    # 3. Remove duplicates based on emp_id (keep first occurrence)
    # 4. Return the combined DataFrame


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 4: Joins and Data Combining")
    print("=" * 50)

    # Test Exercise 4a
    print("\n📝 Exercise 4a: Inner Join (Customers and Orders)")
    try:
        customers_data = [
            (1, "Alice Johnson", "New York"),
            (2, "Bob Smith", "Los Angeles"),
            (3, "Charlie Brown", "Chicago"),
            (4, "Diana Ross", "Miami"),
        ]

        orders_data = [
            (101, 1, 150.00),
            (102, 2, 200.00),
            (103, 1, 75.50),
            (104, 5, 300.00),  # Customer 5 doesn't exist
        ]

        result_4a = exercise_4a(spark, customers_data, orders_data)

        if result_4a is not None:
            print("Inner join result (customers with orders):")
            result_4a.show()
            print(f"Row count: {result_4a.count()}")
            expected_cols = ["customer_name", "city", "order_id", "order_amount"]
            actual_cols = result_4a.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4b
    print("\n📝 Exercise 4b: Left Join (All Employees with Departments)")
    try:
        employees_data = [
            (1, "Alice", 10, 80000),
            (2, "Bob", 20, 75000),
            (3, "Charlie", 10, 85000),
            (4, "Diana", None, 70000),  # No department
        ]

        departments_data = [
            (10, "Engineering", "John Doe"),
            (20, "Marketing", "Jane Smith"),
            (30, "Sales", "Mike Johnson"),  # No employees in this dept
        ]

        result_4b = exercise_4b(spark, employees_data, departments_data)

        if result_4b is not None:
            print("Left join result (all employees):")
            result_4b.show()
            print(f"Row count: {result_4b.count()}")
            expected_cols = ["emp_name", "salary", "dept_name", "manager"]
            actual_cols = result_4b.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4c
    print("\n📝 Exercise 4c: Right Join (All Inventory with Products)")
    try:
        products_data = [
            (101, "Laptop", "Electronics"),
            (102, "Book", "Education"),
            (103, "Phone", "Electronics"),
        ]

        inventory_data = [
            (101, "Warehouse A", 50),
            (102, "Warehouse B", 100),
            (104, "Warehouse A", 25),  # Product 104 doesn't exist
        ]

        result_4c = exercise_4c(spark, products_data, inventory_data)

        if result_4c is not None:
            print("Right join result (all inventory):")
            result_4c.show()
            print(f"Row count: {result_4c.count()}")
            expected_cols = ["product_name", "category", "warehouse", "quantity"]
            actual_cols = result_4c.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4d
    print("\n📝 Exercise 4d: Full Outer Join (Students and Courses)")
    try:
        students_data = [
            (1, "Alice", "Computer Science"),
            (2, "Bob", "Mathematics"),
            (3, "Charlie", "Physics"),
        ]

        courses_data = [
            (1, "Data Structures", "A"),
            (2, "Calculus", "B"),
            (4, "Chemistry", "A"),  # Student 4 doesn't exist
        ]

        result_4d = exercise_4d(spark, students_data, courses_data)

        if result_4d is not None:
            print("Full outer join result (all students and courses):")
            result_4d.show()
            print(f"Row count: {result_4d.count()}")
            expected_cols = ["student_name", "major", "course_name", "grade"]
            actual_cols = result_4d.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4e
    print("\n📝 Exercise 4e: Handle Null Values")
    try:
        # Use result from previous exercise which should have nulls
        if "result_4d" in locals() and result_4d is not None:
            result_4e = exercise_4e(spark, result_4d)

            if result_4e is not None:
                print("DataFrame with null values handled:")
                result_4e.show()
                if "has_missing_data" in result_4e.columns:
                    print("✅ Missing data indicator column added!")
                print("✅ Function executed successfully!")
            else:
                print("❌ Function returned None")
        else:
            print("❌ No input DataFrame available (exercise_4d failed)")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4f
    print("\n📝 Exercise 4f: Union Operations")
    try:
        current_employees = [
            (1, "Alice", "Engineering", "2022-01-15"),
            (2, "Bob", "Marketing", "2022-03-20"),
            (3, "Charlie", "Sales", "2021-11-10"),
        ]

        new_hires = [
            (4, "Diana", "Engineering", "2023-06-01"),
            (5, "Eve", "Marketing", "2023-06-15"),
            (2, "Bob", "Marketing", "2022-03-20"),  # Duplicate
        ]

        result_4f = exercise_4f(spark, current_employees, new_hires)

        if result_4f is not None:
            print("Combined employees (union with employee type):")
            result_4f.show()
            print(
                f"Row count: {result_4f.count()} (should be 5 after removing duplicates)"
            )
            expected_cols = [
                "emp_id",
                "name",
                "department",
                "start_date",
                "employee_type",
            ]
            actual_cols = result_4f.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 4 completed!")
    print("💡 Excellent work with joins and data combining!")
    print("📚 You've mastered the different join types and union operations!")
    print("🔄 Ready to move on to Exercise 5!")


if __name__ == "__main__":
    run_exercises()
