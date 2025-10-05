"""
Exercise 1: DataFrame Creation and Basic Operations
==================================================

This exercise focuses on creating DataFrames from different sources and
performing basic operations like selection, filtering, and simple transformations.

Instructions:
1. Complete each function according to the docstring requirements
2. Run the script to test your implementations
3. Check your answers against the expected outputs

Learning Goals:
- Create DataFrames from various sources (lists, dictionaries, files)
- Understand schema inference and definition
- Perform basic DataFrame operations (select, filter, show)
- Work with column operations and basic transformations

Run this file: python exercise_1.py
"""

from pyspark.sql import SparkSession
from typing import List, Dict, Any
import os


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise1-DataFrameBasics")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_1a(spark: SparkSession, data: List[tuple], columns: List[str]):
    """
    Create a DataFrame from a list of tuples and column names.

    Args:
        spark: SparkSession instance
        data: List of tuples containing the data
        columns: List of column names

    Returns:
        DataFrame created from the data

    Example:
        Input: data=[("Alice", 25), ("Bob", 30)], columns=["name", "age"]
        Output: DataFrame with 2 rows and 2 columns
    """
    # TODO: Implement this function
    # Use spark.createDataFrame() with data and columns
    pass


def exercise_1b(spark: SparkSession, dict_data: List[Dict[str, Any]]):
    """
    Create a DataFrame from a list of dictionaries and return both
    the DataFrame and its schema as a string.

    Args:
        spark: SparkSession instance
        dict_data: List of dictionaries containing the data

    Returns:
        Tuple of (DataFrame, schema_string)

    Example:
        Input: [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        Output: (DataFrame, "struct<age:bigint,name:string>")
    """
    # TODO: Implement this function
    # 1. Create DataFrame from dict_data
    # 2. Get schema string using df.schema.simpleString()
    # 3. Return tuple of (DataFrame, schema_string)
    pass


def exercise_1c(spark: SparkSession, data: List[tuple]):
    """
    Create a DataFrame with an explicit schema.

    Args:
        spark: SparkSession instance
        data: List of tuples (name, age, salary)

    Returns:
        DataFrame with explicit schema (name: string, age: int, salary: double)

    The schema should specify:
    - name: StringType, nullable=True
    - age: IntegerType, nullable=True
    - salary: DoubleType, nullable=True
    """
    # TODO: Implement this function
    # 1. Define StructType schema with StructField for each column
    # 2. Create DataFrame using spark.createDataFrame(data, schema)
    # 3. Return the DataFrame
    pass


def exercise_1d(spark: SparkSession, df):
    """
    Filter DataFrame for Engineering department employees and select name and salary columns.

    Args:
        spark: SparkSession instance
        df: Input DataFrame with columns: name, age, department, salary

    Returns:
        DataFrame with only Engineering employees, showing name and salary,
        ordered by salary in descending order

    Use DataFrame API (not SQL).
    """
    # TODO: Implement this function
    # 1. Filter for department == "Engineering"
    # 2. Select only "name" and "salary" columns
    # 3. Order by salary descending
    # 4. Return the filtered and transformed DataFrame
    pass


def exercise_1e(spark: SparkSession, df):
    """
    Add calculated columns to the DataFrame.

    Args:
        spark: SparkSession instance
        df: Input DataFrame with columns: name, age, salary

    Returns:
        DataFrame with additional columns:
        - annual_salary: salary * 12
        - age_category: "Young" if age < 30, "Senior" otherwise
        - name_length: length of the name string

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # 1. Add annual_salary column (salary * 12)
    # 2. Add age_category column using when().otherwise()
    # 3. Add name_length column using length() function
    # 4. Return DataFrame with all original + new columns
    pass


def exercise_1f(spark: SparkSession, df):
    """
    Perform aggregations on the DataFrame.

    Args:
        spark: SparkSession instance
        df: Input DataFrame with columns: department, salary

    Returns:
        DataFrame with aggregations by department:
        - department: department name
        - employee_count: number of employees
        - avg_salary: average salary
        - max_salary: maximum salary
        - total_payroll: sum of all salaries

    Results should be ordered by avg_salary descending.
    """
    # TODO: Implement this function
    # 1. Group by department
    # 2. Calculate count, avg, max, and sum aggregations
    # 3. Order by average salary descending
    # 4. Return aggregated DataFrame
    pass


def create_sample_data():
    """Create sample data files for testing."""
    os.makedirs("../data", exist_ok=True)

    # Create a simple CSV file
    csv_data = """name,age,department,salary
Alice,25,Engineering,80000
Bob,30,Marketing,75000
Charlie,35,Engineering,85000
Diana,28,Sales,70000
Eve,32,Engineering,90000"""
    with open("../data/employees.csv", "w") as f:
        f.write(csv_data)

    print("Sample data files created successfully!")


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 1: DataFrame Creation and Basic Operations")
    print("=" * 65)

    # Create sample data
    create_sample_data()

    # Test Exercise 1a
    print("\n📝 Exercise 1a: Create DataFrame from Tuples")
    try:
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        result_1a = exercise_1a(spark, data, columns)

        if result_1a is not None:
            print("DataFrame created successfully:")
            result_1a.show()
            print(f"Row count: {result_1a.count()}")
            print(f"Columns: {result_1a.columns}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 1b
    print("\n📝 Exercise 1b: Create DataFrame from Dictionaries")
    try:
        dict_data = [
            {"name": "Alice", "age": 25, "salary": 50000},
            {"name": "Bob", "age": 30, "salary": 60000},
        ]
        result_1b = exercise_1b(spark, dict_data)

        if result_1b is not None and len(result_1b) == 2:
            df, schema_str = result_1b
            print("DataFrame created successfully:")
            df.show()
            print(f"Schema: {schema_str}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function should return (DataFrame, schema_string) tuple")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 1c
    print("\n📝 Exercise 1c: Create DataFrame with Explicit Schema")
    try:
        data = [("Alice", 25, 80000.0), ("Bob", 30, 75000.0)]
        result_1c = exercise_1c(spark, data)

        if result_1c is not None:
            print("DataFrame with explicit schema:")
            result_1c.show()
            result_1c.printSchema()
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 1d
    print("\n📝 Exercise 1d: Filter and Select Operations")
    try:
        # Create test DataFrame
        test_data = [
            ("Alice", 25, "Engineering", 80000),
            ("Bob", 30, "Marketing", 75000),
            ("Charlie", 35, "Engineering", 85000),
            ("Diana", 28, "Sales", 70000),
        ]
        test_df = spark.createDataFrame(
            test_data, ["name", "age", "department", "salary"]
        )

        result_1d = exercise_1d(spark, test_df)

        if result_1d is not None:
            print("Engineering employees (name, salary) ordered by salary:")
            result_1d.show()
            print(f"Row count: {result_1d.count()}")
            print(f"Columns: {result_1d.columns}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 1e
    print("\n📝 Exercise 1e: Add Calculated Columns")
    try:
        test_data = [("Alice", 25, 80000), ("Bob", 35, 75000)]
        test_df = spark.createDataFrame(test_data, ["name", "age", "salary"])

        result_1e = exercise_1e(spark, test_df)

        if result_1e is not None:
            print("DataFrame with calculated columns:")
            result_1e.show()
            expected_cols = [
                "name",
                "age",
                "salary",
                "annual_salary",
                "age_category",
                "name_length",
            ]
            actual_cols = result_1e.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 1f
    print("\n📝 Exercise 1f: Group By Aggregations")
    try:
        test_data = [
            ("Engineering", 80000),
            ("Engineering", 85000),
            ("Marketing", 75000),
            ("Sales", 70000),
            ("Sales", 72000),
        ]
        test_df = spark.createDataFrame(test_data, ["department", "salary"])

        result_1f = exercise_1f(spark, test_df)

        if result_1f is not None:
            print("Department aggregations:")
            result_1f.show()
            expected_cols = [
                "department",
                "employee_count",
                "avg_salary",
                "max_salary",
                "total_payroll",
            ]
            actual_cols = result_1f.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print("✅ Function executed successfully!")
        else:
            print("❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 1 completed!")
    print(
        "💡 If you got any errors, review the function implementations and try again."
    )
    print("📚 Ready to move on to Exercise 2!")


if __name__ == "__main__":
    run_exercises()
