"""
Exercise 7 Solutions: Performance Comparison - DataFrames vs RDDs
================================================================

This file contains the complete solutions for Exercise 7.
Study these solutions to understand performance differences between DataFrames and RDDs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, when
import time
from typing import List, Tuple
from io import StringIO
import sys


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise7-Solutions")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_7a_rdd(spark: SparkSession, data: List[tuple]) -> Tuple[int, float, float]:
    """Calculate statistics using RDD approach (Lesson 1 style)."""
    # Create RDD
    rdd = spark.sparkContext.parallelize(data)

    # Calculate statistics using RDD operations
    count_val = rdd.count()

    # Extract age and salary for calculations
    ages = rdd.map(lambda x: x[1])  # age is second element
    salaries = rdd.map(lambda x: x[2])  # salary is third element

    avg_age = ages.sum() / count_val
    avg_salary = salaries.sum() / count_val

    return (count_val, avg_age, avg_salary)


def exercise_7a_dataframe(
    spark: SparkSession, data: List[tuple]
) -> Tuple[int, float, float]:
    """Calculate statistics using DataFrame approach (Lesson 2 style)."""
    # Create DataFrame
    df = spark.createDataFrame(data, ["name", "age", "salary"])

    # Calculate statistics using DataFrame aggregations
    stats = df.agg(
        count("*").alias("count"),
        avg("age").alias("avg_age"),
        avg("salary").alias("avg_salary"),
    ).collect()[0]

    return (stats["count"], stats["avg_age"], stats["avg_salary"])


def exercise_7b_rdd(
    spark: SparkSession, data: List[tuple]
) -> List[Tuple[str, int, float]]:
    """Group by department and calculate statistics using RDD."""
    # Create RDD
    rdd = spark.sparkContext.parallelize(data)

    # Map to (department, salary) pairs
    dept_salary_pairs = rdd.map(lambda x: (x[1], x[2]))  # (department, salary)

    # Group by department and calculate statistics
    def calculate_dept_stats(salary_list):
        salary_list = list(salary_list)
        count = len(salary_list)
        avg_salary = sum(salary_list) / count if count > 0 else 0.0
        return (count, avg_salary)

    # Use groupByKey and mapValues for aggregation
    dept_stats = (
        dept_salary_pairs.groupByKey()
        .mapValues(calculate_dept_stats)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
    )  # (dept, count, avg)

    return dept_stats.collect()


def exercise_7b_dataframe(
    spark: SparkSession, data: List[tuple]
) -> List[Tuple[str, int, float]]:
    """Group by department and calculate statistics using DataFrame."""
    # Create DataFrame
    df = spark.createDataFrame(data, ["name", "department", "salary"])

    # Group by department and calculate statistics
    result = (
        df.groupBy("department")
        .agg(count("*").alias("employee_count"), avg("salary").alias("average_salary"))
        .select("department", "employee_count", "average_salary")
        .collect()
    )

    # Convert to list of tuples
    return [
        (row["department"], row["employee_count"], row["average_salary"])
        for row in result
    ]


def exercise_7c_rdd(
    spark: SparkSession, data1: List[tuple], data2: List[tuple]
) -> List[tuple]:
    """Join two datasets using RDD operations."""
    # Create RDDs
    rdd1 = spark.sparkContext.parallelize(data1)  # (id, name)
    rdd2 = spark.sparkContext.parallelize(data2)  # (id, salary)

    # Convert to key-value pairs with id as key
    rdd1_kv = rdd1.map(lambda x: (x[0], x[1]))  # (id, name)
    rdd2_kv = rdd2.map(lambda x: (x[0], x[1]))  # (id, salary)

    # Perform join
    joined = rdd1_kv.join(rdd2_kv)  # (id, (name, salary))

    # Transform to desired format
    result = joined.map(lambda x: (x[0], x[1][0], x[1][1]))  # (id, name, salary)

    return result.collect()


def exercise_7c_dataframe(
    spark: SparkSession, data1: List[tuple], data2: List[tuple]
) -> List[tuple]:
    """Join two datasets using DataFrame operations."""
    # Create DataFrames
    df1 = spark.createDataFrame(data1, ["id", "name"])
    df2 = spark.createDataFrame(data2, ["id", "salary"])

    # Perform join
    result = df1.join(df2, "id", "inner").select("id", "name", "salary").collect()

    # Convert to list of tuples
    return [(row["id"], row["name"], row["salary"]) for row in result]


def exercise_7d_caching_comparison(spark: SparkSession, data: List[tuple]):
    """Compare caching performance between RDD and DataFrame approaches."""
    results = {}

    # Test RDD without caching
    start_time = time.time()
    rdd = spark.sparkContext.parallelize(data)
    # Perform multiple operations (results intentionally unused for timing)
    count1 = rdd.count()  # noqa: F841
    sum1 = rdd.map(lambda x: x[1]).sum()  # noqa: F841
    filtered1 = rdd.filter(lambda x: x[1] > 500).count()  # noqa: F841
    results["rdd_no_cache"] = time.time() - start_time

    # Test RDD with caching
    start_time = time.time()
    rdd_cached = spark.sparkContext.parallelize(data).cache()
    # Perform multiple operations (results intentionally unused for timing)
    count2 = rdd_cached.count()  # noqa: F841
    sum2 = rdd_cached.map(lambda x: x[1]).sum()  # noqa: F841
    filtered2 = rdd_cached.filter(lambda x: x[1] > 500).count()  # noqa: F841
    results["rdd_with_cache"] = time.time() - start_time

    # Test DataFrame without caching
    start_time = time.time()
    df = spark.createDataFrame(data, ["id", "value1", "value2"])
    # Perform multiple operations (results intentionally unused for timing)
    count3 = df.count()  # noqa: F841
    sum3 = df.agg(spark_sum("value1")).collect()[0][0]  # noqa: F841
    filtered3 = df.filter(col("value1") > 500).count()  # noqa: F841
    results["df_no_cache"] = time.time() - start_time

    # Test DataFrame with caching
    start_time = time.time()
    df_cached = spark.createDataFrame(data, ["id", "value1", "value2"]).cache()
    # Perform multiple operations (results intentionally unused for timing)
    count4 = df_cached.count()  # noqa: F841
    sum4 = df_cached.agg(spark_sum("value1")).collect()[0][0]  # noqa: F841
    filtered4 = df_cached.filter(col("value1") > 500).count()  # noqa: F841
    results["df_with_cache"] = time.time() - start_time

    return results


def exercise_7e_explain_plans(spark: SparkSession, data: List[tuple]):
    """Compare execution plans between RDD and DataFrame operations."""
    # Create DataFrame
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])

    # Capture execution plans
    plans = {}

    # Simple filter plan
    simple_filter = df.filter(col("age") > 30)
    old_stdout = sys.stdout
    sys.stdout = buffer1 = StringIO()
    simple_filter.explain(True)
    sys.stdout = old_stdout
    plans["simple_filter_plan"] = buffer1.getvalue()

    # Complex aggregation plan
    complex_agg = (
        df.groupBy("department")
        .agg(avg("salary").alias("avg_salary"), count("*").alias("emp_count"))
        .filter(col("avg_salary") > 70000)
    )
    old_stdout = sys.stdout
    sys.stdout = buffer2 = StringIO()
    complex_agg.explain(True)
    sys.stdout = old_stdout
    plans["complex_aggregation_plan"] = buffer2.getvalue()

    # Join plan
    df2 = spark.createDataFrame(
        [(1, "Engineering"), (2, "Marketing")], ["dept_id", "dept_name"]
    )
    df_with_dept_id = df.withColumn(
        "dept_id", when(col("department") == "Engineering", 1).otherwise(2)
    )
    join_result = df_with_dept_id.join(df2, "dept_id")
    old_stdout = sys.stdout
    sys.stdout = buffer3 = StringIO()
    join_result.explain(True)
    sys.stdout = old_stdout
    plans["join_plan"] = buffer3.getvalue()

    return plans


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 7 Solutions")
    print("=" * 30)

    # Solution 7a
    print("\n7a. Basic Statistics Comparison")
    test_data = [
        ("Alice", 28, 85000),
        ("Bob", 34, 75000),
        ("Charlie", 29, 90000),
        ("Diana", 31, 70000),
    ]

    print("RDD Approach:")
    result_7a_rdd = exercise_7a_rdd(spark, test_data)
    print(f"Result: {result_7a_rdd}")

    print("DataFrame Approach:")
    result_7a_df = exercise_7a_dataframe(spark, test_data)
    print(f"Result: {result_7a_df}")

    # Solution 7b
    print("\n7b. GroupBy Operations Comparison")
    dept_data = [
        ("Alice", "Engineering", 85000),
        ("Bob", "Engineering", 75000),
        ("Charlie", "Marketing", 70000),
        ("Diana", "Marketing", 72000),
        ("Eve", "Sales", 68000),
    ]

    print("RDD Approach:")
    result_7b_rdd = exercise_7b_rdd(spark, dept_data)
    print(f"Results: {result_7b_rdd}")

    print("DataFrame Approach:")
    result_7b_df = exercise_7b_dataframe(spark, dept_data)
    print(f"Results: {result_7b_df}")

    # Solution 7c
    print("\n7c. Join Operations Comparison")
    employees = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    salaries = [(1, 85000), (2, 75000), (3, 90000)]

    print("RDD Approach:")
    result_7c_rdd = exercise_7c_rdd(spark, employees, salaries)
    print(f"Joined results: {result_7c_rdd}")

    print("DataFrame Approach:")
    result_7c_df = exercise_7c_dataframe(spark, employees, salaries)
    print(f"Joined results: {result_7c_df}")

    # Solution 7d
    print("\n7d. Caching Performance Comparison")
    cache_data = [(i, i * 2, i * 3) for i in range(1000)]
    result_7d = exercise_7d_caching_comparison(spark, cache_data)
    print("Caching performance results:")
    for approach, time_taken in result_7d.items():
        print(f"  {approach}: {time_taken:.3f}s")

    # Solution 7e
    print("\n7e. DataFrame Execution Plans")
    plan_data = [
        ("Alice", 28, "Engineering", 85000),
        ("Bob", 34, "Marketing", 75000),
        ("Charlie", 29, "Engineering", 90000),
    ]
    result_7e = exercise_7e_explain_plans(spark, plan_data)
    print("Execution plans captured successfully!")
    print(f"Plans available: {list(result_7e.keys())}")

    # Show a sample plan
    print("\nSample execution plan (simple filter):")
    print(
        result_7e["simple_filter_plan"][:200] + "..."
        if len(result_7e["simple_filter_plan"]) > 200
        else result_7e["simple_filter_plan"]
    )

    spark.stop()
    print("\n🎓 All Exercise 7 solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()
