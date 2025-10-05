"""
Exercise 7: Performance Comparison - DataFrames vs RDDs
=======================================================

This final exercise compares DataFrame and RDD approaches for the same tasks,
demonstrating the performance benefits and ease of use of DataFrames.

Instructions:
1. Complete each function according to the docstring requirements
2. Compare execution times and code complexity
3. Understand when to use DataFrames vs RDDs

Learning Goals:
- Compare DataFrame and RDD performance
- Understand DataFrame optimization benefits
- Learn when to choose each approach
- Practice execution plan analysis
- Master caching strategies

Run this file: python exercise_7.py
"""

import time
from typing import List, Tuple

from pyspark.sql import SparkSession


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise7-PerformanceComparison")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_7a_rdd(spark: SparkSession, data: List[tuple]) -> Tuple[int, float, float]:
    """
    Calculate statistics using RDD approach (Lesson 1 style).

    Args:
        spark: SparkSession instance
        data: List of tuples (name, age, salary)

    Returns:
        Tuple of (count, average_age, average_salary)

    Use RDD transformations and actions only.
    """
    # TODO: Implement this function using RDD approach
    # 1. Create RDD using spark.sparkContext.parallelize()
    # 2. Use map() and reduce() operations to calculate statistics
    # 3. Return (count, avg_age, avg_salary)


def exercise_7a_dataframe(
    spark: SparkSession, data: List[tuple]
) -> Tuple[int, float, float]:
    """
    Calculate statistics using DataFrame approach (Lesson 2 style).

    Args:
        spark: SparkSession instance
        data: List of tuples (name, age, salary)

    Returns:
        Tuple of (count, average_age, average_salary)

    Use DataFrame operations only.
    """
    # TODO: Implement this function using DataFrame approach
    # 1. Create DataFrame using spark.createDataFrame()
    # 2. Use DataFrame aggregation functions
    # 3. Return (count, avg_age, avg_salary)


def exercise_7b_rdd(
    spark: SparkSession, data: List[tuple]
) -> List[Tuple[str, int, float]]:
    """
    Group by department and calculate statistics using RDD.

    Args:
        spark: SparkSession instance
        data: List of tuples (name, department, salary)

    Returns:
        List of (department, employee_count, average_salary) tuples

    Use RDD approach with map, groupByKey, and reduce operations.
    """
    # TODO: Implement this function using RDD approach
    # 1. Create RDD and map to (department, salary) pairs
    # 2. Use groupByKey() or reduceByKey() for aggregation
    # 3. Calculate count and average for each department


def exercise_7b_dataframe(
    spark: SparkSession, data: List[tuple]
) -> List[Tuple[str, int, float]]:
    """
    Group by department and calculate statistics using DataFrame.

    Args:
        spark: SparkSession instance
        data: List of tuples (name, department, salary)

    Returns:
        List of (department, employee_count, average_salary) tuples

    Use DataFrame groupBy and aggregation functions.
    """
    # TODO: Implement this function using DataFrame approach
    # 1. Create DataFrame
    # 2. Use groupBy().agg() for aggregation
    # 3. Collect and return results


def exercise_7c_rdd(
    spark: SparkSession, data1: List[tuple], data2: List[tuple]
) -> List[tuple]:
    """
    Join two datasets using RDD operations.

    Args:
        spark: SparkSession instance
        data1: List of tuples (id, name)
        data2: List of tuples (id, salary)

    Returns:
        List of joined tuples (id, name, salary)

    Use RDD join operation.
    """
    # TODO: Implement this function using RDD approach
    # 1. Create RDDs from both datasets
    # 2. Convert to key-value pairs with id as key
    # 3. Use join() operation
    # 4. Transform result to desired format


def exercise_7c_dataframe(
    spark: SparkSession, data1: List[tuple], data2: List[tuple]
) -> List[tuple]:
    """
    Join two datasets using DataFrame operations.

    Args:
        spark: SparkSession instance
        data1: List of tuples (id, name)
        data2: List of tuples (id, salary)

    Returns:
        List of joined tuples (id, name, salary)

    Use DataFrame join operation.
    """
    # TODO: Implement this function using DataFrame approach
    # 1. Create DataFrames from both datasets
    # 2. Use DataFrame join() operation
    # 3. Select and collect results


def exercise_7d_caching_comparison(spark: SparkSession, data: List[tuple]):
    """
    Compare caching performance between RDD and DataFrame approaches.

    Args:
        spark: SparkSession instance
        data: List of tuples (id, value1, value2)

    Returns:
        Dictionary with timing results:
        {
            'rdd_no_cache': time_seconds,
            'rdd_with_cache': time_seconds,
            'df_no_cache': time_seconds,
            'df_with_cache': time_seconds
        }

    Perform multiple operations on the same dataset to test caching effectiveness.
    """
    # TODO: Implement this function
    # Test both RDD and DataFrame approaches with and without caching
    # Perform multiple operations (count, sum, filter) on the same data
    # Measure execution times for comparison


def exercise_7e_explain_plans(spark: SparkSession, data: List[tuple]):
    """
    Compare execution plans between RDD and DataFrame operations.

    Args:
        spark: SparkSession instance
        data: List of tuples (name, age, department, salary)

    Returns:
        Dictionary with execution plan information:
        {
            'simple_filter_plan': DataFrame_explain_output,
            'complex_aggregation_plan': DataFrame_explain_output,
            'join_plan': DataFrame_explain_output
        }

    Create DataFrames and show their execution plans for analysis.
    """
    # TODO: Implement this function
    # 1. Create DataFrame from data
    # 2. Perform different operations and capture explain() output
    # 3. Return plan information for analysis


def performance_benchmark(spark: SparkSession):
    """
    Run comprehensive performance benchmarks comparing RDD vs DataFrame approaches.
    """
    print("\n🏁 Performance Benchmark: RDD vs DataFrame")
    print("=" * 50)

    # Generate larger test dataset
    large_data = [
        (f"Person_{i}", 20 + (i % 40), f"Dept_{i % 5}", 40000 + (i * 100) % 50000)
        for i in range(10000)
    ]

    print(f"\nTesting with {len(large_data)} records...")

    # Test 1: Basic aggregations
    print("\n📊 Test 1: Basic Statistics")

    # RDD approach
    start_time = time.time()
    try:
        result_rdd = exercise_7a_rdd(spark, large_data)
        rdd_time = time.time() - start_time
        print(f"RDD approach: {rdd_time:.3f}s, Result: {result_rdd}")
    except Exception as e:
        print(f"RDD approach failed: {e}")
        rdd_time = float("inf")

    # DataFrame approach
    start_time = time.time()
    try:
        result_df = exercise_7a_dataframe(spark, large_data)
        df_time = time.time() - start_time
        print(f"DataFrame approach: {df_time:.3f}s, Result: {result_df}")
    except Exception as e:
        print(f"DataFrame approach failed: {e}")
        df_time = float("inf")

    if rdd_time != float("inf") and df_time != float("inf"):
        speedup = rdd_time / df_time
        print(f"DataFrame speedup: {speedup:.2f}x")

    # Test 2: GroupBy operations
    print("\n📊 Test 2: GroupBy Operations")

    # RDD approach
    start_time = time.time()
    try:
        result_rdd = exercise_7b_rdd(spark, large_data)
        rdd_time = time.time() - start_time
        print(f"RDD approach: {rdd_time:.3f}s, {len(result_rdd)} groups")
    except Exception as e:
        print(f"RDD approach failed: {e}")
        rdd_time = float("inf")

    # DataFrame approach
    start_time = time.time()
    try:
        result_df = exercise_7b_dataframe(spark, large_data)
        df_time = time.time() - start_time
        print(f"DataFrame approach: {df_time:.3f}s, {len(result_df)} groups")
    except Exception as e:
        print(f"DataFrame approach failed: {e}")
        df_time = float("inf")

    if rdd_time != float("inf") and df_time != float("inf"):
        speedup = rdd_time / df_time
        print(f"DataFrame speedup: {speedup:.2f}x")


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 7: Performance Comparison - DataFrames vs RDDs")
    print("=" * 70)

    # Test Exercise 7a - Basic Statistics
    print("\n📝 Exercise 7a: Basic Statistics Comparison")
    test_data = [
        ("Alice", 28, 85000),
        ("Bob", 34, 75000),
        ("Charlie", 29, 90000),
        ("Diana", 31, 70000),
    ]

    try:
        print("\nRDD Approach:")
        result_7a_rdd = exercise_7a_rdd(spark, test_data)
        print(f"Result: {result_7a_rdd}")

        print("DataFrame Approach:")
        result_7a_df = exercise_7a_dataframe(spark, test_data)
        print(f"Result: {result_7a_df}")

        if result_7a_rdd == result_7a_df:
            print("✅ Both approaches produce same results!")
        else:
            print("❌ Results differ between approaches")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 7b - GroupBy Operations
    print("\n📝 Exercise 7b: GroupBy Operations Comparison")
    dept_data = [
        ("Alice", "Engineering", 85000),
        ("Bob", "Engineering", 75000),
        ("Charlie", "Marketing", 70000),
        ("Diana", "Marketing", 72000),
        ("Eve", "Sales", 68000),
    ]

    try:
        print("\nRDD Approach:")
        result_7b_rdd = exercise_7b_rdd(spark, dept_data)
        print(f"Results: {result_7b_rdd}")

        print("DataFrame Approach:")
        result_7b_df = exercise_7b_dataframe(spark, dept_data)
        print(f"Results: {result_7b_df}")

        print("✅ Both approaches implemented successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 7c - Join Operations
    print("\n📝 Exercise 7c: Join Operations Comparison")
    employees = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    salaries = [(1, 85000), (2, 75000), (3, 90000)]

    try:
        print("\nRDD Approach:")
        result_7c_rdd = exercise_7c_rdd(spark, employees, salaries)
        print(f"Joined results: {result_7c_rdd}")

        print("DataFrame Approach:")
        result_7c_df = exercise_7c_dataframe(spark, employees, salaries)
        print(f"Joined results: {result_7c_df}")

        print("✅ Both join approaches implemented successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 7d - Caching Comparison
    print("\n📝 Exercise 7d: Caching Performance Comparison")
    cache_data = [(i, i * 2, i * 3) for i in range(1000)]

    try:
        result_7d = exercise_7d_caching_comparison(spark, cache_data)
        if result_7d:
            print("Caching performance results:")
            for approach, time_taken in result_7d.items():
                print(f"  {approach}: {time_taken:.3f}s")
            print("✅ Caching comparison completed!")
        else:
            print("❌ Caching comparison not implemented")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 7e - Execution Plans
    print("\n📝 Exercise 7e: DataFrame Execution Plans")
    plan_data = [
        ("Alice", 28, "Engineering", 85000),
        ("Bob", 34, "Marketing", 75000),
        ("Charlie", 29, "Engineering", 90000),
    ]

    try:
        result_7e = exercise_7e_explain_plans(spark, plan_data)
        if result_7e:
            print("Execution plans captured successfully!")
            print(f"Plans available: {list(result_7e.keys())}")
            print("✅ Execution plan analysis completed!")
        else:
            print("❌ Execution plan analysis not implemented")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Run performance benchmark
    performance_benchmark(spark)

    spark.stop()
    print("\n🎉 Exercise 7 completed!")
    print("\n=== Key Takeaways ===")
    print("🔑 DataFrames provide better performance through Catalyst optimization")
    print("🔑 DataFrames have more readable and concise syntax")
    print("🔑 RDDs give lower-level control but require more manual optimization")
    print("🔑 Use DataFrames for structured data and standard operations")
    print("🔑 Use RDDs for complex custom operations or unstructured data")
    print("\n🎓 Congratulations! You've completed all DataFrame exercises!")
    print("📚 Ready for the next lesson in your Spark journey!")


if __name__ == "__main__":
    run_exercises()
