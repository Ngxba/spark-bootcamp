"""
Exercise 3 Solutions: Advanced RDD Operations & Performance
==========================================================

This file contains the complete solutions for Exercise 3.
Study these solutions to understand advanced RDD patterns and performance optimization.
"""

from pyspark.sql import SparkSession
from typing import List, Tuple, Dict
import time
import math
from collections import defaultdict


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise3-Solutions")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def exercise_3a(spark: SparkSession, sales_data: List[Dict]) -> List[Tuple[str, float]]:
    """
    Calculate total sales amount by product category.
    """
    # Create RDD from sales data
    sales_rdd = spark.sparkContext.parallelize(sales_data)

    # Extract category and amount, then sum by category
    category_sales = (
        sales_rdd.map(lambda record: (record["product_category"], record["amount"]))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
        .collect()
    )

    return category_sales


def exercise_3b(
    spark: SparkSession, sales_data: List[Dict]
) -> Dict[str, Dict[str, float]]:
    """
    Create a sales summary by region and category.
    """
    # Create RDD from sales data
    sales_rdd = spark.sparkContext.parallelize(sales_data)

    # Create (region, category) -> amount pairs and aggregate
    region_category_sales = (
        sales_rdd.map(
            lambda record: (
                (record["region"], record["product_category"]),
                record["amount"],
            )
        )
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    # Convert to nested dictionary structure
    result = defaultdict(dict)  # type: ignore
    for (region, category), amount in region_category_sales:
        result[region][category] = amount

    return dict(result)


def exercise_3c(spark: SparkSession, numbers: List[int]) -> List[Tuple[int, List[int]]]:
    """
    Find all factors for each number in the list.
    """

    def find_factors(n):
        if n == 0:
            return []
        if n < 0:
            n = abs(n)

        factors = []
        for i in range(1, int(math.sqrt(n)) + 1):
            if n % i == 0:
                factors.append(i)
                if i != n // i:  # Avoid duplicate for perfect squares
                    factors.append(n // i)
        return sorted(factors)

    # Create RDD and find factors for each number
    numbers_rdd = spark.sparkContext.parallelize(numbers)
    factors_result = numbers_rdd.map(lambda n: (n, find_factors(n))).collect()

    return factors_result


def exercise_3d(spark: SparkSession, data: List[int]) -> Tuple[float, float]:
    """
    Compare performance of computing standard deviation with and without caching.
    """
    # Without caching
    start_time = time.time()
    data_rdd = spark.sparkContext.parallelize(data)

    # Calculate mean (multiple passes through data)
    count = data_rdd.count()
    total = data_rdd.sum()
    mean = total / count

    # Calculate variance (another pass through data)
    variance = data_rdd.map(lambda x: (x - mean) ** 2).sum() / count
    math.sqrt(variance)

    time_without_cache = time.time() - start_time

    # With caching
    start_time = time.time()
    data_rdd_cached = spark.sparkContext.parallelize(data)
    data_rdd_cached.cache()

    # Force materialization
    count = data_rdd_cached.count()
    total = data_rdd_cached.sum()
    mean = total / count

    # Use cached data for variance calculation
    variance = data_rdd_cached.map(lambda x: (x - mean) ** 2).sum() / count
    math.sqrt(variance)

    time_with_cache = time.time() - start_time

    # Clean up
    data_rdd_cached.unpersist()

    return (time_without_cache, time_with_cache)


def exercise_3e(spark: SparkSession, text_data: List[str]) -> List[Tuple[str, int]]:
    """
    Find anagrams in the text data and count them.
    """
    # Create RDD from text data
    text_rdd = spark.sparkContext.parallelize(text_data)

    # Extract words and create anagram signatures
    words_rdd = (
        text_rdd.flatMap(lambda line: line.split())
        .map(lambda word: "".join(filter(str.isalpha, word.lower())))
        .filter(lambda word: len(word) > 0)
    )

    # Create (sorted_letters, word) pairs and group by anagram signature
    anagram_groups = (
        words_rdd.map(lambda word: ("".join(sorted(word)), word))
        .groupByKey()
        .mapValues(lambda words: list(set(words)))
        .filter(lambda x: len(x[1]) > 1)
        .map(lambda x: (x[0], len(x[1])))
        .sortBy(lambda x: x[1], ascending=False)
        .collect()
    )

    return anagram_groups


def exercise_3f(spark: SparkSession, data: List[int], k: int) -> List[int]:
    """
    Find the k largest elements using RDD operations.
    """
    # Create RDD from data
    data_rdd = spark.sparkContext.parallelize(data)

    # Sort in descending order and take k elements
    k_largest = data_rdd.distinct().sortBy(lambda x: x, ascending=False).take(k)

    return k_largest


def exercise_3g(
    spark: SparkSession, transactions: List[Dict]
) -> List[Tuple[str, int, float]]:
    """
    Find customers with suspicious transaction patterns.
    """
    # Create RDD from transactions
    transactions_rdd = spark.sparkContext.parallelize(transactions)

    # Group by customer and calculate statistics
    customer_stats = (
        transactions_rdd.map(lambda t: (t["customer_id"], (t["amount"], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda x: (x[0], x[1][1], x[1][0], x[1][0] / x[1][1]))
    )  # (customer, count, total, avg)

    # Filter suspicious customers
    suspicious = (
        customer_stats.filter(lambda x: (x[1] > 10 and x[2] > 5000) or x[3] > 800)
        .map(lambda x: (x[0], x[1], x[2]))
        .sortBy(lambda x: x[2], ascending=False)
        .collect()
    )

    return suspicious


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 3 Solutions")
    print("=" * 30)

    # Generate sample data
    from exercise_3 import generate_sales_data

    sales_data = generate_sales_data(1000)

    # Solution 3a
    print("\n3a. Sales by Category")
    result_3a = exercise_3a(spark, sales_data)
    print("Top 5 categories by sales:")
    for category, amount in result_3a[:5]:
        print(f"  {category}: ${amount:,.2f}")

    # Solution 3b
    print("\n3b. Sales Summary by Region and Category")
    result_3b = exercise_3b(spark, sales_data[:100])
    print("Regional breakdown (sample):")
    for region, categories in list(result_3b.items())[:2]:
        print(f"  {region}:")
        for category, amount in list(categories.items())[:3]:
            print(f"    {category}: ${amount:,.2f}")

    # Solution 3c
    print("\n3c. Find Factors")
    test_numbers = [12, 15, 20, 24, 30]
    result_3c = exercise_3c(spark, test_numbers)
    print("Number factors:")
    for num, factors in result_3c:
        print(f"  {num}: {factors}")

    # Solution 3d
    print("\n3d. Caching Performance")
    test_data = list(range(1, 10001))
    result_3d = exercise_3d(spark, test_data)
    print(f"Time without cache: {result_3d[0]:.4f}s")
    print(f"Time with cache: {result_3d[1]:.4f}s")
    if result_3d[1] > 0:
        speedup = result_3d[0] / result_3d[1]
        print(f"Speedup: {speedup:.2f}x")

    # Solution 3e
    print("\n3e. Find Anagrams")
    test_text = [
        "listen silent",
        "evil live veil",
        "hello world",
        "silent listen",
        "abc cab bca",
    ]
    result_3e = exercise_3e(spark, test_text)
    print("Anagram groups:")
    for signature, count in result_3e:
        print(f"  '{signature}': {count} words")

    # Solution 3f
    print("\n3f. K Largest Elements")
    test_k_data = [64, 34, 25, 12, 22, 11, 90, 88, 76, 50, 5]
    result_3f = exercise_3f(spark, test_k_data, 5)
    print(f"Input: {test_k_data}")
    print(f"5 largest: {result_3f}")

    # Solution 3g
    print("\n3g. Suspicious Customers")
    result_3g = exercise_3g(spark, sales_data)
    print("Suspicious customers (top 5):")
    for customer_id, count, total in result_3g[:5]:
        print(f"  {customer_id}: {count} transactions, ${total:,.2f} total")

    spark.stop()
    print("\n🎓 All solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()
