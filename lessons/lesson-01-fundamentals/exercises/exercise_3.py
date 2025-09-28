"""
Exercise 3: Advanced RDD Operations & Performance
=================================================

This exercise focuses on advanced RDD operations, performance optimization,
and working with complex data structures.

Instructions:
1. Complete each function according to the docstring requirements
2. Pay attention to performance implications of your solutions
3. Experiment with different approaches and measure performance

Learning Goals:
- Advanced RDD transformations and actions
- Performance optimization techniques
- Working with complex data structures
- Understanding Spark's execution model
- Caching and persistence strategies

Run this file: python exercise_3.py
"""

from pyspark.sql import SparkSession
from pyspark import StorageLevel
from typing import List, Tuple, Dict, Any
import time
import random


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise3-AdvancedRDD") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def generate_sales_data(num_records: int = 10000) -> List[Dict[str, Any]]:
    """Generate sample sales data for exercises."""
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    regions = ["North", "South", "East", "West", "Central"]

    sales_data = []
    for i in range(num_records):
        record = {
            "transaction_id": f"TXN_{i:06d}",
            "customer_id": f"CUST_{random.randint(1, 1000):04d}",
            "product_category": random.choice(categories),
            "region": random.choice(regions),
            "amount": round(random.uniform(10, 1000), 2),
            "quantity": random.randint(1, 10),
            "date": f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        }
        sales_data.append(record)

    return sales_data


def exercise_3a(spark: SparkSession, sales_data: List[Dict]) -> List[Tuple[str, float]]:
    """
    Calculate total sales amount by product category.

    Args:
        spark: SparkSession instance
        sales_data: List of sales records (dictionaries)

    Returns:
        List of (category, total_amount) tuples, sorted by total amount (descending)

    Requirements:
    - Group sales by product category
    - Sum the amount for each category
    - Sort by total amount in descending order
    """
    # TODO: Implement this function
    # Hint: Use map to extract (category, amount) pairs, then reduceByKey
    pass


def exercise_3b(spark: SparkSession, sales_data: List[Dict]) -> Dict[str, Dict[str, float]]:
    """
    Create a sales summary by region and category.

    Args:
        spark: SparkSession instance
        sales_data: List of sales records

    Returns:
        Dictionary where keys are regions and values are dictionaries
        of category -> total_amount

    Example output:
    {
        "North": {"Electronics": 15000, "Clothing": 8000, ...},
        "South": {"Electronics": 12000, "Clothing": 9000, ...},
        ...
    }

    Requirements:
    - Group by region and category
    - Calculate total amount for each region-category combination
    - Return as nested dictionary structure
    """
    # TODO: Implement this function
    # Hint: Use map to create ((region, category), amount) pairs, then group and aggregate
    pass


def exercise_3c(spark: SparkSession, numbers: List[int]) -> List[Tuple[int, List[int]]]:
    """
    Find all factors for each number in the list.

    Args:
        spark: SparkSession instance
        numbers: List of integers

    Returns:
        List of (number, factors) tuples where factors is a list of all factors

    Example:
        Input: [12, 15, 20]
        Output: [(12, [1, 2, 3, 4, 6, 12]), (15, [1, 3, 5, 15]), (20, [1, 2, 4, 5, 10, 20])]

    Requirements:
    - Find all factors for each number (numbers that divide evenly)
    - Return factors in ascending order
    - Handle edge cases (0, negative numbers)
    """
    # TODO: Implement this function
    pass


def exercise_3d(spark: SparkSession, data: List[int]) -> Tuple[float, float]:
    """
    Compare performance of computing standard deviation with and without caching.

    Args:
        spark: SparkSession instance
        data: List of integers

    Returns:
        Tuple of (time_without_cache, time_with_cache) in seconds

    Requirements:
    - Compute standard deviation without caching (time it)
    - Compute standard deviation with caching (time it)
    - Standard deviation formula: sqrt(sum((x - mean)^2) / n)
    - Return execution times for comparison
    """
    # TODO: Implement this function
    # Hint: You'll need to calculate mean first, then variance, then std dev
    # Cache the RDD before multiple operations
    pass


def exercise_3e(spark: SparkSession, text_data: List[str]) -> List[Tuple[str, int]]:
    """
    Find anagrams in the text data and count them.

    Args:
        spark: SparkSession instance
        text_data: List of text lines

    Returns:
        List of (anagram_group, count) tuples for groups with more than 1 word

    Example:
        Input: ["listen silent", "evil live", "hello world"]
        Output: [("eilnst", 2), ("eilv", 2)]  # sorted letters as group key

    Requirements:
    - Extract all words from text (clean: lowercase, alphabetic only)
    - Group words by their sorted letters (anagram signature)
    - Count words in each anagram group
    - Return only groups with more than 1 word
    - Sort by count (descending)
    """
    # TODO: Implement this function
    pass


def exercise_3f(spark: SparkSession, data: List[int], k: int) -> List[int]:
    """
    Find the k largest elements using RDD operations (without using takeOrdered).

    Args:
        spark: SparkSession instance
        data: List of integers
        k: Number of largest elements to find

    Returns:
        List of k largest elements in descending order

    Requirements:
    - Don't use takeOrdered() or top() functions
    - Implement your own algorithm using RDD transformations
    - Handle duplicates appropriately
    - Ensure exactly k elements in result
    """
    # TODO: Implement this function
    # Hint: Consider using sortBy or implementing a custom approach
    pass


def exercise_3g(spark: SparkSession, transactions: List[Dict]) -> List[Tuple[str, int, float]]:
    """
    Find customers with suspicious transaction patterns.

    Args:
        spark: SparkSession instance
        transactions: List of transaction records

    Returns:
        List of (customer_id, transaction_count, total_amount) for suspicious customers

    Requirements:
    - A customer is suspicious if they have:
      * More than 10 transactions AND total amount > $5000
      * OR average transaction amount > $800
    - Sort by total amount (descending)
    - Include customer_id, transaction count, and total amount
    """
    # TODO: Implement this function
    pass


def run_performance_comparison(spark: SparkSession):
    """Demonstrate performance differences between different approaches."""
    print("\n🔬 Performance Comparison: Caching vs No Caching")
    print("-" * 50)

    # Generate test data
    large_data = list(range(1, 100001))  # 100k numbers
    rdd = spark.sparkContext.parallelize(large_data, 8)

    # Without caching - multiple computations
    start_time = time.time()
    count = rdd.count()
    total = rdd.sum()
    maximum = rdd.max()
    minimum = rdd.min()
    no_cache_time = time.time() - start_time

    # With caching - same computations
    rdd.cache()
    start_time = time.time()
    count = rdd.count()  # This will cache the RDD
    total = rdd.sum()    # These will use cached data
    maximum = rdd.max()
    minimum = rdd.min()
    cache_time = time.time() - start_time

    print(f"Without caching: {no_cache_time:.3f} seconds")
    print(f"With caching: {cache_time:.3f} seconds")
    print(f"Speedup: {no_cache_time/cache_time:.2f}x")

    rdd.unpersist()


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 3: Advanced RDD Operations & Performance")
    print("=" * 65)

    # Generate sample data
    print("📊 Generating sample sales data...")
    sales_data = generate_sales_data(1000)  # Smaller dataset for testing
    print(f"Generated {len(sales_data)} sales records")

    # Test Exercise 3a
    print("\n📝 Exercise 3a: Sales by Category")
    try:
        result_3a = exercise_3a(spark, sales_data)
        print(f"Top 5 categories by sales: {result_3a[:5]}")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3b
    print("\n📝 Exercise 3b: Sales Summary by Region and Category")
    try:
        result_3b = exercise_3b(spark, sales_data[:100])  # Smaller sample
        print("Sample regional breakdown:")
        for region, categories in list(result_3b.items())[:2]:
            print(f"  {region}: {dict(list(categories.items())[:3])}")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3c
    print("\n📝 Exercise 3c: Find Factors")
    test_numbers = [12, 15, 20, 24, 30]
    try:
        result_3c = exercise_3c(spark, test_numbers)
        print("Factors:")
        for num, factors in result_3c:
            print(f"  {num}: {factors}")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3d
    print("\n📝 Exercise 3d: Caching Performance Comparison")
    test_data = list(range(1, 10001))
    try:
        result_3d = exercise_3d(spark, test_data)
        print(f"Time without cache: {result_3d[0]:.3f}s")
        print(f"Time with cache: {result_3d[1]:.3f}s")
        if result_3d[1] > 0:
            speedup = result_3d[0] / result_3d[1]
            print(f"Speedup: {speedup:.2f}x")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3e
    print("\n📝 Exercise 3e: Find Anagrams")
    test_text = [
        "listen silent",
        "evil live veil",
        "hello world",
        "silent listen",
        "abc cab bca"
    ]
    try:
        result_3e = exercise_3e(spark, test_text)
        print(f"Anagram groups: {result_3e}")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3f
    print("\n📝 Exercise 3f: K Largest Elements")
    test_k_data = [64, 34, 25, 12, 22, 11, 90, 88, 76, 50, 5]
    try:
        result_3f = exercise_3f(spark, test_k_data, 5)
        print(f"Input: {test_k_data}")
        print(f"5 largest elements: {result_3f}")
        print(f"Expected: [90, 88, 76, 64, 50]")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 3g
    print("\n📝 Exercise 3g: Suspicious Customers")
    try:
        result_3g = exercise_3g(spark, sales_data)
        print(f"Suspicious customers (top 5): {result_3g[:5]}")
        print(f"✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Performance comparison demo
    run_performance_comparison(spark)

    spark.stop()
    print("\n🎉 Exercise 3 completed!")
    print("💡 This exercise demonstrated advanced RDD operations and performance optimization.")
    print("🎓 You've completed all the fundamental RDD exercises!")
    print("📚 Ready to move on to validation tests and Lesson 2!")


if __name__ == "__main__":
    run_exercises()