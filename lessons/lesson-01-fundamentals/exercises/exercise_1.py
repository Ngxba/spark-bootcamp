"""
Exercise 1: Basic RDD Operations
===============================

This exercise focuses on fundamental RDD operations including creating RDDs,
applying transformations, and executing actions.

Instructions:
1. Complete each function according to the docstring requirements
2. Run the script to test your implementations
3. Check your answers against the expected outputs

Learning Goals:
- Create RDDs from collections and ranges
- Apply map, filter, and reduce operations
- Understand the difference between transformations and actions
- Work with partitions

Run this file: python exercise_1.py
"""

from pyspark.sql import SparkSession
from typing import List, Tuple


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise1-BasicRDDOperations") \
        .master("local[*]") \
        .getOrCreate()


def exercise_1a(spark: SparkSession, numbers: List[int]) -> List[int]:
    """
    Create an RDD from the given list of numbers and return all even numbers.

    Args:
        spark: SparkSession instance
        numbers: List of integers

    Returns:
        List of even numbers from the input

    Example:
        Input: [1, 2, 3, 4, 5, 6]
        Output: [2, 4, 6]
    """
    # TODO: Implement this function
    # 1. Create RDD from numbers list using spark.sparkContext.parallelize()
    # 2. Filter for even numbers (hint: use lambda x: x % 2 == 0)
    # 3. Collect and return the results

    pass


def exercise_1b(spark: SparkSession, words: List[str]) -> int:
    """
    Count the total number of characters across all words.

    Args:
        spark: SparkSession instance
        words: List of strings

    Returns:
        Total character count across all words

    Example:
        Input: ["hello", "world", "spark"]
        Output: 15 (5 + 5 + 5)
    """
    # TODO: Implement this function
    # 1. Create RDD from words list
    # 2. Map each word to its length using len()
    # 3. Sum all lengths using reduce() or sum()

    pass


def exercise_1c(spark: SparkSession, data: List[int]) -> Tuple[int, int, float]:
    """
    Calculate statistics for the given data: count, sum, and average.

    Args:
        spark: SparkSession instance
        data: List of integers

    Returns:
        Tuple of (count, sum, average)

    Example:
        Input: [1, 2, 3, 4, 5]
        Output: (5, 15, 3.0)
    """
    # TODO: Implement this function
    # 1. Create RDD from data list
    # 2. Use RDD actions to calculate count, sum
    # 3. Calculate average and return as tuple

    pass


def exercise_1d(spark: SparkSession, text_data: List[str]) -> List[Tuple[str, int]]:
    """
    Perform word count on the given text data.

    Args:
        spark: SparkSession instance
        text_data: List of text lines

    Returns:
        List of (word, count) tuples, sorted by word alphabetically

    Example:
        Input: ["hello world", "hello spark"]
        Output: [("hello", 2), ("spark", 1), ("world", 1)]
    """
    # TODO: Implement this function
    # 1. Create RDD from text_data
    # 2. Use flatMap to split lines into words
    # 3. Convert words to lowercase
    # 4. Create (word, 1) pairs
    # 5. Use reduceByKey to count occurrences
    # 6. Sort by key and collect results

    pass


def exercise_1e(spark: SparkSession, numbers: List[int], num_partitions: int) -> List[List[int]]:
    """
    Create an RDD with specified number of partitions and return the partition contents.

    Args:
        spark: SparkSession instance
        numbers: List of integers
        num_partitions: Number of partitions to create

    Returns:
        List of lists, where each inner list contains the elements in that partition

    Example:
        Input: numbers=[1,2,3,4,5,6], num_partitions=3
        Output: [[1, 2], [3, 4], [5, 6]] (approximately, depends on partitioning)
    """
    # TODO: Implement this function
    # 1. Create RDD with specified number of partitions
    # 2. Use glom() to get partition contents
    # 3. Collect and return the results

    pass


def exercise_1f(spark: SparkSession, pairs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """
    Work with key-value pairs: group by key and sum the values.

    Args:
        spark: SparkSession instance
        pairs: List of (key, value) tuples

    Returns:
        List of (key, sum_of_values) tuples, sorted by key

    Example:
        Input: [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5)]
        Output: [("a", 4), ("b", 7), ("c", 4)]
    """
    # TODO: Implement this function
    # 1. Create RDD from pairs
    # 2. Use reduceByKey to sum values for each key
    # 3. Sort by key and collect results

    pass


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 1: Basic RDD Operations")
    print("=" * 50)

    # Test Exercise 1a
    print("\n📝 Exercise 1a: Filter Even Numbers")
    test_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    result_1a = exercise_1a(spark, test_numbers)
    print(f"Input: {test_numbers}")
    print(f"Output: {result_1a}")
    print(f"Expected: [2, 4, 6, 8, 10]")
    print(f"✅ Correct!" if result_1a == [2, 4, 6, 8, 10] else "❌ Incorrect")

    # Test Exercise 1b
    print("\n📝 Exercise 1b: Count Characters")
    test_words = ["hello", "world", "apache", "spark"]
    result_1b = exercise_1b(spark, test_words)
    print(f"Input: {test_words}")
    print(f"Output: {result_1b}")
    print(f"Expected: 20")
    print(f"✅ Correct!" if result_1b == 20 else "❌ Incorrect")

    # Test Exercise 1c
    print("\n📝 Exercise 1c: Calculate Statistics")
    test_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    result_1c = exercise_1c(spark, test_data)
    print(f"Input: {test_data}")
    print(f"Output: {result_1c}")
    print(f"Expected: (10, 55, 5.5)")
    print(f"✅ Correct!" if result_1c == (10, 55, 5.5) else "❌ Incorrect")

    # Test Exercise 1d
    print("\n📝 Exercise 1d: Word Count")
    test_text = ["hello world", "hello spark", "world of spark"]
    result_1d = exercise_1d(spark, test_text)
    print(f"Input: {test_text}")
    print(f"Output: {result_1d}")
    expected_1d = [("hello", 2), ("of", 1), ("spark", 2), ("world", 2)]
    print(f"Expected: {expected_1d}")
    print(f"✅ Correct!" if result_1d == expected_1d else "❌ Incorrect")

    # Test Exercise 1e
    print("\n📝 Exercise 1e: Partition Contents")
    test_nums = [1, 2, 3, 4, 5, 6]
    result_1e = exercise_1e(spark, test_nums, 3)
    print(f"Input: {test_nums}, partitions: 3")
    print(f"Output: {result_1e}")
    print(f"Expected: 3 partitions with all numbers distributed")
    total_elements = sum(len(partition) for partition in result_1e)
    print(f"✅ Correct!" if len(result_1e) == 3 and total_elements == 6 else "❌ Incorrect")

    # Test Exercise 1f
    print("\n📝 Exercise 1f: Key-Value Pairs")
    test_pairs = [("apple", 1), ("banana", 2), ("apple", 3), ("cherry", 1), ("banana", 4)]
    result_1f = exercise_1f(spark, test_pairs)
    print(f"Input: {test_pairs}")
    print(f"Output: {result_1f}")
    expected_1f = [("apple", 4), ("banana", 6), ("cherry", 1)]
    print(f"Expected: {expected_1f}")
    print(f"✅ Correct!" if result_1f == expected_1f else "❌ Incorrect")

    spark.stop()
    print("\n🎉 Exercise 1 completed!")
    print("💡 If you got any incorrect answers, review your implementation and try again.")
    print("📚 Ready to move on to Exercise 2!")


if __name__ == "__main__":
    run_exercises()