"""
Exercise 1 Solutions: Basic RDD Operations
=========================================

This file contains the complete solutions for Exercise 1.
Study these solutions to understand the correct implementation patterns.
"""

from typing import List, Tuple

from pyspark.sql import SparkSession


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise1-Solutions")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_1a(spark: SparkSession, numbers: List[int]) -> List[int]:
    """
    Create an RDD from the given list of numbers and return all even numbers.
    """
    # Create RDD from numbers list
    numbers_rdd = spark.sparkContext.parallelize(numbers)

    # Filter for even numbers and collect results
    even_numbers = numbers_rdd.filter(lambda x: x % 2 == 0).collect()

    return even_numbers


def exercise_1b(spark: SparkSession, words: List[str]) -> int:
    """
    Count the total number of characters across all words.
    """
    # Create RDD from words list
    words_rdd = spark.sparkContext.parallelize(words)

    # Map each word to its length and sum all lengths
    total_chars = words_rdd.map(lambda word: len(word)).sum()

    return total_chars


def exercise_1c(spark: SparkSession, data: List[int]) -> Tuple[int, int, float]:
    """
    Calculate statistics for the given data: count, sum, and average.
    """
    # Create RDD from data list
    data_rdd = spark.sparkContext.parallelize(data)

    # Calculate statistics using RDD actions
    count = data_rdd.count()
    total_sum = data_rdd.sum()
    average = total_sum / count

    return (count, total_sum, average)


def exercise_1d(spark: SparkSession, text_data: List[str]) -> List[Tuple[str, int]]:
    """
    Perform word count on the given text data.
    """
    # Create RDD from text data
    text_rdd = spark.sparkContext.parallelize(text_data)

    # Split lines into words, convert to lowercase, create pairs, count, and sort
    word_counts = (
        text_rdd.flatMap(lambda line: line.split())
        .map(lambda word: word.lower())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .collect()
    )

    return word_counts


def exercise_1e(
    spark: SparkSession, numbers: List[int], num_partitions: int
) -> List[List[int]]:
    """
    Create an RDD with specified number of partitions and return the partition contents.
    """
    # Create RDD with specified number of partitions
    numbers_rdd = spark.sparkContext.parallelize(numbers, num_partitions)

    # Use glom() to get partition contents and collect
    partition_contents = numbers_rdd.glom().collect()

    return partition_contents


def exercise_1f(
    spark: SparkSession, pairs: List[Tuple[str, int]]
) -> List[Tuple[str, int]]:
    """
    Work with key-value pairs: group by key and sum the values.
    """
    # Create RDD from pairs
    pairs_rdd = spark.sparkContext.parallelize(pairs)

    # Sum values by key, sort by key, and collect
    summed_pairs = pairs_rdd.reduceByKey(lambda a, b: a + b).sortByKey().collect()

    return summed_pairs


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 1 Solutions")
    print("=" * 30)

    # Solution 1a
    print("\n1a. Filter Even Numbers")
    test_numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    result_1a = exercise_1a(spark, test_numbers)
    print(f"Input: {test_numbers}")
    print(f"Output: {result_1a}")

    # Solution 1b
    print("\n1b. Count Characters")
    test_words = ["hello", "world", "apache", "spark"]
    result_1b = exercise_1b(spark, test_words)
    print(f"Input: {test_words}")
    print(f"Total characters: {result_1b}")

    # Solution 1c
    print("\n1c. Calculate Statistics")
    test_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    result_1c = exercise_1c(spark, test_data)
    print(f"Input: {test_data}")
    print(f"Statistics (count, sum, avg): {result_1c}")

    # Solution 1d
    print("\n1d. Word Count")
    test_text = ["hello world", "hello spark", "world of spark"]
    result_1d = exercise_1d(spark, test_text)
    print(f"Input: {test_text}")
    print(f"Word counts: {result_1d}")

    # Solution 1e
    print("\n1e. Partition Contents")
    test_nums = [1, 2, 3, 4, 5, 6]
    result_1e = exercise_1e(spark, test_nums, 3)
    print(f"Input: {test_nums}, partitions: 3")
    print(f"Partition contents: {result_1e}")

    # Solution 1f
    print("\n1f. Key-Value Pairs")
    test_pairs = [
        ("apple", 1),
        ("banana", 2),
        ("apple", 3),
        ("cherry", 1),
        ("banana", 4),
    ]
    result_1f = exercise_1f(spark, test_pairs)
    print(f"Input: {test_pairs}")
    print(f"Summed by key: {result_1f}")

    spark.stop()
    print("\n🎓 All solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()
