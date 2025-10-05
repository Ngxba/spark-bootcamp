"""
Exercise 2: Text Processing Challenge
====================================

This exercise focuses on more advanced text processing using RDDs.
You'll work with real text data and implement common text analysis tasks.

Instructions:
1. Complete each function according to the docstring requirements
2. Run the script to test your implementations
3. Pay attention to edge cases and data cleaning

Learning Goals:
- Advanced text processing with RDDs
- String manipulation and regular expressions
- Working with file I/O in Spark
- Implementing complex transformations

Run this file: python exercise_2.py
"""

from pyspark.sql import SparkSession
from typing import List, Tuple, Dict
import os


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise2-TextProcessing")
        .master("local[*]")
        .getOrCreate()
    )


def create_sample_data():
    """Create sample text files for the exercises."""
    os.makedirs("../data", exist_ok=True)

    # Sample book text (simplified)
    book_text = """
        Chapter 1: The Beginning

        It was the best of times, it was the worst of times, it was the age of wisdom,
        it was the age of foolishness, it was the epoch of belief, it was the epoch of
        incredulity, it was the season of Light, it was the season of Darkness, it was
        the spring of hope, it was the winter of despair.

        Chapter 2: The Journey

        We had everything before us, we had nothing before us, we were all going direct
        to Heaven, we were all going direct the other way. In short, the period was so
        far like the present period, that some of its noisiest authorities insisted on
        its being received, for good or for evil, in the superlative degree of comparison only.

        Chapter 3: The End

        There were a king with a large jaw and a queen with a plain face, on the throne
        of England; there were a king with a large jaw and a queen with a fair face, on
        the throne of France. In both countries it was clearer than crystal to the lords
        of the State preserves of loaves and fishes, that things in general were settled
        for ever.
    """.strip()

    with open("../data/sample_book.txt", "w") as f:
        f.write(book_text)

    # Sample log data
    log_data = """
        2023-10-01 08:00:01 INFO Application started successfully
        2023-10-01 08:00:02 DEBUG Loading configuration from config.yml
        2023-10-01 08:00:03 INFO Database connection established
        2023-10-01 08:00:05 WARN High memory usage detected: 85%
        2023-10-01 08:00:07 ERROR Failed to connect to external API
        2023-10-01 08:00:08 INFO Retrying API connection...
        2023-10-01 08:00:10 INFO API connection successful
        2023-10-01 08:00:15 DEBUG Processing batch of 1000 records
        2023-10-01 08:00:20 WARN Slow query detected: 5.2 seconds
        2023-10-01 08:00:25 INFO Batch processing completed
    """.strip()

    with open("../data/sample_logs.txt", "w") as f:
        f.write(log_data)

    print("Sample data files created successfully!")


def exercise_2a(spark: SparkSession) -> List[Tuple[str, int]]:  # type: ignore
    """
    Read the sample book text and find the top 10 most frequent words (case-insensitive).
    Exclude common stop words and words shorter than 4 characters.

    Args:
        spark: SparkSession instance

    Returns:
        List of (word, count) tuples, sorted by count (descending)

    Requirements:
    - Read from ../data/sample_book.txt
    - Convert to lowercase
    - Remove punctuation
    - Filter out words < 4 characters
    - Exclude stop words: ["that", "with", "were", "they", "this", "from", "have"]
    - Return top 10 by frequency
    """
    # TODO: Implement this function
    pass


def exercise_2b(spark: SparkSession, text_lines: List[str]) -> Dict[str, List[str]]:  # type: ignore
    """
    Categorize words by their starting letter.

    Args:
        spark: SparkSession instance
        text_lines: List of text lines

    Returns:
        Dictionary where keys are letters (a-z) and values are lists of unique words
        starting with that letter

    Example:
        Input: ["apple banana", "cherry date"]
        Output: {"a": ["apple"], "b": ["banana"], "c": ["cherry"], "d": ["date"]}

    Requirements:
    - Convert to lowercase
    - Remove punctuation
    - Only include alphabetic words
    - Return unique words for each letter
    """
    # TODO: Implement this function
    pass


def exercise_2c(spark: SparkSession) -> List[Tuple[str, int]]:  # type: ignore
    """
    Analyze log file and count occurrences of each log level.

    Args:
        spark: SparkSession instance

    Returns:
        List of (log_level, count) tuples, sorted by log level

    Requirements:
    - Read from ../data/sample_logs.txt
    - Extract log levels (INFO, DEBUG, WARN, ERROR)
    - Count occurrences of each level
    - Return sorted by log level alphabetically
    """
    # TODO: Implement this function
    pass


def exercise_2d(  # type: ignore
    spark: SparkSession, sentences: List[str]
) -> List[Tuple[str, int, List[str]]]:
    """
    Analyze sentences: return sentence, word count, and words longer than 5 characters.

    Args:
        spark: SparkSession instance
        sentences: List of sentences

    Returns:
        List of (sentence, word_count, long_words) tuples

    Requirements:
    - Keep original sentence text
    - Count total words in each sentence
    - Find words longer than 5 characters
    - Clean words (remove punctuation, lowercase)
    """
    # TODO: Implement this function
    pass


def exercise_2e(spark: SparkSession, text_data: List[str]) -> Tuple[int, int, float]:  # type: ignore
    """
    Calculate text statistics: total words, unique words, and average word length.

    Args:
        spark: SparkSession instance
        text_data: List of text lines

    Returns:
        Tuple of (total_words, unique_words, average_word_length)

    Requirements:
    - Count total words across all lines
    - Count unique words (case-insensitive)
    - Calculate average length of unique words
    - Clean words (remove punctuation, lowercase, non-empty)
    """
    # TODO: Implement this function
    pass


def exercise_2f(spark: SparkSession) -> List[Tuple[str, int]]:  # type: ignore
    """
    Find the most common word pairs (bigrams) in the sample book.

    Args:
        spark: SparkSession instance

    Returns:
        List of (bigram, count) tuples, top 10 by frequency

    Requirements:
    - Read from ../data/sample_book.txt
    - Create word pairs from consecutive words
    - Clean words (lowercase, remove punctuation, min 3 chars)
    - Return top 10 bigrams by frequency
    - Format bigrams as "word1 word2"
    """
    # TODO: Implement this function
    pass


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 2: Text Processing Challenge")
    print("=" * 55)

    # Create sample data
    create_sample_data()

    # Test Exercise 2a
    print("\n📝 Exercise 2a: Top Words in Book")
    try:
        result_2a = exercise_2a(spark)
        print(f"Top 10 words: {result_2a}")
        print("✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2b
    print("\n📝 Exercise 2b: Categorize Words by Starting Letter")
    test_text = ["The quick brown fox", "jumps over the lazy dog"]
    try:
        result_2b = exercise_2b(spark, test_text)
        print(f"Input: {test_text}")
        print(f"Categories: {dict(sorted(result_2b.items()))}")
        print("✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2c
    print("\n📝 Exercise 2c: Log Level Analysis")
    try:
        result_2c = exercise_2c(spark)
        print(f"Log levels: {result_2c}")
        print("✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2d
    print("\n📝 Exercise 2d: Sentence Analysis")
    test_sentences = [
        "The quick brown fox jumps over the lazy dog.",
        "Apache Spark is a powerful distributed computing framework.",
        "Data processing at scale requires efficient algorithms.",
    ]
    try:
        result_2d = exercise_2d(spark, test_sentences)
        print("Sentence analysis:")
        for sentence, word_count, long_words in result_2d:
            print(f"  '{sentence}' -> {word_count} words, long words: {long_words}")
        print("✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2e
    print("\n📝 Exercise 2e: Text Statistics")
    test_stats_text = [
        "Hello world from Apache Spark",
        "Spark makes big data processing easy",
        "Hello again from the world of data",
    ]
    try:
        result_2e = exercise_2e(spark, test_stats_text)
        print(f"Input: {test_stats_text}")
        print(f"Statistics (total, unique, avg_length): {result_2e}")
        print("✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2f
    print("\n📝 Exercise 2f: Common Bigrams")
    try:
        result_2f = exercise_2f(spark)
        print(f"Top 10 bigrams: {result_2f}")
        print("✅ Function executed successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 2 completed!")
    print("💡 This exercise demonstrates real-world text processing scenarios.")
    print("📚 Ready to move on to Exercise 3!")


if __name__ == "__main__":
    run_exercises()
