"""
Exercise 2 Solutions: Text Processing Challenge
==============================================

This file contains the complete solutions for Exercise 2.
Study these solutions to understand advanced text processing patterns.
"""

from pyspark.sql import SparkSession
from typing import List, Tuple, Dict
import re


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise2-Solutions")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_2a(spark: SparkSession) -> List[Tuple[str, int]]:
    """
    Read the sample book text and find the top 10 most frequent words.
    """
    # Read text file
    text_rdd = spark.sparkContext.textFile("../data/sample_book.txt")

    # Define stop words to exclude
    stop_words = {"that", "with", "were", "they", "this", "from", "have"}

    # Process text: split words, clean, filter, count
    word_counts = (
        text_rdd.flatMap(lambda line: line.split())
        .map(lambda word: re.sub(r"[^a-zA-Z]", "", word.lower()))
        .filter(lambda word: len(word) >= 4)
        .filter(lambda word: word not in stop_words)
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
        .take(10)
    )

    return word_counts


def exercise_2b(spark: SparkSession, text_lines: List[str]) -> Dict[str, List[str]]:
    """
    Categorize words by their starting letter.
    """
    # Create RDD from text lines
    text_rdd = spark.sparkContext.parallelize(text_lines)

    # Extract and clean words
    words_rdd = (
        text_rdd.flatMap(lambda line: line.split())
        .map(lambda word: re.sub(r"[^a-zA-Z]", "", word.lower()))
        .filter(lambda word: word.isalpha() and len(word) > 0)
    )

    # Group by starting letter
    by_letter = (
        words_rdd.map(lambda word: (word[0], word))
        .groupByKey()
        .mapValues(lambda words: list(set(words)))
        .collectAsMap()
    )

    return dict(by_letter)


def exercise_2c(spark: SparkSession) -> List[Tuple[str, int]]:
    """
    Analyze log file and count occurrences of each log level.
    """
    # Read log file
    logs_rdd = spark.sparkContext.textFile("../data/sample_logs.txt")

    # Extract log levels using regex
    log_levels = (
        logs_rdd.map(lambda line: re.search(r"(INFO|DEBUG|WARN|ERROR)", line))
        .filter(lambda match: match is not None)
        .map(lambda match: match.group(1))
        .map(lambda level: (level, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .collect()
    )

    return log_levels


def exercise_2d(
    spark: SparkSession, sentences: List[str]
) -> List[Tuple[str, int, List[str]]]:
    """
    Analyze sentences: return sentence, word count, and words longer than 5 characters.
    """
    # Create RDD from sentences
    sentences_rdd = spark.sparkContext.parallelize(sentences)

    def analyze_sentence(sentence):
        # Split into words and clean
        words = sentence.split()
        clean_words = [re.sub(r"[^a-zA-Z]", "", word.lower()) for word in words]
        clean_words = [word for word in clean_words if len(word) > 0]

        # Find long words (> 5 chars)
        long_words = [word for word in clean_words if len(word) > 5]

        return (sentence, len(clean_words), long_words)

    # Apply analysis to each sentence
    results = sentences_rdd.map(analyze_sentence).collect()

    return results


def exercise_2e(spark: SparkSession, text_data: List[str]) -> Tuple[int, int, float]:
    """
    Calculate text statistics: total words, unique words, and average word length.
    """
    # Create RDD from text data
    text_rdd = spark.sparkContext.parallelize(text_data)

    # Extract and clean all words
    words_rdd = (
        text_rdd.flatMap(lambda line: line.split())
        .map(lambda word: re.sub(r"[^a-zA-Z]", "", word.lower()))
        .filter(lambda word: len(word) > 0)
    )

    # Calculate statistics
    total_words = words_rdd.count()
    unique_words_rdd = words_rdd.distinct()
    unique_count = unique_words_rdd.count()

    # Calculate average word length
    total_length = unique_words_rdd.map(lambda word: len(word)).sum()
    avg_length = total_length / unique_count if unique_count > 0 else 0.0

    return (total_words, unique_count, avg_length)


def exercise_2f(spark: SparkSession) -> List[Tuple[str, int]]:
    """
    Find the most common word pairs (bigrams) in the sample book.
    """
    # Read text file
    text_rdd = spark.sparkContext.textFile("../data/sample_book.txt")

    # Clean and prepare words
    words_rdd = (
        text_rdd.flatMap(lambda line: line.split())
        .map(lambda word: re.sub(r"[^a-zA-Z]", "", word.lower()))
        .filter(lambda word: len(word) >= 3)
    )

    # Collect words to create bigrams
    all_words = words_rdd.collect()

    # Create bigrams
    bigrams = []
    for i in range(len(all_words) - 1):
        bigram = f"{all_words[i]} {all_words[i + 1]}"
        bigrams.append(bigram)

    # Count bigrams
    bigrams_rdd = spark.sparkContext.parallelize(bigrams)
    bigram_counts = (
        bigrams_rdd.map(lambda bigram: (bigram, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
        .take(10)
    )

    return bigram_counts


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 2 Solutions")
    print("=" * 30)

    # Create sample data first
    from exercise_2 import create_sample_data

    create_sample_data()

    # Solution 2a
    print("\n2a. Top Words in Book")
    try:
        result_2a = exercise_2a(spark)
        print("Top 10 words:")
        for word, count in result_2a:
            print(f"  {word}: {count}")
    except Exception as e:
        print(f"Error: {e}")

    # Solution 2b
    print("\n2b. Words by Starting Letter")
    test_text = ["The quick brown fox", "jumps over the lazy dog"]
    result_2b = exercise_2b(spark, test_text)
    print(f"Input: {test_text}")
    print("Categorized words:")
    for letter in sorted(result_2b.keys()):
        print(f"  {letter}: {result_2b[letter]}")

    # Solution 2c
    print("\n2c. Log Level Analysis")
    try:
        result_2c = exercise_2c(spark)
        print("Log level counts:")
        for level, count in result_2c:
            print(f"  {level}: {count}")
    except Exception as e:
        print(f"Error: {e}")

    # Solution 2d
    print("\n2d. Sentence Analysis")
    test_sentences = [
        "The quick brown fox jumps over the lazy dog.",
        "Apache Spark is a powerful distributed computing framework.",
        "Data processing at scale requires efficient algorithms.",
    ]
    result_2d = exercise_2d(spark, test_sentences)
    print("Sentence analysis:")
    for sentence, word_count, long_words in result_2d:
        print(f"  '{sentence[:30]}...' -> {word_count} words, long: {long_words}")

    # Solution 2e
    print("\n2e. Text Statistics")
    test_stats_text = [
        "Hello world from Apache Spark",
        "Spark makes big data processing easy",
        "Hello again from the world of data",
    ]
    result_2e = exercise_2e(spark, test_stats_text)
    print(f"Input: {test_stats_text}")
    print(f"Total words: {result_2e[0]}")
    print(f"Unique words: {result_2e[1]}")
    print(f"Average word length: {result_2e[2]:.2f}")

    # Solution 2f
    print("\n2f. Common Bigrams")
    try:
        result_2f = exercise_2f(spark)
        print("Top 10 bigrams:")
        for bigram, count in result_2f:
            print(f"  '{bigram}': {count}")
    except Exception as e:
        print(f"Error: {e}")

    spark.stop()
    print("\n🎓 All solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()
