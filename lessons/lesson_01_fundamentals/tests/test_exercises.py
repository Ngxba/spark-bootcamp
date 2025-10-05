"""
Test Exercise Implementations
============================

This module contains tests to validate exercise implementations.
These tests check that student solutions are working correctly.

Run tests: python -m pytest test_exercises.py -v
"""

import pytest
import os
from pyspark.sql import SparkSession
from solutions.exercise_1_solution import (
    exercise_1a,
    exercise_1b,
    exercise_1c,
    exercise_1d,
    exercise_1e,
    exercise_1f,
)
from solutions.exercise_2_solution import exercise_2b, exercise_2d, exercise_2e
from solutions.exercise_3_solution import (
    exercise_3a,
    exercise_3c,
    exercise_3d,
    exercise_3f,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("TestExercises").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


class TestExercise1:
    """Test Exercise 1 implementations."""

    def test_exercise_1a_functionality(self, spark):
        """Test exercise_1a functionality."""
        try:
            # Test with known input
            test_input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            result = exercise_1a(spark, test_input)

            # Check if result is correct
            expected = [2, 4, 6, 8, 10]
            assert result == expected, f"Expected {expected}, got {result}"

        except Exception as e:
            pytest.fail(f"exercise_1a failed with error: {e}")

    def test_exercise_1b_functionality(self, spark):
        """Test exercise_1b functionality."""
        try:
            test_input = ["hello", "world", "apache", "spark"]
            result = exercise_1b(spark, test_input)

            expected = 21  # 5 + 5 + 6 + 5
            assert result == expected, f"Expected {expected}, got {result}"

        except Exception as e:
            pytest.fail(f"exercise_1b failed with error: {e}")

    def test_exercise_1c_functionality(self, spark):
        """Test exercise_1c functionality."""
        try:
            test_input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            result = exercise_1c(spark, test_input)

            expected = (10, 55, 5.5)
            assert result == expected, f"Expected {expected}, got {result}"

        except Exception as e:
            pytest.fail(f"exercise_1c failed with error: {e}")

    def test_exercise_1d_functionality(self, spark):
        """Test exercise_1d functionality."""
        try:
            test_input = ["hello world", "hello spark", "world of spark"]
            result = exercise_1d(spark, test_input)

            # Check if result is a list of tuples
            assert isinstance(result, list)
            if result:  # Only check if result is not empty
                assert all(
                    isinstance(item, tuple) and len(item) == 2 for item in result
                )
                assert all(
                    isinstance(item[0], str) and isinstance(item[1], int)
                    for item in result
                )

        except Exception as e:
            pytest.fail(f"exercise_1d failed with error: {e}")

    def test_exercise_1e_functionality(self, spark):
        """Test exercise_1e functionality."""
        try:
            test_input = [1, 2, 3, 4, 5, 6]
            num_partitions = 3
            result = exercise_1e(spark, test_input, num_partitions)

            # Check basic structure
            assert isinstance(result, list)
            assert len(result) == num_partitions

            # Check that all elements are preserved
            flattened = [item for partition in result for item in partition]
            assert len(flattened) == len(test_input)

        except Exception as e:
            pytest.fail(f"exercise_1e failed with error: {e}")

    def test_exercise_1f_functionality(self, spark):
        """Test exercise_1f functionality."""
        try:
            test_input = [
                ("apple", 1),
                ("banana", 2),
                ("apple", 3),
                ("cherry", 1),
                ("banana", 4),
            ]
            result = exercise_1f(spark, test_input)

            # Convert to dict for easier checking
            result_dict = dict(result) if result else {}

            # Check expected sums
            if "apple" in result_dict:
                assert result_dict["apple"] == 4
            if "banana" in result_dict:
                assert result_dict["banana"] == 6
            if "cherry" in result_dict:
                assert result_dict["cherry"] == 1

        except Exception as e:
            pytest.fail(f"exercise_1f failed with error: {e}")


class TestExercise2:
    """Test Exercise 2 implementations."""

    def test_exercise_2b_functionality(self, spark):
        """Test exercise_2b functionality."""
        try:
            test_input = ["The quick brown fox", "jumps over the lazy dog"]
            result = exercise_2b(spark, test_input)

            # Check basic structure
            assert isinstance(result, dict)

            # Check that all letters have lists of words
            for letter, words in result.items():
                assert isinstance(letter, str)
                assert len(letter) == 1
                assert letter.islower()
                assert isinstance(words, list)

        except Exception as e:
            pytest.fail(f"exercise_2b failed with error: {e}")

    def test_exercise_2d_functionality(self, spark):
        """Test exercise_2d functionality."""
        try:
            test_input = [
                "The quick brown fox jumps over the lazy dog.",
                "Apache Spark is a powerful distributed computing framework.",
            ]
            result = exercise_2d(spark, test_input)

            # Check basic structure
            assert isinstance(result, list)
            assert len(result) == len(test_input)

            for item in result:
                assert isinstance(item, tuple)
                assert len(item) == 3
                assert isinstance(item[0], str)  # sentence
                assert isinstance(item[1], int)  # word count
                assert isinstance(item[2], list)  # long words

        except Exception as e:
            pytest.fail(f"exercise_2d failed with error: {e}")

    def test_exercise_2e_functionality(self, spark):
        """Test exercise_2e functionality."""
        try:
            test_input = [
                "Hello world from Apache Spark",
                "Spark makes big data processing easy",
                "Hello again from the world of data",
            ]
            result = exercise_2e(spark, test_input)

            # Check basic structure
            assert isinstance(result, tuple)
            assert len(result) == 3
            assert isinstance(result[0], int)  # total words
            assert isinstance(result[1], int)  # unique words
            assert isinstance(result[2], float)  # average length

            # Basic sanity checks
            assert result[0] > 0  # should have some words
            assert result[1] > 0  # should have some unique words
            assert result[2] > 0  # average length should be positive

        except Exception as e:
            pytest.fail(f"exercise_2e failed with error: {e}")


class TestExercise3:
    """Test Exercise 3 implementations."""

    def test_exercise_3a_functionality(self, spark):
        """Test exercise_3a functionality."""
        try:
            # Create test sales data
            test_data = [
                {"product_category": "Electronics", "amount": 100.0},
                {"product_category": "Books", "amount": 50.0},
                {"product_category": "Electronics", "amount": 200.0},
                {"product_category": "Books", "amount": 75.0},
            ]

            result = exercise_3a(spark, test_data)

            # Check basic structure
            assert isinstance(result, list)
            if result:
                for item in result:
                    assert isinstance(item, tuple)
                    assert len(item) == 2
                    assert isinstance(item[0], str)  # category
                    assert isinstance(item[1], (int, float))  # amount

        except Exception as e:
            pytest.fail(f"exercise_3a failed with error: {e}")

    def test_exercise_3c_functionality(self, spark):
        """Test exercise_3c functionality."""
        try:
            test_input = [12, 15, 20]
            result = exercise_3c(spark, test_input)

            # Check basic structure
            assert isinstance(result, list)
            assert len(result) == len(test_input)

            for item in result:
                assert isinstance(item, tuple)
                assert len(item) == 2
                assert isinstance(item[0], int)  # number
                assert isinstance(item[1], list)  # factors

                # Check that factors are valid
                number, factors = item
                if number > 0:
                    assert all(number % factor == 0 for factor in factors)
                    assert 1 in factors if factors else True
                    assert number in factors if factors else True

        except Exception as e:
            pytest.fail(f"exercise_3c failed with error: {e}")

    def test_exercise_3d_functionality(self, spark):
        """Test exercise_3d functionality."""
        try:
            test_input = list(range(1, 1001))  # Smaller dataset for testing
            result = exercise_3d(spark, test_input)

            # Check basic structure
            assert isinstance(result, tuple)
            assert len(result) == 2
            assert isinstance(result[0], (int, float))  # time without cache
            assert isinstance(result[1], (int, float))  # time with cache

            # Basic sanity checks
            assert result[0] >= 0
            assert result[1] >= 0

        except Exception as e:
            pytest.fail(f"exercise_3d failed with error: {e}")

    def test_exercise_3f_functionality(self, spark):
        """Test exercise_3f functionality."""
        try:
            test_input = [64, 34, 25, 12, 22, 11, 90, 88, 76, 50, 5]
            k = 5
            result = exercise_3f(spark, test_input, k)

            # Check basic structure
            assert isinstance(result, list)
            assert len(result) <= k  # Should return at most k elements

            # Check that result is sorted in descending order
            if len(result) > 1:
                for i in range(len(result) - 1):
                    assert result[i] >= result[i + 1]

            # Check that all elements are from the original data
            assert all(x in test_input for x in result)

        except Exception as e:
            pytest.fail(f"exercise_3f failed with error: {e}")


class TestAllExercises:
    """Integration tests for all exercises."""

    def test_all_exercise_files_exist(self):
        """Test that all exercise files exist."""
        exercise_dir = os.path.join(os.path.dirname(__file__), "..", "exercises")

        expected_files = ["exercise_1.py", "exercise_2.py", "exercise_3.py"]

        for filename in expected_files:
            filepath = os.path.join(exercise_dir, filename)
            assert os.path.exists(filepath), f"Exercise file {filename} not found"

    def test_spark_context_cleanup(self, spark):
        """Test that Spark context is properly managed."""
        # This test ensures our fixture properly manages Spark context
        assert spark is not None
        assert spark.sparkContext is not None

        # Create a simple RDD to verify Spark is working
        rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
        count = rdd.count()
        assert count == 5


if __name__ == "__main__":
    # Allow running this file directly for quick testing
    pytest.main([__file__, "-v"])
