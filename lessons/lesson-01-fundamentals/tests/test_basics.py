"""
Test Basic Spark Concepts
=========================

This module contains tests to validate understanding of basic Spark concepts.
Run these tests to ensure you've grasped the fundamental principles.

Run tests: python -m pytest test_basics.py -v
"""

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder.appName("TestBasics").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def sc(spark):
    """Get SparkContext from SparkSession."""
    return spark.sparkContext


class TestSparkSetup:
    """Test Spark environment setup."""

    def test_spark_session_creation(self, spark):
        """Test that Spark session is created successfully."""
        assert spark is not None
        assert isinstance(spark, SparkSession)

    def test_spark_context_access(self, sc):
        """Test that SparkContext is accessible."""
        assert sc is not None
        assert isinstance(sc, SparkContext)

    def test_spark_version(self, spark):
        """Test that Spark version is accessible."""
        version = spark.version
        assert version is not None
        assert isinstance(version, str)
        assert len(version) > 0

    def test_application_name(self, sc):
        """Test that application name is set correctly."""
        app_name = sc.appName
        assert app_name == "TestBasics"

    def test_default_parallelism(self, sc):
        """Test that default parallelism is set correctly."""
        parallelism = sc.defaultParallelism
        assert parallelism >= 1


class TestRDDCreation:
    """Test RDD creation methods."""

    def test_parallelize_list(self, sc):
        """Test creating RDD from Python list."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        assert rdd.count() == 5
        assert rdd.collect() == data

    def test_parallelize_with_partitions(self, sc):
        """Test creating RDD with specific number of partitions."""
        data = [1, 2, 3, 4, 5, 6, 7, 8]
        rdd = sc.parallelize(data, 4)

        assert rdd.getNumPartitions() == 4
        assert rdd.count() == 8

    def test_range_rdd(self, sc):
        """Test creating RDD from range."""
        rdd = sc.range(1, 11)  # 1 to 10

        assert rdd.count() == 10
        assert rdd.first() == 1
        assert rdd.collect() == list(range(1, 11))

    def test_empty_rdd(self, sc):
        """Test creating empty RDD."""
        rdd = sc.parallelize([])

        assert rdd.count() == 0
        assert rdd.collect() == []

    def test_partition_distribution(self, sc):
        """Test how data is distributed across partitions."""
        data = list(range(1, 9))  # [1, 2, 3, 4, 5, 6, 7, 8]
        rdd = sc.parallelize(data, 4)

        partitions = rdd.glom().collect()
        assert len(partitions) == 4

        # Check that all data is preserved
        flattened = [item for partition in partitions for item in partition]
        assert sorted(flattened) == data


class TestTransformations:
    """Test RDD transformations."""

    def test_map_transformation(self, sc):
        """Test map transformation."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        doubled = rdd.map(lambda x: x * 2)
        assert doubled.collect() == [2, 4, 6, 8, 10]

    def test_filter_transformation(self, sc):
        """Test filter transformation."""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        rdd = sc.parallelize(data)

        evens = rdd.filter(lambda x: x % 2 == 0)
        assert evens.collect() == [2, 4, 6, 8, 10]

    def test_flatmap_transformation(self, sc):
        """Test flatMap transformation."""
        data = ["hello world", "spark is great"]
        rdd = sc.parallelize(data)

        words = rdd.flatMap(lambda line: line.split())
        assert words.collect() == ["hello", "world", "spark", "is", "great"]

    def test_distinct_transformation(self, sc):
        """Test distinct transformation."""
        data = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
        rdd = sc.parallelize(data)

        unique = rdd.distinct()
        assert sorted(unique.collect()) == [1, 2, 3, 4]

    def test_sample_transformation(self, sc):
        """Test sample transformation."""
        data = list(range(1, 101))  # 1 to 100
        rdd = sc.parallelize(data)

        # Sample 50% with seed for reproducibility
        sample = rdd.sample(withReplacement=False, fraction=0.5, seed=42)
        sample_data = sample.collect()

        # Should be approximately 50 elements (±20% tolerance)
        assert 40 <= len(sample_data) <= 60
        # All sampled elements should be from original data
        assert all(x in data for x in sample_data)

    def test_chained_transformations(self, sc):
        """Test chaining multiple transformations."""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        rdd = sc.parallelize(data)

        result = (
            rdd.filter(lambda x: x % 2 == 0)
            .map(lambda x: x * x)
            .filter(lambda x: x > 10)
        )

        assert result.collect() == [16, 36, 64, 100]


class TestActions:
    """Test RDD actions."""

    def test_collect_action(self, sc):
        """Test collect action."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        result = rdd.collect()
        assert result == data
        assert isinstance(result, list)

    def test_count_action(self, sc):
        """Test count action."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        count = rdd.count()
        assert count == 5
        assert isinstance(count, int)

    def test_first_action(self, sc):
        """Test first action."""
        data = [10, 20, 30, 40, 50]
        rdd = sc.parallelize(data)

        first = rdd.first()
        assert first == 10

    def test_take_action(self, sc):
        """Test take action."""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        rdd = sc.parallelize(data)

        first_three = rdd.take(3)
        assert first_three == [1, 2, 3]
        assert len(first_three) == 3

    def test_reduce_action(self, sc):
        """Test reduce action."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        sum_result = rdd.reduce(lambda a, b: a + b)
        assert sum_result == 15

        max_result = rdd.reduce(lambda a, b: max(a, b))
        assert max_result == 5

    def test_fold_action(self, sc):
        """Test fold action."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        # Sum with initial value of 10
        result = rdd.fold(10, lambda a, b: a + b)
        # Note: fold applies the initial value per partition + once more
        # So the exact result depends on number of partitions
        assert result >= 25  # At least 15 (sum) + 10 (initial)

    def test_foreach_action(self, sc):
        """Test foreach action."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        # Create a list to collect results (note: this is for testing only)
        results = []

        def collect_item(x):
            results.append(x * 2)

        # Note: foreach doesn't return anything
        result = rdd.foreach(collect_item)
        assert result is None


class TestKeyValueOperations:
    """Test key-value pair operations."""

    def test_pair_rdd_creation(self, sc):
        """Test creating pair RDDs."""
        data = [("apple", 1), ("banana", 2), ("cherry", 3)]
        rdd = sc.parallelize(data)

        assert rdd.count() == 3
        assert rdd.collect() == data

    def test_map_values(self, sc):
        """Test mapValues transformation."""
        data = [("a", 1), ("b", 2), ("c", 3)]
        rdd = sc.parallelize(data)

        doubled = rdd.mapValues(lambda x: x * 2)
        assert doubled.collect() == [("a", 2), ("b", 4), ("c", 6)]

    def test_reduce_by_key(self, sc):
        """Test reduceByKey transformation."""
        data = [("apple", 1), ("banana", 2), ("apple", 3), ("banana", 4)]
        rdd = sc.parallelize(data)

        reduced = rdd.reduceByKey(lambda a, b: a + b)
        result = dict(reduced.collect())

        assert result["apple"] == 4
        assert result["banana"] == 6

    def test_group_by_key(self, sc):
        """Test groupByKey transformation."""
        data = [("apple", 1), ("banana", 2), ("apple", 3), ("banana", 4)]
        rdd = sc.parallelize(data)

        grouped = rdd.groupByKey()
        result = grouped.mapValues(list).collect()
        result_dict = dict(result)

        assert sorted(result_dict["apple"]) == [1, 3]
        assert sorted(result_dict["banana"]) == [2, 4]

    def test_keys_and_values(self, sc):
        """Test keys and values transformations."""
        data = [("apple", 1), ("banana", 2), ("cherry", 3)]
        rdd = sc.parallelize(data)

        keys = rdd.keys().collect()
        values = rdd.values().collect()

        assert keys == ["apple", "banana", "cherry"]
        assert values == [1, 2, 3]

    def test_sort_by_key(self, sc):
        """Test sortByKey transformation."""
        data = [("cherry", 3), ("apple", 1), ("banana", 2)]
        rdd = sc.parallelize(data)

        sorted_rdd = rdd.sortByKey()
        assert sorted_rdd.collect() == [("apple", 1), ("banana", 2), ("cherry", 3)]

        # Test descending order
        sorted_desc = rdd.sortByKey(ascending=False)
        assert sorted_desc.collect() == [("cherry", 3), ("banana", 2), ("apple", 1)]


class TestLazyEvaluation:
    """Test lazy evaluation concepts."""

    def test_transformations_are_lazy(self, sc):
        """Test that transformations don't execute immediately."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        # These transformations should not execute yet
        mapped = rdd.map(lambda x: x * 2)
        filtered = mapped.filter(lambda x: x > 5)

        # RDDs exist but computation hasn't happened
        assert mapped is not None
        assert filtered is not None

        # Only when we call an action does computation happen
        result = filtered.collect()
        assert result == [6, 8, 10]

    def test_multiple_actions_recompute(self, sc):
        """Test that multiple actions cause recomputation."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        # Add expensive transformation
        expensive = rdd.map(lambda x: x * 2)

        # Multiple actions will recompute the transformation
        count1 = expensive.count()
        count2 = expensive.count()
        first = expensive.first()

        assert count1 == 5
        assert count2 == 5
        assert first == 2


class TestCaching:
    """Test caching and persistence."""

    def test_cache_method(self, sc):
        """Test cache method."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        # Cache the RDD
        cached_rdd = rdd.cache()

        # Force materialization
        cached_rdd.count()

        # Check if cached
        assert cached_rdd.is_cached

        # Clean up
        cached_rdd.unpersist()
        assert not cached_rdd.is_cached

    def test_persist_method(self, sc):
        """Test persist method with storage level."""
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)

        # Persist with memory only
        from pyspark import StorageLevel

        rdd.persist(StorageLevel.MEMORY_ONLY)

        # Force materialization
        rdd.count()

        # Check storage level
        assert rdd.getStorageLevel() == StorageLevel.MEMORY_ONLY

        # Clean up
        rdd.unpersist()


class TestPartitions:
    """Test partition-related operations."""

    def test_get_num_partitions(self, sc):
        """Test getting number of partitions."""
        data = [1, 2, 3, 4, 5, 6, 7, 8]
        rdd = sc.parallelize(data, 4)

        assert rdd.getNumPartitions() == 4

    def test_glom_partitions(self, sc):
        """Test viewing partition contents with glom."""
        data = [1, 2, 3, 4, 5, 6]
        rdd = sc.parallelize(data, 3)

        partitions = rdd.glom().collect()
        assert len(partitions) == 3

        # All elements should be preserved
        flattened = [item for partition in partitions for item in partition]
        assert sorted(flattened) == data

    def test_map_partitions(self, sc):
        """Test mapPartitions transformation."""
        data = [1, 2, 3, 4, 5, 6, 7, 8]
        rdd = sc.parallelize(data, 2)

        def sum_partition(iterator):
            yield sum(iterator)

        partition_sums = rdd.mapPartitions(sum_partition).collect()
        assert len(partition_sums) == 2
        assert sum(partition_sums) == sum(data)

    def test_map_partitions_with_index(self, sc):
        """Test mapPartitionsWithIndex transformation."""
        data = [1, 2, 3, 4, 5, 6]
        rdd = sc.parallelize(data, 3)

        def add_partition_index(index, iterator):
            for item in iterator:
                yield (index, item)

        result = rdd.mapPartitionsWithIndex(add_partition_index).collect()

        # Check that we have partition indices
        partition_indices = set(item[0] for item in result)
        assert partition_indices == {0, 1, 2}

        # Check that all original data is preserved
        original_values = [item[1] for item in result]
        assert sorted(original_values) == data
