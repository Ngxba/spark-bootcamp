"""
Exercise 4: Testing Framework
============================

This exercise focuses on building a comprehensive testing infrastructure for
Spark applications. You'll create unit tests, integration tests, and data
quality validation frameworks.

Learning Goals:
- Build comprehensive test suites for Spark applications
- Create test data generators and fixtures
- Implement data quality validation
- Design test-driven development patterns for Spark

Instructions:
1. Create a testing framework for Spark applications
2. Implement data quality validation tests
3. Build test data generators
4. Create integration testing patterns

Run this file: python exercise_4.py
"""

import unittest
import pytest
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod
import tempfile
import shutil
from pathlib import Path
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, isnan, when, avg, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType


def exercise_4a_create_spark_test_base() -> type:
    """
    Create a base test class for Spark applications with common setup and utilities.

    Returns:
        SparkTestBase class with methods:
        - setUpClass(): Initialize Spark session
        - tearDownClass(): Stop Spark session
        - create_test_dataframe(data, schema): Create test DataFrames
        - assert_dataframe_equal(df1, df2): Compare DataFrames
        - assert_schema_equal(df, expected_schema): Validate schemas
        - get_temp_path(): Get temporary file paths
    """
    # TODO: Implement this function
    # 1. Create SparkTestBase class inheriting from unittest.TestCase
    # 2. Add class-level Spark session setup/teardown
    # 3. Create utility methods for DataFrame testing
    # 4. Add assertion methods for DataFrame comparison
    # 5. Include temporary file management
    pass


def exercise_4b_create_test_data_generator() -> type:
    """
    Create a test data generator for creating realistic test datasets.

    Returns:
        TestDataGenerator class with methods:
        - generate_customers(count: int) -> List[tuple]
        - generate_sales(count: int, customer_ids: List[str]) -> List[tuple]
        - generate_products(count: int) -> List[tuple]
        - generate_time_series(days: int, frequency: str) -> List[tuple]
        - add_data_quality_issues(data: List[tuple]) -> List[tuple]
    """
    # TODO: Implement this function
    # 1. Create TestDataGenerator class
    # 2. Add methods to generate realistic customer data
    # 3. Add methods to generate sales transactions
    # 4. Add methods to generate product catalogs
    # 5. Include data quality issue injection for testing
    pass


def exercise_4c_create_data_quality_validator() -> type:
    """
    Create a data quality validation framework.

    Returns:
        DataQualityValidator class with methods:
        - check_completeness(df: DataFrame) -> Dict[str, float]
        - check_uniqueness(df: DataFrame, columns: List[str]) -> Dict[str, int]
        - check_data_types(df: DataFrame, expected_schema: StructType) -> Dict[str, bool]
        - check_value_ranges(df: DataFrame, range_rules: Dict) -> Dict[str, bool]
        - check_referential_integrity(df1: DataFrame, df2: DataFrame, join_keys: List[str]) -> bool
        - generate_quality_report(df: DataFrame) -> Dict[str, Any]
    """
    # TODO: Implement this function
    # 1. Create DataQualityValidator class
    # 2. Implement completeness checks (null values)
    # 3. Add uniqueness validation
    # 4. Create data type validation
    # 5. Add business rule validation (ranges, patterns)
    # 6. Include referential integrity checks
    pass


def exercise_4d_create_transformation_tests() -> Dict[str, type]:
    """
    Create test classes for data transformation functions.

    Returns:
        Dictionary of test classes:
        - "TestDataCleaning": Tests for data cleaning functions
        - "TestDataEnrichment": Tests for data enrichment functions
        - "TestAggregations": Tests for aggregation functions
        - "TestBusinessLogic": Tests for business logic implementations
    """
    # TODO: Implement this function
    # 1. Create test class for data cleaning functions
    # 2. Create test class for data enrichment functions
    # 3. Create test class for aggregation functions
    # 4. Create test class for business logic
    # 5. Include edge cases and error conditions
    pass


def exercise_4e_create_integration_test_framework() -> type:
    """
    Create an integration testing framework for end-to-end pipeline testing.

    Returns:
        IntegrationTestFramework class with methods:
        - setup_test_environment() -> None
        - create_test_datasets() -> Dict[str, DataFrame]
        - run_pipeline_test(pipeline_func: Callable) -> Dict[str, Any]
        - validate_pipeline_output(output_df: DataFrame) -> bool
        - cleanup_test_environment() -> None
    """
    # TODO: Implement this function
    # 1. Create IntegrationTestFramework class
    # 2. Add test environment setup/teardown
    # 3. Create test dataset generation for pipelines
    # 4. Add pipeline execution and validation
    # 5. Include performance benchmarking
    pass


def exercise_4f_create_performance_test_framework() -> type:
    """
    Create a performance testing framework for Spark applications.

    Returns:
        PerformanceTestFramework class with methods:
        - benchmark_operation(operation: Callable, data_size: int) -> Dict[str, float]
        - test_scalability(operation: Callable, sizes: List[int]) -> Dict[int, float]
        - profile_memory_usage(operation: Callable) -> Dict[str, Any]
        - test_partition_optimization(df: DataFrame) -> Dict[str, Any]
        - generate_performance_report() -> str
    """
    # TODO: Implement this function
    # 1. Create PerformanceTestFramework class
    # 2. Add operation benchmarking capabilities
    # 3. Create scalability testing
    # 4. Add memory profiling
    # 5. Include partition optimization testing
    pass


def exercise_4g_create_data_contract_validator() -> type:
    """
    Create a data contract validation system for ensuring data consistency.

    Returns:
        DataContractValidator class with methods:
        - define_contract(schema: StructType, rules: Dict) -> None
        - validate_contract(df: DataFrame) -> Dict[str, bool]
        - check_schema_evolution(old_schema: StructType, new_schema: StructType) -> Dict[str, Any]
        - validate_data_lineage(source_df: DataFrame, target_df: DataFrame) -> bool
        - generate_contract_report(df: DataFrame) -> str
    """
    # TODO: Implement this function
    # 1. Create DataContractValidator class
    # 2. Add schema contract definition and validation
    # 3. Create data quality contract enforcement
    # 4. Add schema evolution checking
    # 5. Include data lineage validation
    pass


def exercise_4h_create_mock_data_sources() -> Dict[str, type]:
    """
    Create mock data sources for testing external dependencies.

    Returns:
        Dictionary of mock classes:
        - "MockDatabase": Mock database connections
        - "MockFileSystem": Mock file system operations
        - "MockAPIClient": Mock REST API clients
        - "MockStreamingSource": Mock streaming data sources
    """
    # TODO: Implement this function
    # 1. Create MockDatabase class for database testing
    # 2. Create MockFileSystem for file I/O testing
    # 3. Create MockAPIClient for API integration testing
    # 4. Create MockStreamingSource for stream testing
    # 5. Include realistic response simulation
    pass


# Sample data and utilities for testing
def create_sample_customer_data() -> List[tuple]:
    """Create sample customer data for testing"""
    return [
        ("C001", "Alice Johnson", 28, "alice@email.com", 75000.0, "New York", "2023-01-15"),
        ("C002", "Bob Smith", 35, "bob@email.com", 65000.0, "Chicago", "2023-02-20"),
        ("C003", "Charlie Brown", 42, "charlie@email.com", 85000.0, "Los Angeles", "2023-01-10"),
        ("C004", "Diana Wilson", 29, "diana@email.com", 70000.0, "Houston", "2023-03-05"),
        ("C005", "Eve Davis", 38, "eve@email.com", 90000.0, "Phoenix", "2023-02-14"),
        # Include some data quality issues for testing
        ("C006", None, 25, "invalid-email", 50000.0, "Seattle", "2023-01-20"),  # Missing name, invalid email
        ("C007", "Frank Miller", -5, "frank@email.com", -1000.0, "", ""),  # Invalid age, salary, missing city/date
        ("C001", "Alice Johnson", 28, "alice@email.com", 75000.0, "New York", "2023-01-15"),  # Duplicate
    ]


def create_sample_sales_data() -> List[tuple]:
    """Create sample sales data for testing"""
    return [
        ("T001", "C001", "P001", 2, 99.99, "2023-03-01"),
        ("T002", "C002", "P002", 1, 149.99, "2023-03-02"),
        ("T003", "C003", "P001", 3, 99.99, "2023-03-03"),
        ("T004", "C004", "P003", 1, 299.99, "2023-03-04"),
        ("T005", "C005", "P002", 2, 149.99, "2023-03-05"),
        # Data quality issues
        ("T006", "C999", "P001", 1, 99.99, "2023-03-06"),  # Non-existent customer
        ("T007", "C001", "P999", 1, 199.99, "2023-03-07"),  # Non-existent product
        ("T008", "C001", "P001", -1, 99.99, "2023-03-08"),  # Negative quantity
        ("T009", "C001", "P001", 1, -99.99, "2023-03-09"),  # Negative price
    ]


def demonstrate_testing_concepts():
    """Demonstrate testing concepts and best practices"""
    print("\n📚 Testing Framework Concepts")
    print("=" * 40)

    print("\n🧪 Types of Tests:")
    print("  • Unit Tests - Test individual functions")
    print("  • Integration Tests - Test component interactions")
    print("  • End-to-End Tests - Test complete workflows")
    print("  • Performance Tests - Test scalability and speed")
    print("  • Data Quality Tests - Validate data integrity")

    print("\n🎯 Testing Best Practices:")
    print("  • Test early and often")
    print("  • Use realistic test data")
    print("  • Test edge cases and error conditions")
    print("  • Isolate external dependencies")
    print("  • Automate test execution")
    print("  • Monitor test coverage")

    print("\n🔍 Spark-Specific Testing:")
    print("  • Use small datasets for unit tests")
    print("  • Test transformations independently")
    print("  • Validate schemas and data types")
    print("  • Test partition strategies")
    print("  • Verify data quality constraints")


def run_exercises():
    """Run all exercises and display results."""
    print("🚀 Running Exercise 4: Testing Framework")
    print("=" * 45)

    # Demonstrate testing concepts
    demonstrate_testing_concepts()

    # Test Exercise 4a
    print("\n📝 Exercise 4a: Create Spark Test Base")
    try:
        SparkTestBase = exercise_4a_create_spark_test_base()

        if SparkTestBase:
            print(f"✅ SparkTestBase class created: {SparkTestBase.__name__}")
            methods = [m for m in dir(SparkTestBase) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4b
    print("\n📝 Exercise 4b: Create Test Data Generator")
    try:
        TestDataGenerator = exercise_4b_create_test_data_generator()

        if TestDataGenerator:
            print(f"✅ TestDataGenerator class created: {TestDataGenerator.__name__}")
            methods = [m for m in dir(TestDataGenerator) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4c
    print("\n📝 Exercise 4c: Create Data Quality Validator")
    try:
        DataQualityValidator = exercise_4c_create_data_quality_validator()

        if DataQualityValidator:
            print(f"✅ DataQualityValidator class created: {DataQualityValidator.__name__}")
            methods = [m for m in dir(DataQualityValidator) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4d
    print("\n📝 Exercise 4d: Create Transformation Tests")
    try:
        test_classes = exercise_4d_create_transformation_tests()

        if test_classes:
            print(f"✅ {len(test_classes)} test classes created:")
            for name, test_class in test_classes.items():
                print(f"  - {name}: {test_class.__name__}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4e
    print("\n📝 Exercise 4e: Create Integration Test Framework")
    try:
        IntegrationTestFramework = exercise_4e_create_integration_test_framework()

        if IntegrationTestFramework:
            print(f"✅ IntegrationTestFramework class created: {IntegrationTestFramework.__name__}")
            methods = [m for m in dir(IntegrationTestFramework) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4f
    print("\n📝 Exercise 4f: Create Performance Test Framework")
    try:
        PerformanceTestFramework = exercise_4f_create_performance_test_framework()

        if PerformanceTestFramework:
            print(f"✅ PerformanceTestFramework class created: {PerformanceTestFramework.__name__}")
            methods = [m for m in dir(PerformanceTestFramework) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4g
    print("\n📝 Exercise 4g: Create Data Contract Validator")
    try:
        DataContractValidator = exercise_4g_create_data_contract_validator()

        if DataContractValidator:
            print(f"✅ DataContractValidator class created: {DataContractValidator.__name__}")
            methods = [m for m in dir(DataContractValidator) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 4h
    print("\n📝 Exercise 4h: Create Mock Data Sources")
    try:
        mock_classes = exercise_4h_create_mock_data_sources()

        if mock_classes:
            print(f"✅ {len(mock_classes)} mock classes created:")
            for name, mock_class in mock_classes.items():
                print(f"  - {name}: {mock_class.__name__}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Show sample data for testing
    print("\n📊 Sample Test Data:")
    print("=" * 25)

    customer_data = create_sample_customer_data()
    sales_data = create_sample_sales_data()

    print(f"👥 Customer records: {len(customer_data)} (including {len([r for r in customer_data if r[1] is None or '@' not in str(r[3])])} with quality issues)")
    print(f"💰 Sales records: {len(sales_data)} (including {len([r for r in sales_data if r[2].startswith('P999') or r[4] < 0])} with quality issues)")

    print("\n🔍 Data Quality Issues to Test:")
    print("  • Missing values (null names)")
    print("  • Invalid formats (malformed emails)")
    print("  • Business rule violations (negative values)")
    print("  • Referential integrity (orphaned records)")
    print("  • Duplicate records")

    print("\n🎉 Exercise 4 completed!")
    print("🧪 Excellent work on testing frameworks!")
    print("✅ You've mastered comprehensive testing for Spark applications!")
    print("🚀 Ready for Exercise 5!")


if __name__ == "__main__":
    run_exercises()