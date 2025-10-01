"""
Solution 4: Testing Framework
============================

Complete solution for Exercise 4 demonstrating how to build a comprehensive
testing infrastructure for Spark applications.

This solution shows:
- Base test classes with Spark session management
- Test data generators for realistic datasets
- Data quality validation frameworks
- Unit tests for data transformations
- Integration testing patterns
- Performance testing frameworks
- Data contract validation
- Mock data sources for external dependencies
"""

import unittest
import pytest
import time
import random
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable, Tuple, Union
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import json
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, isnan, when, avg, stddev, min as spark_min,
    max as spark_max, sum as spark_sum, desc, asc, lit,
    regexp_extract, trim, upper, lower, size, array_contains
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType, ArrayType
)


def exercise_4a_create_spark_test_base() -> type:
    """
    SOLUTION: Create a base test class for Spark applications with common setup and utilities.
    """

    class SparkTestBase(unittest.TestCase):
        """Base test class for Spark applications with comprehensive testing utilities"""

        spark: SparkSession = None
        temp_dir: Path = None

        @classmethod
        def setUpClass(cls):
            """Initialize Spark session for testing"""
            cls.spark = (SparkSession.builder
                        .appName("TestSuite")
                        .master("local[2]")
                        .config("spark.sql.shuffle.partitions", "2")
                        .config("spark.sql.adaptive.enabled", "false")  # Disable for deterministic tests
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config("spark.sql.warehouse.dir", str(Path(tempfile.gettempdir()) / "spark-warehouse"))
                        .getOrCreate())

            # Set log level to reduce noise during testing
            cls.spark.sparkContext.setLogLevel("WARN")

            # Create temporary directory for test files
            cls.temp_dir = Path(tempfile.mkdtemp())

            print(f"🧪 Spark test session initialized: {cls.spark.sparkContext.appName}")

        @classmethod
        def tearDownClass(cls):
            """Stop Spark session and cleanup"""
            if cls.spark:
                cls.spark.stop()
                print("🛑 Spark test session stopped")

            # Cleanup temporary directory
            if cls.temp_dir and cls.temp_dir.exists():
                shutil.rmtree(cls.temp_dir, ignore_errors=True)

        def create_test_dataframe(self, data: List[tuple], schema: StructType) -> DataFrame:
            """Create DataFrame from test data with specified schema"""
            if not data:
                # Create empty DataFrame with schema
                return self.spark.createDataFrame([], schema)

            return self.spark.createDataFrame(data, schema)

        def assert_dataframe_equal(self, df1: DataFrame, df2: DataFrame, check_schema: bool = True) -> None:
            """Compare two DataFrames for equality"""
            # Check row counts
            df1_count = df1.count()
            df2_count = df2.count()
            self.assertEqual(df1_count, df2_count,
                           f"DataFrames have different row counts: {df1_count} vs {df2_count}")

            # Check schemas if requested
            if check_schema:
                self.assert_schema_equal(df1, df2.schema)

            # Compare data by sorting both DataFrames
            df1_sorted = df1.orderBy(*[col(c) for c in df1.columns])
            df2_sorted = df2.orderBy(*[col(c) for c in df2.columns])

            # Collect and compare rows
            rows1 = df1_sorted.collect()
            rows2 = df2_sorted.collect()

            for i, (row1, row2) in enumerate(zip(rows1, rows2)):
                self.assertEqual(row1, row2, f"Row {i} differs: {row1} vs {row2}")

        def assert_schema_equal(self, df: DataFrame, expected_schema: StructType) -> None:
            """Validate DataFrame schema matches expected schema"""
            actual_schema = df.schema

            # Check field count
            self.assertEqual(len(actual_schema.fields), len(expected_schema.fields),
                           f"Schema field count differs: {len(actual_schema.fields)} vs {len(expected_schema.fields)}")

            # Check each field
            for actual_field, expected_field in zip(actual_schema.fields, expected_schema.fields):
                self.assertEqual(actual_field.name, expected_field.name,
                               f"Field name differs: {actual_field.name} vs {expected_field.name}")
                self.assertEqual(type(actual_field.dataType), type(expected_field.dataType),
                               f"Field type differs for {actual_field.name}: {actual_field.dataType} vs {expected_field.dataType}")

        def assert_dataframe_not_empty(self, df: DataFrame) -> None:
            """Assert DataFrame is not empty"""
            count = df.count()
            self.assertGreater(count, 0, "DataFrame should not be empty")

        def assert_dataframe_has_columns(self, df: DataFrame, expected_columns: List[str]) -> None:
            """Assert DataFrame has expected columns"""
            actual_columns = set(df.columns)
            expected_columns_set = set(expected_columns)

            missing_columns = expected_columns_set - actual_columns
            self.assertEqual(len(missing_columns), 0,
                           f"Missing columns: {missing_columns}")

        def assert_column_values_unique(self, df: DataFrame, column: str) -> None:
            """Assert all values in column are unique"""
            total_count = df.count()
            unique_count = df.select(column).distinct().count()
            self.assertEqual(total_count, unique_count,
                           f"Column '{column}' contains duplicate values")

        def assert_column_no_nulls(self, df: DataFrame, column: str) -> None:
            """Assert column contains no null values"""
            null_count = df.filter(col(column).isNull()).count()
            self.assertEqual(null_count, 0,
                           f"Column '{column}' contains {null_count} null values")

        def get_temp_path(self, filename: str = None) -> str:
            """Get temporary file path for testing"""
            if filename:
                return str(self.temp_dir / filename)
            else:
                return str(self.temp_dir / f"test_file_{uuid.uuid4().hex}")

        def write_test_csv(self, data: List[Dict[str, Any]], filename: str) -> str:
            """Write test data to CSV file and return path"""
            import csv

            file_path = self.get_temp_path(filename)

            if data:
                with open(file_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)

            return file_path

        def assert_approximately_equal(self, actual: float, expected: float, tolerance: float = 0.01) -> None:
            """Assert two float values are approximately equal"""
            self.assertAlmostEqual(actual, expected, delta=tolerance,
                                 msg=f"Values not approximately equal: {actual} vs {expected} (tolerance: {tolerance})")

    return SparkTestBase


def exercise_4b_create_test_data_generator() -> type:
    """
    SOLUTION: Create a test data generator for creating realistic test datasets.
    """

    class TestDataGenerator:
        """Generate realistic test datasets for comprehensive testing"""

        def __init__(self, seed: int = 42):
            """Initialize generator with random seed for reproducible tests"""
            random.seed(seed)
            self.fake_data = self._initialize_fake_data()

        def _initialize_fake_data(self) -> Dict[str, List[str]]:
            """Initialize pools of fake data for generation"""
            return {
                "first_names": [
                    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
                    "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter",
                    "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe"
                ],
                "last_names": [
                    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
                    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
                ],
                "cities": [
                    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
                    "Fort Worth", "Columbus", "Charlotte", "Seattle", "Denver", "Boston"
                ],
                "email_domains": [
                    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com",
                    "email.com", "mail.com", "test.com", "example.org", "domain.net"
                ],
                "product_categories": [
                    "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
                    "Toys", "Health", "Beauty", "Automotive", "Food"
                ],
                "product_names": [
                    "Laptop", "Smartphone", "Tablet", "Headphones", "Camera", "Watch",
                    "Shoes", "Shirt", "Jeans", "Jacket", "Book", "Magazine", "Chair",
                    "Table", "Lamp", "Bicycle", "Basketball", "Tennis Racket"
                ]
            }

        def generate_customers(self, count: int) -> List[tuple]:
            """Generate realistic customer data"""
            customers = []

            for i in range(count):
                customer_id = f"C{i+1:06d}"
                first_name = random.choice(self.fake_data["first_names"])
                last_name = random.choice(self.fake_data["last_names"])
                name = f"{first_name} {last_name}"

                age = random.randint(18, 80)
                email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(self.fake_data['email_domains'])}"
                income = round(random.uniform(30000, 150000), 2)
                city = random.choice(self.fake_data["cities"])

                # Generate registration date within last 2 years
                start_date = datetime.now() - timedelta(days=730)
                end_date = datetime.now()
                random_date = start_date + timedelta(
                    seconds=random.randint(0, int((end_date - start_date).total_seconds()))
                )
                registration_date = random_date.strftime("%Y-%m-%d")

                customers.append((customer_id, name, age, email, income, city, registration_date))

            return customers

        def generate_sales(self, count: int, customer_ids: List[str]) -> List[tuple]:
            """Generate realistic sales transaction data"""
            sales = []

            for i in range(count):
                transaction_id = f"T{i+1:08d}"
                customer_id = random.choice(customer_ids)
                product_id = f"P{random.randint(1, 1000):03d}"
                quantity = random.randint(1, 5)
                unit_price = round(random.uniform(9.99, 999.99), 2)

                # Generate transaction date within last 6 months
                start_date = datetime.now() - timedelta(days=180)
                end_date = datetime.now()
                random_date = start_date + timedelta(
                    seconds=random.randint(0, int((end_date - start_date).total_seconds()))
                )
                transaction_date = random_date.strftime("%Y-%m-%d")

                sales.append((transaction_id, customer_id, product_id, quantity, unit_price, transaction_date))

            return sales

        def generate_products(self, count: int) -> List[tuple]:
            """Generate realistic product catalog data"""
            products = []

            for i in range(count):
                product_id = f"P{i+1:03d}"
                name = random.choice(self.fake_data["product_names"])
                category = random.choice(self.fake_data["product_categories"])
                price = round(random.uniform(9.99, 999.99), 2)
                stock_quantity = random.randint(0, 1000)

                # Generate product description
                description = f"High-quality {name.lower()} in {category.lower()} category"

                # Random supplier
                supplier = f"Supplier {random.randint(1, 50):02d}"

                products.append((product_id, name, category, price, stock_quantity, description, supplier))

            return products

        def generate_time_series(self, days: int, frequency: str = "daily") -> List[tuple]:
            """Generate time series data for testing temporal operations"""
            time_series = []

            start_date = datetime.now() - timedelta(days=days)

            if frequency == "daily":
                delta = timedelta(days=1)
            elif frequency == "hourly":
                delta = timedelta(hours=1)
                days = days * 24  # Convert to hours
            elif frequency == "weekly":
                delta = timedelta(weeks=1)
                days = days // 7  # Convert to weeks
            else:
                raise ValueError(f"Unsupported frequency: {frequency}")

            for i in range(days):
                timestamp = start_date + (delta * i)
                value = 100 + random.uniform(-20, 50) + (i * 0.1)  # Trending upward with noise
                category = random.choice(["A", "B", "C"])

                time_series.append((
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    round(value, 2),
                    category,
                    f"metric_{i%10}"
                ))

            return time_series

        def add_data_quality_issues(self, data: List[tuple], corruption_rate: float = 0.1) -> List[tuple]:
            """Inject data quality issues for testing validation logic"""
            corrupted_data = []

            for row in data:
                if random.random() < corruption_rate:
                    # Apply random corruption
                    corruption_type = random.choice([
                        "null_value", "invalid_email", "negative_numeric",
                        "empty_string", "invalid_date", "duplicate"
                    ])

                    row_list = list(row)

                    if corruption_type == "null_value":
                        # Set random field to None
                        field_idx = random.randint(0, len(row_list) - 1)
                        row_list[field_idx] = None

                    elif corruption_type == "invalid_email" and len(row_list) > 3:
                        # Corrupt email field (assuming it's around index 3)
                        if "@" in str(row_list[3]):
                            row_list[3] = str(row_list[3]).replace("@", "INVALID")

                    elif corruption_type == "negative_numeric":
                        # Make numeric values negative
                        for i, value in enumerate(row_list):
                            if isinstance(value, (int, float)) and value > 0:
                                row_list[i] = -abs(value)
                                break

                    elif corruption_type == "empty_string":
                        # Set string field to empty
                        for i, value in enumerate(row_list):
                            if isinstance(value, str) and value:
                                row_list[i] = ""
                                break

                    elif corruption_type == "invalid_date":
                        # Corrupt date field
                        for i, value in enumerate(row_list):
                            if isinstance(value, str) and "-" in value and len(value) == 10:
                                row_list[i] = "1900-13-45"  # Invalid date
                                break

                    corrupted_data.append(tuple(row_list))
                else:
                    corrupted_data.append(row)

            return corrupted_data

        def generate_nested_data(self, count: int) -> List[tuple]:
            """Generate data with nested structures (arrays, maps)"""
            nested_data = []

            for i in range(count):
                record_id = f"R{i+1:06d}"

                # Array field
                tags = random.sample(["tag1", "tag2", "tag3", "tag4", "tag5"],
                                   random.randint(1, 3))

                # Nested structure as JSON string
                metadata = {
                    "created_by": random.choice(self.fake_data["first_names"]),
                    "version": random.randint(1, 10),
                    "active": random.choice([True, False])
                }

                nested_data.append((record_id, tags, json.dumps(metadata)))

            return nested_data

    return TestDataGenerator


def exercise_4c_create_data_quality_validator() -> type:
    """
    SOLUTION: Create a data quality validation framework.
    """

    class DataQualityValidator:
        """Comprehensive data quality validation framework"""

        def __init__(self, spark: SparkSession):
            self.spark = spark

        def check_completeness(self, df: DataFrame) -> Dict[str, float]:
            """Check data completeness (percentage of non-null values per column)"""
            total_rows = df.count()
            if total_rows == 0:
                return {col: 0.0 for col in df.columns}

            completeness = {}

            for column in df.columns:
                non_null_count = df.filter(col(column).isNotNull()).count()
                completeness_rate = (non_null_count / total_rows) * 100
                completeness[column] = round(completeness_rate, 2)

            return completeness

        def check_uniqueness(self, df: DataFrame, columns: List[str]) -> Dict[str, int]:
            """Check uniqueness constraints and return duplicate counts"""
            uniqueness = {}

            for column in columns:
                if column not in df.columns:
                    uniqueness[column] = -1  # Column doesn't exist
                    continue

                total_count = df.count()
                unique_count = df.select(column).distinct().count()
                duplicate_count = total_count - unique_count
                uniqueness[column] = duplicate_count

            return uniqueness

        def check_data_types(self, df: DataFrame, expected_schema: StructType) -> Dict[str, bool]:
            """Validate actual data types match expected schema"""
            type_validation = {}
            actual_schema = df.schema

            # Create lookup for expected types
            expected_types = {field.name: field.dataType for field in expected_schema.fields}

            for field in actual_schema.fields:
                column_name = field.name
                actual_type = field.dataType
                expected_type = expected_types.get(column_name)

                if expected_type is None:
                    type_validation[column_name] = False  # Unexpected column
                else:
                    # Check if types match (exact or compatible)
                    type_validation[column_name] = type(actual_type) == type(expected_type)

            # Check for missing columns
            for expected_field in expected_schema.fields:
                if expected_field.name not in [f.name for f in actual_schema.fields]:
                    type_validation[expected_field.name] = False

            return type_validation

        def check_value_ranges(self, df: DataFrame, range_rules: Dict[str, Dict[str, Any]]) -> Dict[str, bool]:
            """Validate values are within expected ranges"""
            range_validation = {}

            for column, rules in range_rules.items():
                if column not in df.columns:
                    range_validation[column] = False
                    continue

                violations = 0

                # Check minimum value
                if "min" in rules:
                    min_violations = df.filter(col(column) < rules["min"]).count()
                    violations += min_violations

                # Check maximum value
                if "max" in rules:
                    max_violations = df.filter(col(column) > rules["max"]).count()
                    violations += max_violations

                # Check allowed values
                if "allowed_values" in rules:
                    allowed_values = rules["allowed_values"]
                    invalid_values = df.filter(~col(column).isin(allowed_values)).count()
                    violations += invalid_values

                # Check regex pattern
                if "pattern" in rules:
                    pattern = rules["pattern"]
                    pattern_violations = df.filter(
                        ~col(column).rlike(pattern)
                    ).count()
                    violations += pattern_violations

                range_validation[column] = violations == 0

            return range_validation

        def check_referential_integrity(self, df1: DataFrame, df2: DataFrame,
                                      join_keys: List[str]) -> bool:
            """Check referential integrity between two DataFrames"""

            # Check if all join keys exist in both DataFrames
            for key in join_keys:
                if key not in df1.columns or key not in df2.columns:
                    return False

            # Find orphaned records (records in df1 without matching records in df2)
            left_anti_join = df1.join(df2, join_keys, "left_anti")
            orphaned_count = left_anti_join.count()

            return orphaned_count == 0

        def check_business_rules(self, df: DataFrame, rules: List[Callable]) -> Dict[str, bool]:
            """Apply custom business rule validations"""
            rule_results = {}

            for i, rule_func in enumerate(rules):
                try:
                    rule_name = getattr(rule_func, "__name__", f"rule_{i}")
                    result = rule_func(df)
                    rule_results[rule_name] = bool(result)
                except Exception as e:
                    rule_results[f"rule_{i}_error"] = False

            return rule_results

        def generate_quality_report(self, df: DataFrame,
                                  expected_schema: Optional[StructType] = None,
                                  range_rules: Optional[Dict[str, Dict[str, Any]]] = None,
                                  uniqueness_columns: Optional[List[str]] = None) -> Dict[str, Any]:
            """Generate comprehensive data quality report"""

            report = {
                "timestamp": datetime.now().isoformat(),
                "dataset_info": {
                    "row_count": df.count(),
                    "column_count": len(df.columns),
                    "columns": df.columns
                }
            }

            # Completeness check
            report["completeness"] = self.check_completeness(df)

            # Type validation if schema provided
            if expected_schema:
                report["type_validation"] = self.check_data_types(df, expected_schema)

            # Range validation if rules provided
            if range_rules:
                report["range_validation"] = self.check_value_ranges(df, range_rules)

            # Uniqueness check if columns specified
            if uniqueness_columns:
                report["uniqueness"] = self.check_uniqueness(df, uniqueness_columns)

            # Statistical summary for numeric columns
            numeric_columns = [f.name for f in df.schema.fields
                             if isinstance(f.dataType, (IntegerType, DoubleType))]

            if numeric_columns:
                stats_df = df.select(numeric_columns).describe()
                report["statistics"] = {}
                for col_name in numeric_columns:
                    col_stats = {}
                    for row in stats_df.collect():
                        stat_name = row["summary"]
                        stat_value = row[col_name]
                        if stat_value is not None:
                            try:
                                col_stats[stat_name] = float(stat_value)
                            except ValueError:
                                col_stats[stat_name] = stat_value
                    report["statistics"][col_name] = col_stats

            # Quality score calculation
            total_checks = 0
            passed_checks = 0

            for check_category in ["completeness", "type_validation", "range_validation", "uniqueness"]:
                if check_category in report:
                    for result in report[check_category].values():
                        total_checks += 1
                        if isinstance(result, bool) and result:
                            passed_checks += 1
                        elif isinstance(result, (int, float)) and result >= 95:  # 95% threshold for completeness
                            passed_checks += 1

            report["quality_score"] = round((passed_checks / total_checks * 100) if total_checks > 0 else 0, 2)

            return report

        def validate_schema_evolution(self, old_schema: StructType, new_schema: StructType) -> Dict[str, Any]:
            """Validate schema evolution compatibility"""
            evolution_report = {
                "compatible": True,
                "added_fields": [],
                "removed_fields": [],
                "modified_fields": [],
                "issues": []
            }

            old_fields = {f.name: f for f in old_schema.fields}
            new_fields = {f.name: f for f in new_schema.fields}

            # Check for added fields
            added = set(new_fields.keys()) - set(old_fields.keys())
            evolution_report["added_fields"] = list(added)

            # Check for removed fields
            removed = set(old_fields.keys()) - set(new_fields.keys())
            evolution_report["removed_fields"] = list(removed)

            # Check for modified fields
            for field_name in set(old_fields.keys()) & set(new_fields.keys()):
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]

                if type(old_field.dataType) != type(new_field.dataType):
                    evolution_report["modified_fields"].append({
                        "field": field_name,
                        "old_type": str(old_field.dataType),
                        "new_type": str(new_field.dataType)
                    })

            # Determine compatibility issues
            if removed:
                evolution_report["compatible"] = False
                evolution_report["issues"].append("Removed fields may break existing queries")

            for mod_field in evolution_report["modified_fields"]:
                # Type changes are generally incompatible
                evolution_report["compatible"] = False
                evolution_report["issues"].append(
                    f"Type change in field '{mod_field['field']}' may cause data loss"
                )

            return evolution_report

    return DataQualityValidator


def exercise_4d_create_transformation_tests() -> Dict[str, type]:
    """
    SOLUTION: Create test classes for data transformation functions.
    """

    # First, let's create some sample transformation functions to test
    def clean_customer_data(df: DataFrame) -> DataFrame:
        """Sample data cleaning transformation"""
        return (df
                .filter(col("age") >= 18)
                .filter(col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$'))
                .filter(col("name").isNotNull())
                .dropDuplicates(["customer_id"]))

    def enrich_with_age_groups(df: DataFrame) -> DataFrame:
        """Sample data enrichment transformation"""
        return (df
                .withColumn("age_group",
                           when(col("age") < 30, "Young")
                           .when(col("age") <= 50, "Middle")
                           .otherwise("Senior")))

    def calculate_customer_metrics(df: DataFrame) -> DataFrame:
        """Sample aggregation transformation"""
        return (df
                .groupBy("city")
                .agg(
                    count("customer_id").alias("customer_count"),
                    avg("age").alias("avg_age"),
                    avg("income").alias("avg_income")
                ))

    def apply_business_rules(df: DataFrame) -> DataFrame:
        """Sample business logic transformation"""
        return (df
                .withColumn("is_premium", col("income") > 75000)
                .withColumn("risk_score",
                           when(col("age") < 25, 3)
                           .when(col("income") < 40000, 2)
                           .otherwise(1)))

    # Create base test class
    SparkTestBase = exercise_4a_create_spark_test_base()

    class TestDataCleaning(SparkTestBase):
        """Test cases for data cleaning transformations"""

        def setUp(self):
            """Set up test data for each test"""
            self.test_schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("income", DoubleType(), True)
            ])

        def test_clean_customer_data_removes_underage(self):
            """Test that cleaning removes customers under 18"""
            test_data = [
                ("C001", "Alice", 25, "alice@email.com", 50000.0),
                ("C002", "Bob", 16, "bob@email.com", 30000.0),  # Underage
                ("C003", "Charlie", 30, "charlie@email.com", 60000.0)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            cleaned_df = clean_customer_data(df)

            # Should remove the underage customer
            self.assertEqual(cleaned_df.count(), 2)
            ages = [row.age for row in cleaned_df.collect()]
            self.assertTrue(all(age >= 18 for age in ages))

        def test_clean_customer_data_removes_invalid_emails(self):
            """Test that cleaning removes invalid email addresses"""
            test_data = [
                ("C001", "Alice", 25, "alice@email.com", 50000.0),
                ("C002", "Bob", 25, "invalid-email", 30000.0),  # Invalid email
                ("C003", "Charlie", 30, "charlie@email.com", 60000.0)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            cleaned_df = clean_customer_data(df)

            # Should remove the invalid email
            self.assertEqual(cleaned_df.count(), 2)
            emails = [row.email for row in cleaned_df.collect()]
            self.assertTrue(all("@" in email and "." in email for email in emails))

        def test_clean_customer_data_removes_null_names(self):
            """Test that cleaning removes null names"""
            test_data = [
                ("C001", "Alice", 25, "alice@email.com", 50000.0),
                ("C002", None, 25, "bob@email.com", 30000.0),  # Null name
                ("C003", "Charlie", 30, "charlie@email.com", 60000.0)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            cleaned_df = clean_customer_data(df)

            # Should remove the null name
            self.assertEqual(cleaned_df.count(), 2)
            names = [row.name for row in cleaned_df.collect()]
            self.assertTrue(all(name is not None for name in names))

        def test_clean_customer_data_removes_duplicates(self):
            """Test that cleaning removes duplicate customer IDs"""
            test_data = [
                ("C001", "Alice", 25, "alice@email.com", 50000.0),
                ("C001", "Alice Duplicate", 25, "alice2@email.com", 50000.0),  # Duplicate ID
                ("C002", "Bob", 25, "bob@email.com", 30000.0)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            cleaned_df = clean_customer_data(df)

            # Should remove one of the duplicates
            self.assertEqual(cleaned_df.count(), 2)
            customer_ids = [row.customer_id for row in cleaned_df.collect()]
            self.assertEqual(len(set(customer_ids)), len(customer_ids))

    class TestDataEnrichment(SparkTestBase):
        """Test cases for data enrichment transformations"""

        def setUp(self):
            """Set up test data for each test"""
            self.test_schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True)
            ])

        def test_enrich_with_age_groups_young(self):
            """Test age group enrichment for young customers"""
            test_data = [
                ("C001", "Alice", 25),
                ("C002", "Bob", 28)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            enriched_df = enrich_with_age_groups(df)

            self.assert_dataframe_has_columns(enriched_df, ["age_group"])
            age_groups = [row.age_group for row in enriched_df.collect()]
            self.assertTrue(all(group == "Young" for group in age_groups))

        def test_enrich_with_age_groups_middle(self):
            """Test age group enrichment for middle-aged customers"""
            test_data = [
                ("C001", "Alice", 35),
                ("C002", "Bob", 45)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            enriched_df = enrich_with_age_groups(df)

            age_groups = [row.age_group for row in enriched_df.collect()]
            self.assertTrue(all(group == "Middle" for group in age_groups))

        def test_enrich_with_age_groups_senior(self):
            """Test age group enrichment for senior customers"""
            test_data = [
                ("C001", "Alice", 55),
                ("C002", "Bob", 65)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            enriched_df = enrich_with_age_groups(df)

            age_groups = [row.age_group for row in enriched_df.collect()]
            self.assertTrue(all(group == "Senior" for group in age_groups))

    class TestAggregations(SparkTestBase):
        """Test cases for aggregation transformations"""

        def setUp(self):
            """Set up test data for each test"""
            self.test_schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("city", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("income", DoubleType(), True)
            ])

        def test_calculate_customer_metrics_aggregation(self):
            """Test customer metrics calculation"""
            test_data = [
                ("C001", "New York", 25, 50000.0),
                ("C002", "New York", 35, 70000.0),
                ("C003", "Chicago", 30, 60000.0),
                ("C004", "Chicago", 40, 80000.0)
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            metrics_df = calculate_customer_metrics(df)

            # Should have 2 cities
            self.assertEqual(metrics_df.count(), 2)

            # Check columns exist
            self.assert_dataframe_has_columns(metrics_df,
                                            ["city", "customer_count", "avg_age", "avg_income"])

            # Verify aggregation for New York
            ny_row = metrics_df.filter(col("city") == "New York").collect()[0]
            self.assertEqual(ny_row.customer_count, 2)
            self.assertEqual(ny_row.avg_age, 30.0)  # (25 + 35) / 2
            self.assertEqual(ny_row.avg_income, 60000.0)  # (50000 + 70000) / 2

        def test_calculate_customer_metrics_empty_data(self):
            """Test metrics calculation with empty data"""
            df = self.create_test_dataframe([], self.test_schema)
            metrics_df = calculate_customer_metrics(df)

            self.assertEqual(metrics_df.count(), 0)

    class TestBusinessLogic(SparkTestBase):
        """Test cases for business logic implementations"""

        def setUp(self):
            """Set up test data for each test"""
            self.test_schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("income", DoubleType(), True)
            ])

        def test_apply_business_rules_premium_classification(self):
            """Test premium customer classification"""
            test_data = [
                ("C001", 30, 80000.0),  # Premium
                ("C002", 25, 50000.0),  # Not premium
                ("C003", 35, 100000.0)  # Premium
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            business_df = apply_business_rules(df)

            # Check premium classification
            premium_customers = business_df.filter(col("is_premium") == True).collect()
            self.assertEqual(len(premium_customers), 2)

            premium_incomes = [row.income for row in premium_customers]
            self.assertTrue(all(income > 75000 for income in premium_incomes))

        def test_apply_business_rules_risk_scoring(self):
            """Test risk scoring logic"""
            test_data = [
                ("C001", 22, 80000.0),  # Young -> Risk 3
                ("C002", 30, 35000.0),  # Low income -> Risk 2
                ("C003", 35, 100000.0)  # Normal -> Risk 1
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            business_df = apply_business_rules(df)

            # Verify risk scores
            rows = business_df.orderBy("customer_id").collect()
            self.assertEqual(rows[0].risk_score, 3)  # Young customer
            self.assertEqual(rows[1].risk_score, 2)  # Low income
            self.assertEqual(rows[2].risk_score, 1)  # Normal

        def test_apply_business_rules_edge_cases(self):
            """Test business rules with edge cases"""
            test_data = [
                ("C001", 25, 75000.0),  # Exactly at premium threshold
                ("C002", 30, 40000.0),  # Exactly at income threshold
            ]

            df = self.create_test_dataframe(test_data, self.test_schema)
            business_df = apply_business_rules(df)

            rows = business_df.orderBy("customer_id").collect()

            # 75000 is not > 75000, so not premium
            self.assertFalse(rows[0].is_premium)

            # 40000 is not < 40000, so risk score should be 1
            self.assertEqual(rows[1].risk_score, 1)

    return {
        "TestDataCleaning": TestDataCleaning,
        "TestDataEnrichment": TestDataEnrichment,
        "TestAggregations": TestAggregations,
        "TestBusinessLogic": TestBusinessLogic
    }


def exercise_4e_create_integration_test_framework() -> type:
    """
    SOLUTION: Create an integration testing framework for end-to-end pipeline testing.
    """

    class IntegrationTestFramework:
        """Framework for end-to-end pipeline integration testing"""

        def __init__(self, spark: SparkSession):
            self.spark = spark
            self.test_data_generator = exercise_4b_create_test_data_generator()()
            self.temp_dir = Path(tempfile.mkdtemp())
            self.test_datasets = {}

        def setup_test_environment(self) -> None:
            """Set up test environment with necessary directories and configurations"""
            print("🔧 Setting up integration test environment...")

            # Create test directories
            directories = ["input", "output", "temp", "checkpoints"]
            for directory in directories:
                (self.temp_dir / directory).mkdir(exist_ok=True)

            # Set Spark configurations for testing
            self.spark.conf.set("spark.sql.adaptive.enabled", "false")
            self.spark.conf.set("spark.sql.shuffle.partitions", "2")

            print(f"✅ Test environment ready at: {self.temp_dir}")

        def create_test_datasets(self) -> Dict[str, DataFrame]:
            """Create comprehensive test datasets for pipeline testing"""
            print("📊 Creating test datasets...")

            # Generate customers
            customer_data = self.test_data_generator.generate_customers(1000)
            customer_schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("income", DoubleType(), True),
                StructField("city", StringType(), True),
                StructField("registration_date", StringType(), True)
            ])
            customers_df = self.spark.createDataFrame(customer_data, customer_schema)

            # Generate products
            product_data = self.test_data_generator.generate_products(100)
            product_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("stock_quantity", IntegerType(), True),
                StructField("description", StringType(), True),
                StructField("supplier", StringType(), True)
            ])
            products_df = self.spark.createDataFrame(product_data, product_schema)

            # Generate sales
            customer_ids = [row.customer_id for row in customers_df.select("customer_id").collect()]
            sales_data = self.test_data_generator.generate_sales(5000, customer_ids)
            sales_schema = StructType([
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("transaction_date", StringType(), True)
            ])
            sales_df = self.spark.createDataFrame(sales_data, sales_schema)

            # Add data quality issues to test data validation
            corrupted_customer_data = self.test_data_generator.add_data_quality_issues(
                customer_data, corruption_rate=0.05
            )
            corrupted_customers_df = self.spark.createDataFrame(corrupted_customer_data, customer_schema)

            self.test_datasets = {
                "customers": customers_df,
                "products": products_df,
                "sales": sales_df,
                "corrupted_customers": corrupted_customers_df
            }

            # Save datasets to files for file-based testing
            for name, df in self.test_datasets.items():
                output_path = str(self.temp_dir / "input" / name)
                df.write.mode("overwrite").parquet(output_path)

            print(f"✅ Created {len(self.test_datasets)} test datasets")
            return self.test_datasets

        def run_pipeline_test(self, pipeline_func: Callable,
                            input_data: Dict[str, DataFrame],
                            test_name: str = "pipeline_test") -> Dict[str, Any]:
            """Run pipeline with test data and collect metrics"""
            print(f"🚀 Running pipeline test: {test_name}")

            start_time = time.time()

            try:
                # Execute pipeline
                result = pipeline_func(input_data)

                end_time = time.time()
                execution_time = end_time - start_time

                # Collect metrics
                metrics = {
                    "test_name": test_name,
                    "status": "success",
                    "execution_time": execution_time,
                    "input_datasets": list(input_data.keys()),
                    "input_row_counts": {name: df.count() for name, df in input_data.items()},
                    "output_metrics": self._analyze_pipeline_output(result),
                    "memory_usage": self._get_memory_usage(),
                    "timestamp": datetime.now().isoformat()
                }

                print(f"✅ Pipeline test completed in {execution_time:.2f}s")
                return metrics

            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time

                metrics = {
                    "test_name": test_name,
                    "status": "failed",
                    "execution_time": execution_time,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }

                print(f"❌ Pipeline test failed: {e}")
                return metrics

        def _analyze_pipeline_output(self, result: Any) -> Dict[str, Any]:
            """Analyze pipeline output and extract metrics"""
            if isinstance(result, DataFrame):
                return {
                    "type": "DataFrame",
                    "row_count": result.count(),
                    "column_count": len(result.columns),
                    "columns": result.columns,
                    "partitions": result.rdd.getNumPartitions()
                }
            elif isinstance(result, dict):
                output_metrics = {}
                for key, value in result.items():
                    if isinstance(value, DataFrame):
                        output_metrics[key] = {
                            "row_count": value.count(),
                            "column_count": len(value.columns)
                        }
                    else:
                        output_metrics[key] = {"type": type(value).__name__}
                return output_metrics
            else:
                return {"type": type(result).__name__}

        def _get_memory_usage(self) -> Dict[str, Any]:
            """Get current Spark memory usage"""
            # This is a simplified version - in practice, you'd use Spark metrics
            return {
                "driver_memory_used": "N/A",  # Would get from Spark metrics
                "executor_memory_used": "N/A",
                "storage_memory_used": "N/A"
            }

        def validate_pipeline_output(self, output_df: DataFrame,
                                   validation_rules: Dict[str, Any]) -> bool:
            """Validate pipeline output against specified rules"""
            print("🔍 Validating pipeline output...")

            validator = exercise_4c_create_data_quality_validator()(self.spark)

            # Run validation checks
            validation_results = []

            # Row count validation
            if "min_rows" in validation_rules:
                min_rows = validation_rules["min_rows"]
                actual_rows = output_df.count()
                row_count_valid = actual_rows >= min_rows
                validation_results.append(row_count_valid)
                print(f"  Row count: {actual_rows} >= {min_rows} ✅" if row_count_valid
                      else f"  Row count: {actual_rows} < {min_rows} ❌")

            # Schema validation
            if "expected_columns" in validation_rules:
                expected_columns = validation_rules["expected_columns"]
                actual_columns = set(output_df.columns)
                missing_columns = set(expected_columns) - actual_columns
                schema_valid = len(missing_columns) == 0
                validation_results.append(schema_valid)
                print(f"  Schema: All columns present ✅" if schema_valid
                      else f"  Schema: Missing columns {missing_columns} ❌")

            # Data quality validation
            if "quality_rules" in validation_rules:
                quality_rules = validation_rules["quality_rules"]
                quality_report = validator.generate_quality_report(
                    output_df,
                    range_rules=quality_rules.get("range_rules"),
                    uniqueness_columns=quality_rules.get("uniqueness_columns")
                )
                quality_score = quality_report["quality_score"]
                min_quality_score = quality_rules.get("min_quality_score", 90)
                quality_valid = quality_score >= min_quality_score
                validation_results.append(quality_valid)
                print(f"  Data Quality: {quality_score}% >= {min_quality_score}% ✅" if quality_valid
                      else f"  Data Quality: {quality_score}% < {min_quality_score}% ❌")

            # Business rule validation
            if "business_rules" in validation_rules:
                business_rules = validation_rules["business_rules"]
                business_results = validator.check_business_rules(output_df, business_rules)
                business_valid = all(business_results.values())
                validation_results.append(business_valid)
                print(f"  Business Rules: All passed ✅" if business_valid
                      else f"  Business Rules: Some failed ❌")

            overall_valid = all(validation_results)
            print(f"🎯 Overall validation: {'PASSED' if overall_valid else 'FAILED'}")

            return overall_valid

        def benchmark_performance(self, pipeline_func: Callable,
                                input_data: Dict[str, DataFrame],
                                iterations: int = 3) -> Dict[str, float]:
            """Benchmark pipeline performance across multiple runs"""
            print(f"⏱️  Benchmarking pipeline performance ({iterations} iterations)...")

            execution_times = []

            for i in range(iterations):
                print(f"  Run {i+1}/{iterations}...")
                start_time = time.time()

                try:
                    result = pipeline_func(input_data)

                    # Force action to ensure computation
                    if isinstance(result, DataFrame):
                        result.count()
                    elif isinstance(result, dict):
                        for value in result.values():
                            if isinstance(value, DataFrame):
                                value.count()

                    end_time = time.time()
                    execution_time = end_time - start_time
                    execution_times.append(execution_time)

                except Exception as e:
                    print(f"    ❌ Run {i+1} failed: {e}")
                    continue

            if execution_times:
                avg_time = sum(execution_times) / len(execution_times)
                min_time = min(execution_times)
                max_time = max(execution_times)

                benchmark_results = {
                    "avg_execution_time": avg_time,
                    "min_execution_time": min_time,
                    "max_execution_time": max_time,
                    "successful_runs": len(execution_times),
                    "total_runs": iterations
                }

                print(f"📊 Benchmark Results:")
                print(f"  Average: {avg_time:.2f}s")
                print(f"  Min: {min_time:.2f}s")
                print(f"  Max: {max_time:.2f}s")

                return benchmark_results
            else:
                return {"error": "All benchmark runs failed"}

        def cleanup_test_environment(self) -> None:
            """Clean up test environment and temporary files"""
            print("🧹 Cleaning up test environment...")

            # Clear cached DataFrames
            self.spark.catalog.clearCache()

            # Remove temporary directory
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir, ignore_errors=True)

            print("✅ Test environment cleaned up")

    return IntegrationTestFramework


def exercise_4f_create_performance_test_framework() -> type:
    """
    SOLUTION: Create a performance testing framework for Spark applications.
    """

    class PerformanceTestFramework:
        """Framework for performance testing and benchmarking Spark operations"""

        def __init__(self, spark: SparkSession):
            self.spark = spark
            self.test_data_generator = exercise_4b_create_test_data_generator()()
            self.benchmark_results = []

        def benchmark_operation(self, operation: Callable, data_size: int,
                              operation_name: str = "operation") -> Dict[str, float]:
            """Benchmark a Spark operation with specified data size"""
            print(f"⏱️  Benchmarking {operation_name} with {data_size:,} records...")

            # Generate test data
            test_data = self._generate_benchmark_data(data_size)

            # Warm up (run once without measuring)
            try:
                _ = operation(test_data)
            except Exception:
                pass  # Ignore warmup failures

            # Clear cache to ensure fresh execution
            self.spark.catalog.clearCache()

            # Measure execution time
            start_time = time.time()

            try:
                result = operation(test_data)

                # Force action to ensure computation
                if hasattr(result, 'count'):
                    result.count()
                elif hasattr(result, 'collect'):
                    result.collect()

                end_time = time.time()
                execution_time = end_time - start_time

                # Calculate throughput
                throughput = data_size / execution_time if execution_time > 0 else 0

                benchmark_result = {
                    "operation": operation_name,
                    "data_size": data_size,
                    "execution_time": execution_time,
                    "throughput_rows_per_sec": throughput,
                    "status": "success",
                    "timestamp": datetime.now().isoformat()
                }

                print(f"  ✅ Completed in {execution_time:.2f}s ({throughput:,.0f} rows/sec)")

            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time

                benchmark_result = {
                    "operation": operation_name,
                    "data_size": data_size,
                    "execution_time": execution_time,
                    "error": str(e),
                    "status": "failed",
                    "timestamp": datetime.now().isoformat()
                }

                print(f"  ❌ Failed after {execution_time:.2f}s: {e}")

            self.benchmark_results.append(benchmark_result)
            return benchmark_result

        def test_scalability(self, operation: Callable, sizes: List[int],
                           operation_name: str = "operation") -> Dict[int, float]:
            """Test operation scalability across different data sizes"""
            print(f"📈 Testing scalability of {operation_name}...")

            scalability_results = {}

            for size in sizes:
                benchmark_result = self.benchmark_operation(operation, size, operation_name)
                if benchmark_result["status"] == "success":
                    scalability_results[size] = benchmark_result["execution_time"]
                else:
                    scalability_results[size] = None

            # Analyze scalability
            print(f"📊 Scalability Analysis for {operation_name}:")
            for size, exec_time in scalability_results.items():
                if exec_time is not None:
                    print(f"  {size:,} rows: {exec_time:.2f}s")
                else:
                    print(f"  {size:,} rows: FAILED")

            return scalability_results

        def profile_memory_usage(self, operation: Callable,
                                data_size: int = 10000) -> Dict[str, Any]:
            """Profile memory usage of an operation"""
            print(f"💾 Profiling memory usage for operation with {data_size:,} records...")

            # Get initial memory state
            initial_memory = self._get_memory_metrics()

            # Generate test data
            test_data = self._generate_benchmark_data(data_size)

            # Cache the test data
            test_data.cache()
            test_data.count()  # Force caching

            cached_memory = self._get_memory_metrics()

            # Execute operation
            try:
                result = operation(test_data)

                # Force computation and cache result
                if hasattr(result, 'cache'):
                    result.cache()
                    result.count()

                final_memory = self._get_memory_metrics()

                memory_profile = {
                    "initial_memory": initial_memory,
                    "cached_input_memory": cached_memory,
                    "final_memory": final_memory,
                    "input_memory_usage": cached_memory["storage_used"] - initial_memory["storage_used"],
                    "operation_memory_usage": final_memory["storage_used"] - cached_memory["storage_used"],
                    "total_memory_usage": final_memory["storage_used"] - initial_memory["storage_used"],
                    "data_size": data_size,
                    "status": "success"
                }

                print(f"  Input data memory: {memory_profile['input_memory_usage']:.2f} MB")
                print(f"  Operation memory: {memory_profile['operation_memory_usage']:.2f} MB")
                print(f"  Total memory: {memory_profile['total_memory_usage']:.2f} MB")

            except Exception as e:
                memory_profile = {
                    "error": str(e),
                    "status": "failed",
                    "data_size": data_size
                }
                print(f"  ❌ Memory profiling failed: {e}")

            # Cleanup
            self.spark.catalog.clearCache()

            return memory_profile

        def test_partition_optimization(self, df: DataFrame) -> Dict[str, Any]:
            """Test different partitioning strategies and their performance"""
            print("🔧 Testing partition optimization strategies...")

            partition_tests = {}

            # Test current partitioning
            current_partitions = df.rdd.getNumPartitions()
            partition_tests["current"] = self._test_partition_strategy(
                df, current_partitions, "current"
            )

            # Test optimal partitioning (based on data size)
            row_count = df.count()
            optimal_partitions = max(1, min(row_count // 10000, 200))  # 10k rows per partition, max 200
            partition_tests["optimal"] = self._test_partition_strategy(
                df.repartition(optimal_partitions), optimal_partitions, "optimal"
            )

            # Test coalesce (reduce partitions)
            if current_partitions > 2:
                coalesced_partitions = max(1, current_partitions // 2)
                partition_tests["coalesced"] = self._test_partition_strategy(
                    df.coalesce(coalesced_partitions), coalesced_partitions, "coalesced"
                )

            # Test hash partitioning (if DataFrame has suitable columns)
            if "customer_id" in df.columns:
                hash_partitions = max(1, min(current_partitions, 10))
                partition_tests["hash_partitioned"] = self._test_partition_strategy(
                    df.repartition(hash_partitions, "customer_id"), hash_partitions, "hash_partitioned"
                )

            # Find best strategy
            successful_tests = {k: v for k, v in partition_tests.items()
                              if v["status"] == "success"}

            if successful_tests:
                best_strategy = min(successful_tests.items(),
                                  key=lambda x: x[1]["execution_time"])

                print(f"🏆 Best partitioning strategy: {best_strategy[0]} " +
                      f"({best_strategy[1]['partitions']} partitions, " +
                      f"{best_strategy[1]['execution_time']:.2f}s)")

            return partition_tests

        def _test_partition_strategy(self, df: DataFrame, partition_count: int,
                                   strategy_name: str) -> Dict[str, Any]:
            """Test a specific partitioning strategy"""
            print(f"  Testing {strategy_name} strategy ({partition_count} partitions)...")

            start_time = time.time()

            try:
                # Perform a typical operation (group by and aggregation)
                result = df.groupBy(df.columns[0]).count()
                result.count()  # Force execution

                end_time = time.time()
                execution_time = end_time - start_time

                return {
                    "strategy": strategy_name,
                    "partitions": partition_count,
                    "execution_time": execution_time,
                    "status": "success"
                }

            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time

                return {
                    "strategy": strategy_name,
                    "partitions": partition_count,
                    "execution_time": execution_time,
                    "error": str(e),
                    "status": "failed"
                }

        def generate_performance_report(self) -> str:
            """Generate comprehensive performance report"""
            if not self.benchmark_results:
                return "No benchmark results available."

            report_lines = []
            report_lines.append("📊 PERFORMANCE TEST REPORT")
            report_lines.append("=" * 50)
            report_lines.append(f"Generated: {datetime.now().isoformat()}")
            report_lines.append(f"Total tests: {len(self.benchmark_results)}")
            report_lines.append("")

            # Summary statistics
            successful_tests = [r for r in self.benchmark_results if r["status"] == "success"]
            failed_tests = [r for r in self.benchmark_results if r["status"] == "failed"]

            report_lines.append(f"✅ Successful tests: {len(successful_tests)}")
            report_lines.append(f"❌ Failed tests: {len(failed_tests)}")
            report_lines.append("")

            # Detailed results
            report_lines.append("📈 DETAILED RESULTS")
            report_lines.append("-" * 30)

            for result in self.benchmark_results:
                if result["status"] == "success":
                    throughput = result.get("throughput_rows_per_sec", 0)
                    report_lines.append(
                        f"{result['operation']}: {result['data_size']:,} rows in " +
                        f"{result['execution_time']:.2f}s ({throughput:,.0f} rows/sec)"
                    )
                else:
                    report_lines.append(
                        f"{result['operation']}: FAILED - {result.get('error', 'Unknown error')}"
                    )

            # Performance insights
            if successful_tests:
                report_lines.append("")
                report_lines.append("💡 PERFORMANCE INSIGHTS")
                report_lines.append("-" * 30)

                # Find fastest operation
                fastest = max(successful_tests,
                            key=lambda x: x.get("throughput_rows_per_sec", 0))
                report_lines.append(f"Fastest operation: {fastest['operation']} " +
                                  f"({fastest.get('throughput_rows_per_sec', 0):,.0f} rows/sec)")

                # Find slowest operation
                slowest = min(successful_tests,
                            key=lambda x: x.get("throughput_rows_per_sec", float('inf')))
                report_lines.append(f"Slowest operation: {slowest['operation']} " +
                                  f"({slowest.get('throughput_rows_per_sec', 0):,.0f} rows/sec)")

            return "\n".join(report_lines)

        def _generate_benchmark_data(self, size: int) -> DataFrame:
            """Generate test data for benchmarking"""
            # Generate data in chunks to avoid memory issues
            chunk_size = min(size, 100000)
            chunks = []

            for i in range(0, size, chunk_size):
                current_chunk_size = min(chunk_size, size - i)
                chunk_data = self.test_data_generator.generate_customers(current_chunk_size)
                chunks.extend(chunk_data)

            schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("income", DoubleType(), True),
                StructField("city", StringType(), True),
                StructField("registration_date", StringType(), True)
            ])

            return self.spark.createDataFrame(chunks, schema)

        def _get_memory_metrics(self) -> Dict[str, float]:
            """Get current Spark memory metrics (simplified version)"""
            # In a real implementation, this would use Spark's StatusTracker
            # For this demo, we'll return mock values
            return {
                "storage_used": random.uniform(0, 1000),  # MB
                "storage_remaining": random.uniform(0, 2000),  # MB
                "execution_memory": random.uniform(0, 500)  # MB
            }

    return PerformanceTestFramework


def exercise_4g_create_data_contract_validator() -> type:
    """
    SOLUTION: Create a data contract validation system for ensuring data consistency.
    """

    @dataclass
    class DataContract:
        """Data contract definition"""
        name: str
        version: str
        schema: StructType
        quality_rules: Dict[str, Any]
        business_rules: List[Callable]
        sla_requirements: Dict[str, Any]

    class DataContractValidator:
        """System for validating and enforcing data contracts"""

        def __init__(self, spark: SparkSession):
            self.spark = spark
            self.contracts = {}
            self.validation_history = []

        def define_contract(self, contract: DataContract) -> None:
            """Define a new data contract"""
            self.contracts[contract.name] = contract
            print(f"📋 Data contract '{contract.name}' v{contract.version} defined")

        def validate_contract(self, df: DataFrame, contract_name: str) -> Dict[str, bool]:
            """Validate DataFrame against specified data contract"""
            if contract_name not in self.contracts:
                raise ValueError(f"Data contract '{contract_name}' not found")

            contract = self.contracts[contract_name]
            validation_results = {}

            print(f"🔍 Validating data against contract '{contract_name}' v{contract.version}...")

            # 1. Schema validation
            schema_valid = self._validate_schema_contract(df, contract.schema)
            validation_results["schema"] = schema_valid

            # 2. Quality rules validation
            quality_valid = self._validate_quality_contract(df, contract.quality_rules)
            validation_results["quality"] = quality_valid

            # 3. Business rules validation
            business_valid = self._validate_business_contract(df, contract.business_rules)
            validation_results["business_rules"] = business_valid

            # 4. SLA requirements validation
            sla_valid = self._validate_sla_contract(df, contract.sla_requirements)
            validation_results["sla"] = sla_valid

            # Record validation
            validation_record = {
                "timestamp": datetime.now().isoformat(),
                "contract_name": contract_name,
                "contract_version": contract.version,
                "data_rows": df.count(),
                "results": validation_results,
                "overall_valid": all(validation_results.values())
            }
            self.validation_history.append(validation_record)

            overall_status = "PASSED" if validation_record["overall_valid"] else "FAILED"
            print(f"📊 Contract validation {overall_status}")

            return validation_results

        def _validate_schema_contract(self, df: DataFrame, expected_schema: StructType) -> bool:
            """Validate schema against contract"""
            try:
                validator = exercise_4c_create_data_quality_validator()(self.spark)
                type_validation = validator.check_data_types(df, expected_schema)
                schema_valid = all(type_validation.values())

                if schema_valid:
                    print("  ✅ Schema: Compliant")
                else:
                    failed_fields = [k for k, v in type_validation.items() if not v]
                    print(f"  ❌ Schema: Failed fields {failed_fields}")

                return schema_valid
            except Exception as e:
                print(f"  ❌ Schema: Validation error - {e}")
                return False

        def _validate_quality_contract(self, df: DataFrame, quality_rules: Dict[str, Any]) -> bool:
            """Validate data quality against contract"""
            try:
                validator = exercise_4c_create_data_quality_validator()(self.spark)

                quality_checks = []

                # Completeness check
                if "min_completeness" in quality_rules:
                    completeness = validator.check_completeness(df)
                    min_completeness = quality_rules["min_completeness"]

                    for column, completeness_rate in completeness.items():
                        check_passed = completeness_rate >= min_completeness
                        quality_checks.append(check_passed)
                        if not check_passed:
                            print(f"    ❌ Completeness: {column} only {completeness_rate}% complete")

                # Uniqueness check
                if "unique_columns" in quality_rules:
                    unique_columns = quality_rules["unique_columns"]
                    uniqueness = validator.check_uniqueness(df, unique_columns)

                    for column, duplicate_count in uniqueness.items():
                        check_passed = duplicate_count == 0
                        quality_checks.append(check_passed)
                        if not check_passed:
                            print(f"    ❌ Uniqueness: {column} has {duplicate_count} duplicates")

                # Range validation
                if "range_rules" in quality_rules:
                    range_validation = validator.check_value_ranges(df, quality_rules["range_rules"])

                    for column, range_valid in range_validation.items():
                        quality_checks.append(range_valid)
                        if not range_valid:
                            print(f"    ❌ Range: {column} has values outside allowed range")

                quality_valid = all(quality_checks) if quality_checks else True

                if quality_valid:
                    print("  ✅ Quality: All checks passed")
                else:
                    failed_count = len([c for c in quality_checks if not c])
                    print(f"  ❌ Quality: {failed_count}/{len(quality_checks)} checks failed")

                return quality_valid

            except Exception as e:
                print(f"  ❌ Quality: Validation error - {e}")
                return False

        def _validate_business_contract(self, df: DataFrame, business_rules: List[Callable]) -> bool:
            """Validate business rules against contract"""
            try:
                validator = exercise_4c_create_data_quality_validator()(self.spark)
                business_results = validator.check_business_rules(df, business_rules)

                business_valid = all(business_results.values())

                if business_valid:
                    print("  ✅ Business Rules: All rules satisfied")
                else:
                    failed_rules = [k for k, v in business_results.items() if not v]
                    print(f"  ❌ Business Rules: Failed rules {failed_rules}")

                return business_valid

            except Exception as e:
                print(f"  ❌ Business Rules: Validation error - {e}")
                return False

        def _validate_sla_contract(self, df: DataFrame, sla_requirements: Dict[str, Any]) -> bool:
            """Validate SLA requirements against contract"""
            try:
                sla_checks = []

                # Row count SLA
                if "min_rows" in sla_requirements:
                    row_count = df.count()
                    min_rows = sla_requirements["min_rows"]
                    row_count_check = row_count >= min_rows
                    sla_checks.append(row_count_check)

                    if not row_count_check:
                        print(f"    ❌ SLA: Row count {row_count} below minimum {min_rows}")

                # Freshness SLA (if timestamp column exists)
                if "max_age_hours" in sla_requirements and "timestamp_column" in sla_requirements:
                    timestamp_col = sla_requirements["timestamp_column"]
                    max_age_hours = sla_requirements["max_age_hours"]

                    if timestamp_col in df.columns:
                        # This is a simplified check - in practice, you'd parse timestamps
                        freshness_check = True  # Placeholder
                        sla_checks.append(freshness_check)

                # Completeness SLA
                if "min_completeness_sla" in sla_requirements:
                    validator = exercise_4c_create_data_quality_validator()(self.spark)
                    completeness = validator.check_completeness(df)
                    min_completeness_sla = sla_requirements["min_completeness_sla"]

                    avg_completeness = sum(completeness.values()) / len(completeness) if completeness else 0
                    completeness_check = avg_completeness >= min_completeness_sla
                    sla_checks.append(completeness_check)

                    if not completeness_check:
                        print(f"    ❌ SLA: Average completeness {avg_completeness:.1f}% below SLA {min_completeness_sla}%")

                sla_valid = all(sla_checks) if sla_checks else True

                if sla_valid:
                    print("  ✅ SLA: All requirements met")
                else:
                    failed_count = len([c for c in sla_checks if not c])
                    print(f"  ❌ SLA: {failed_count}/{len(sla_checks)} requirements failed")

                return sla_valid

            except Exception as e:
                print(f"  ❌ SLA: Validation error - {e}")
                return False

        def check_schema_evolution(self, old_schema: StructType, new_schema: StructType,
                                 contract_name: str) -> Dict[str, Any]:
            """Check schema evolution compatibility with existing contract"""
            print(f"🔄 Checking schema evolution for contract '{contract_name}'...")

            evolution_report = {
                "compatible": True,
                "changes": [],
                "issues": [],
                "recommendations": []
            }

            old_fields = {f.name: f for f in old_schema.fields}
            new_fields = {f.name: f for f in new_schema.fields}

            # Check for added fields
            added_fields = set(new_fields.keys()) - set(old_fields.keys())
            for field_name in added_fields:
                field = new_fields[field_name]
                evolution_report["changes"].append({
                    "type": "added",
                    "field": field_name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })

                # Non-nullable added fields are potentially problematic
                if not field.nullable:
                    evolution_report["compatible"] = False
                    evolution_report["issues"].append(
                        f"Added non-nullable field '{field_name}' breaks compatibility"
                    )

            # Check for removed fields
            removed_fields = set(old_fields.keys()) - set(new_fields.keys())
            for field_name in removed_fields:
                evolution_report["changes"].append({
                    "type": "removed",
                    "field": field_name
                })
                evolution_report["compatible"] = False
                evolution_report["issues"].append(
                    f"Removed field '{field_name}' breaks compatibility"
                )

            # Check for modified fields
            for field_name in set(old_fields.keys()) & set(new_fields.keys()):
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]

                changes = {}

                # Type changes
                if type(old_field.dataType) != type(new_field.dataType):
                    changes["data_type"] = {
                        "old": str(old_field.dataType),
                        "new": str(new_field.dataType)
                    }
                    evolution_report["compatible"] = False
                    evolution_report["issues"].append(
                        f"Type change in field '{field_name}' may cause data loss"
                    )

                # Nullability changes
                if old_field.nullable != new_field.nullable:
                    changes["nullable"] = {
                        "old": old_field.nullable,
                        "new": new_field.nullable
                    }

                    # Making field non-nullable is potentially breaking
                    if old_field.nullable and not new_field.nullable:
                        evolution_report["compatible"] = False
                        evolution_report["issues"].append(
                            f"Making field '{field_name}' non-nullable breaks compatibility"
                        )

                if changes:
                    evolution_report["changes"].append({
                        "type": "modified",
                        "field": field_name,
                        **changes
                    })

            # Generate recommendations
            if not evolution_report["compatible"]:
                evolution_report["recommendations"].extend([
                    "Consider versioning the schema instead of breaking changes",
                    "Implement gradual migration strategy",
                    "Add data transformation logic for compatibility"
                ])

            status = "COMPATIBLE" if evolution_report["compatible"] else "INCOMPATIBLE"
            print(f"📋 Schema evolution: {status}")

            return evolution_report

        def validate_data_lineage(self, source_df: DataFrame, target_df: DataFrame,
                                lineage_rules: Dict[str, Any]) -> bool:
            """Validate data lineage between source and target DataFrames"""
            print("🔗 Validating data lineage...")

            lineage_checks = []

            # Row count preservation (with tolerance)
            if "row_count_tolerance" in lineage_rules:
                source_count = source_df.count()
                target_count = target_df.count()
                tolerance = lineage_rules["row_count_tolerance"]

                count_diff = abs(source_count - target_count)
                count_check = count_diff <= tolerance
                lineage_checks.append(count_check)

                if not count_check:
                    print(f"    ❌ Row count: {source_count} -> {target_count} (diff: {count_diff})")

            # Key preservation
            if "preserve_keys" in lineage_rules:
                key_columns = lineage_rules["preserve_keys"]

                for key_col in key_columns:
                    if key_col in source_df.columns and key_col in target_df.columns:
                        source_keys = set([row[key_col] for row in source_df.select(key_col).collect()])
                        target_keys = set([row[key_col] for row in target_df.select(key_col).collect()])

                        missing_keys = source_keys - target_keys
                        key_check = len(missing_keys) == 0
                        lineage_checks.append(key_check)

                        if not key_check:
                            print(f"    ❌ Key preservation: {len(missing_keys)} keys lost in {key_col}")

            # Aggregate preservation
            if "preserve_aggregates" in lineage_rules:
                agg_rules = lineage_rules["preserve_aggregates"]

                for column, agg_func in agg_rules.items():
                    if column in source_df.columns and column in target_df.columns:
                        if agg_func == "sum":
                            source_sum = source_df.agg(spark_sum(column)).collect()[0][0]
                            target_sum = target_df.agg(spark_sum(column)).collect()[0][0]

                            # Allow small differences due to floating point precision
                            sum_check = abs(source_sum - target_sum) < 0.01
                            lineage_checks.append(sum_check)

                            if not sum_check:
                                print(f"    ❌ Sum preservation: {column} {source_sum} -> {target_sum}")

            lineage_valid = all(lineage_checks) if lineage_checks else True

            if lineage_valid:
                print("  ✅ Data Lineage: Validation passed")
            else:
                failed_count = len([c for c in lineage_checks if not c])
                print(f"  ❌ Data Lineage: {failed_count}/{len(lineage_checks)} checks failed")

            return lineage_valid

        def generate_contract_report(self, contract_name: str) -> str:
            """Generate compliance report for a specific contract"""
            if contract_name not in self.contracts:
                return f"Contract '{contract_name}' not found"

            contract = self.contracts[contract_name]

            # Find validation history for this contract
            contract_validations = [v for v in self.validation_history
                                  if v["contract_name"] == contract_name]

            if not contract_validations:
                return f"No validation history found for contract '{contract_name}'"

            report_lines = []
            report_lines.append(f"📋 DATA CONTRACT COMPLIANCE REPORT")
            report_lines.append(f"Contract: {contract_name} v{contract.version}")
            report_lines.append("=" * 50)
            report_lines.append(f"Generated: {datetime.now().isoformat()}")
            report_lines.append("")

            # Summary statistics
            total_validations = len(contract_validations)
            successful_validations = len([v for v in contract_validations if v["overall_valid"]])
            compliance_rate = (successful_validations / total_validations * 100) if total_validations > 0 else 0

            report_lines.append(f"📊 COMPLIANCE SUMMARY")
            report_lines.append(f"Total validations: {total_validations}")
            report_lines.append(f"Successful validations: {successful_validations}")
            report_lines.append(f"Compliance rate: {compliance_rate:.1f}%")
            report_lines.append("")

            # Recent validation results
            recent_validations = sorted(contract_validations,
                                      key=lambda x: x["timestamp"], reverse=True)[:5]

            report_lines.append(f"🕐 RECENT VALIDATIONS")
            report_lines.append("-" * 30)

            for validation in recent_validations:
                status = "✅ PASSED" if validation["overall_valid"] else "❌ FAILED"
                report_lines.append(f"{validation['timestamp']}: {status}")

                for check_type, result in validation["results"].items():
                    check_status = "✅" if result else "❌"
                    report_lines.append(f"  {check_status} {check_type}")

                report_lines.append(f"  Data rows: {validation['data_rows']:,}")
                report_lines.append("")

            return "\n".join(report_lines)

        def get_contract_status(self) -> Dict[str, Dict[str, Any]]:
            """Get status of all defined contracts"""
            status = {}

            for contract_name, contract in self.contracts.items():
                contract_validations = [v for v in self.validation_history
                                      if v["contract_name"] == contract_name]

                if contract_validations:
                    latest_validation = max(contract_validations, key=lambda x: x["timestamp"])
                    total_validations = len(contract_validations)
                    successful_validations = len([v for v in contract_validations if v["overall_valid"]])
                    compliance_rate = (successful_validations / total_validations * 100)

                    status[contract_name] = {
                        "version": contract.version,
                        "last_validation": latest_validation["timestamp"],
                        "last_validation_passed": latest_validation["overall_valid"],
                        "total_validations": total_validations,
                        "compliance_rate": compliance_rate
                    }
                else:
                    status[contract_name] = {
                        "version": contract.version,
                        "last_validation": None,
                        "last_validation_passed": None,
                        "total_validations": 0,
                        "compliance_rate": 0
                    }

            return status

    return DataContractValidator, DataContract


def exercise_4h_create_mock_data_sources() -> Dict[str, type]:
    """
    SOLUTION: Create mock data sources for testing external dependencies.
    """

    class MockDatabase:
        """Mock database connection for testing database interactions"""

        def __init__(self, spark: SparkSession):
            self.spark = spark
            self.tables = {}
            self.connection_status = "connected"
            self.query_log = []

        def create_table(self, table_name: str, df: DataFrame) -> None:
            """Create a mock table with data"""
            self.tables[table_name] = df
            print(f"📄 Mock table '{table_name}' created with {df.count()} rows")

        def read_table(self, table_name: str) -> DataFrame:
            """Read data from mock table"""
            if self.connection_status != "connected":
                raise Exception("Database connection failed")

            if table_name not in self.tables:
                raise Exception(f"Table '{table_name}' not found")

            self.query_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "read",
                "table": table_name
            })

            return self.tables[table_name]

        def execute_query(self, sql: str) -> DataFrame:
            """Execute SQL query on mock database"""
            if self.connection_status != "connected":
                raise Exception("Database connection failed")

            self.query_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "query",
                "sql": sql[:100] + "..." if len(sql) > 100 else sql
            })

            # Simple mock query execution - in practice, you'd parse SQL
            if "customers" in sql.lower() and "customers" in self.tables:
                return self.tables["customers"]
            elif "products" in sql.lower() and "products" in self.tables:
                return self.tables["products"]
            else:
                # Return empty DataFrame with basic schema
                schema = StructType([StructField("result", StringType(), True)])
                return self.spark.createDataFrame([("mock_result",)], schema)

        def write_table(self, df: DataFrame, table_name: str) -> None:
            """Write DataFrame to mock table"""
            if self.connection_status != "connected":
                raise Exception("Database connection failed")

            self.tables[table_name] = df
            self.query_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "write",
                "table": table_name,
                "rows": df.count()
            })

            print(f"💾 Written {df.count()} rows to mock table '{table_name}'")

        def simulate_connection_failure(self) -> None:
            """Simulate database connection failure for testing error handling"""
            self.connection_status = "failed"
            print("❌ Simulated database connection failure")

        def restore_connection(self) -> None:
            """Restore database connection"""
            self.connection_status = "connected"
            print("✅ Database connection restored")

        def get_query_log(self) -> List[Dict[str, Any]]:
            """Get log of all database operations"""
            return self.query_log.copy()

    class MockFileSystem:
        """Mock file system for testing file I/O operations"""

        def __init__(self, spark: SparkSession):
            self.spark = spark
            self.files = {}
            self.directories = set()
            self.access_log = []

        def write_file(self, path: str, df: DataFrame, format: str = "parquet") -> None:
            """Write DataFrame to mock file system"""
            self.files[path] = {
                "data": df,
                "format": format,
                "created": datetime.now().isoformat(),
                "size": df.count()
            }

            # Add directory to directories set
            directory = "/".join(path.split("/")[:-1])
            if directory:
                self.directories.add(directory)

            self.access_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "write",
                "path": path,
                "format": format,
                "rows": df.count()
            })

            print(f"💾 Written {df.count()} rows to mock file '{path}' ({format})")

        def read_file(self, path: str) -> DataFrame:
            """Read DataFrame from mock file system"""
            if path not in self.files:
                raise FileNotFoundError(f"File '{path}' not found")

            file_info = self.files[path]

            self.access_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "read",
                "path": path,
                "format": file_info["format"]
            })

            return file_info["data"]

        def list_files(self, directory: str = "") -> List[str]:
            """List files in mock directory"""
            if directory:
                matching_files = [path for path in self.files.keys()
                                if path.startswith(directory)]
            else:
                matching_files = list(self.files.keys())

            self.access_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "list",
                "directory": directory,
                "files_found": len(matching_files)
            })

            return matching_files

        def delete_file(self, path: str) -> None:
            """Delete file from mock file system"""
            if path not in self.files:
                raise FileNotFoundError(f"File '{path}' not found")

            del self.files[path]

            self.access_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "delete",
                "path": path
            })

            print(f"🗑️ Deleted mock file '{path}'")

        def file_exists(self, path: str) -> bool:
            """Check if file exists in mock file system"""
            exists = path in self.files

            self.access_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "exists",
                "path": path,
                "exists": exists
            })

            return exists

        def get_file_info(self, path: str) -> Dict[str, Any]:
            """Get information about mock file"""
            if path not in self.files:
                raise FileNotFoundError(f"File '{path}' not found")

            return self.files[path].copy()

        def simulate_permission_error(self, path: str) -> None:
            """Simulate permission error for testing error handling"""
            if path in self.files:
                self.files[path]["permission_denied"] = True
            print(f"❌ Simulated permission error for '{path}'")

        def get_access_log(self) -> List[Dict[str, Any]]:
            """Get log of all file system operations"""
            return self.access_log.copy()

    class MockAPIClient:
        """Mock REST API client for testing API integrations"""

        def __init__(self):
            self.base_url = "http://mock-api.example.com"
            self.responses = {}
            self.request_log = []
            self.status = "available"

        def setup_response(self, endpoint: str, response_data: Any,
                         status_code: int = 200) -> None:
            """Setup mock response for specific endpoint"""
            self.responses[endpoint] = {
                "data": response_data,
                "status_code": status_code
            }
            print(f"🔧 Mock response configured for {endpoint}")

        def get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
            """Mock GET request"""
            if self.status != "available":
                raise Exception("API service unavailable")

            self.request_log.append({
                "timestamp": datetime.now().isoformat(),
                "method": "GET",
                "endpoint": endpoint,
                "params": params
            })

            if endpoint in self.responses:
                response = self.responses[endpoint]
                if response["status_code"] != 200:
                    raise Exception(f"API error: {response['status_code']}")
                return response["data"]
            else:
                # Default mock response
                return {
                    "status": "success",
                    "data": [{"mock": "data", "endpoint": endpoint}]
                }

        def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
            """Mock POST request"""
            if self.status != "available":
                raise Exception("API service unavailable")

            self.request_log.append({
                "timestamp": datetime.now().isoformat(),
                "method": "POST",
                "endpoint": endpoint,
                "data_size": len(str(data))
            })

            if endpoint in self.responses:
                response = self.responses[endpoint]
                if response["status_code"] != 200:
                    raise Exception(f"API error: {response['status_code']}")
                return response["data"]
            else:
                return {
                    "status": "created",
                    "id": f"mock_{random.randint(1000, 9999)}"
                }

        def simulate_api_failure(self) -> None:
            """Simulate API service failure for testing error handling"""
            self.status = "unavailable"
            print("❌ Simulated API service failure")

        def restore_api_service(self) -> None:
            """Restore API service"""
            self.status = "available"
            print("✅ API service restored")

        def get_request_log(self) -> List[Dict[str, Any]]:
            """Get log of all API requests"""
            return self.request_log.copy()

    class MockStreamingSource:
        """Mock streaming data source for testing streaming applications"""

        def __init__(self, spark: SparkSession):
            self.spark = spark
            self.is_streaming = False
            self.message_queue = []
            self.consumer_log = []

        def start_stream(self, topic: str = "test_topic") -> None:
            """Start mock streaming"""
            self.is_streaming = True
            self.topic = topic
            print(f"🌊 Started mock streaming from topic '{topic}'")

        def stop_stream(self) -> None:
            """Stop mock streaming"""
            self.is_streaming = False
            print("🛑 Stopped mock streaming")

        def add_messages(self, messages: List[Dict[str, Any]]) -> None:
            """Add messages to mock stream"""
            for message in messages:
                message["timestamp"] = datetime.now().isoformat()
                message["offset"] = len(self.message_queue)
                self.message_queue.append(message)

            print(f"📨 Added {len(messages)} messages to mock stream")

        def consume_messages(self, batch_size: int = 100) -> List[Dict[str, Any]]:
            """Consume messages from mock stream"""
            if not self.is_streaming:
                raise Exception("Stream not started")

            # Return a batch of messages
            batch = self.message_queue[:batch_size]
            self.message_queue = self.message_queue[batch_size:]

            self.consumer_log.append({
                "timestamp": datetime.now().isoformat(),
                "batch_size": len(batch),
                "remaining_messages": len(self.message_queue)
            })

            return batch

        def create_streaming_dataframe(self, schema: StructType) -> DataFrame:
            """Create a mock streaming DataFrame for testing"""
            # In a real implementation, this would create an actual streaming DataFrame
            # For testing purposes, we'll return a batch DataFrame

            if not self.message_queue:
                # Create empty DataFrame
                return self.spark.createDataFrame([], schema)

            # Convert messages to DataFrame
            batch = self.consume_messages()

            # Extract data fields from messages
            data = []
            for message in batch:
                if "data" in message:
                    # Assume message data matches schema
                    data.append(tuple(message["data"].values()))

            return self.spark.createDataFrame(data, schema)

        def simulate_stream_failure(self) -> None:
            """Simulate streaming failure for testing error handling"""
            self.is_streaming = False
            print("❌ Simulated streaming failure")

        def get_consumer_log(self) -> List[Dict[str, Any]]:
            """Get log of message consumption"""
            return self.consumer_log.copy()

        def get_stream_status(self) -> Dict[str, Any]:
            """Get current stream status"""
            return {
                "is_streaming": self.is_streaming,
                "topic": getattr(self, "topic", None),
                "queue_size": len(self.message_queue),
                "total_consumed": len(self.consumer_log)
            }

    return {
        "MockDatabase": MockDatabase,
        "MockFileSystem": MockFileSystem,
        "MockAPIClient": MockAPIClient,
        "MockStreamingSource": MockStreamingSource
    }


def run_complete_solution():
    """Run the complete solution demonstration"""
    print("🚀 Running Complete Solution: Testing Framework")
    print("=" * 55)

    # Initialize Spark session for testing
    spark = (SparkSession.builder
            .appName("TestingSolutionDemo")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())

    try:
        # Step 1: Spark Test Base
        print("\n🧪 Step 1: Spark Test Base")
        SparkTestBase = exercise_4a_create_spark_test_base()
        print(f"✅ SparkTestBase class created with test utilities")

        # Step 2: Test Data Generator
        print("\n📊 Step 2: Test Data Generator")
        TestDataGenerator = exercise_4b_create_test_data_generator()
        generator = TestDataGenerator()

        customers = generator.generate_customers(10)
        products = generator.generate_products(5)
        sales = generator.generate_sales(20, [c[0] for c in customers])

        print(f"✅ Generated test data:")
        print(f"  - {len(customers)} customers")
        print(f"  - {len(products)} products")
        print(f"  - {len(sales)} sales transactions")

        # Step 3: Data Quality Validator
        print("\n🔍 Step 3: Data Quality Validator")
        DataQualityValidator = exercise_4c_create_data_quality_validator()
        validator = DataQualityValidator(spark)

        # Create test DataFrame
        customer_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("income", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("registration_date", StringType(), True)
        ])
        customers_df = spark.createDataFrame(customers, customer_schema)

        quality_report = validator.generate_quality_report(customers_df)
        print(f"✅ Data quality report generated:")
        print(f"  - Quality score: {quality_report['quality_score']}%")
        print(f"  - Row count: {quality_report['dataset_info']['row_count']:,}")

        # Step 4: Transformation Tests
        print("\n🔄 Step 4: Transformation Tests")
        test_classes = exercise_4d_create_transformation_tests()
        print(f"✅ Created {len(test_classes)} test classes:")
        for name in test_classes.keys():
            print(f"  - {name}")

        # Step 5: Integration Test Framework
        print("\n🔗 Step 5: Integration Test Framework")
        IntegrationTestFramework = exercise_4e_create_integration_test_framework()
        integration_framework = IntegrationTestFramework(spark)

        integration_framework.setup_test_environment()
        test_datasets = integration_framework.create_test_datasets()
        print(f"✅ Integration test framework ready with {len(test_datasets)} datasets")

        # Step 6: Performance Test Framework
        print("\n⚡ Step 6: Performance Test Framework")
        PerformanceTestFramework = exercise_4f_create_performance_test_framework()
        perf_framework = PerformanceTestFramework(spark)

        # Test simple operation
        def simple_count_operation(df):
            return df.count()

        benchmark_result = perf_framework.benchmark_operation(
            simple_count_operation, 1000, "count_operation"
        )
        print(f"✅ Performance benchmark completed:")
        print(f"  - Execution time: {benchmark_result.get('execution_time', 0):.2f}s")

        # Step 7: Data Contract Validator
        print("\n📋 Step 7: Data Contract Validator")
        DataContractValidator, DataContract = exercise_4g_create_data_contract_validator()
        contract_validator = DataContractValidator(spark)

        # Define a sample contract
        sample_contract = DataContract(
            name="customer_data_contract",
            version="1.0",
            schema=customer_schema,
            quality_rules={
                "min_completeness": 95,
                "unique_columns": ["customer_id"],
                "range_rules": {
                    "age": {"min": 18, "max": 120},
                    "income": {"min": 0}
                }
            },
            business_rules=[],
            sla_requirements={
                "min_rows": 1
            }
        )

        contract_validator.define_contract(sample_contract)
        validation_results = contract_validator.validate_contract(
            customers_df, "customer_data_contract"
        )
        print(f"✅ Data contract validation:")
        print(f"  - Schema: {'✅' if validation_results.get('schema') else '❌'}")
        print(f"  - Quality: {'✅' if validation_results.get('quality') else '❌'}")

        # Step 8: Mock Data Sources
        print("\n🔧 Step 8: Mock Data Sources")
        mock_classes = exercise_4h_create_mock_data_sources()
        print(f"✅ Created {len(mock_classes)} mock classes:")
        for name in mock_classes.keys():
            print(f"  - {name}")

        # Demonstrate mock database
        MockDatabase = mock_classes["MockDatabase"]
        mock_db = MockDatabase(spark)
        mock_db.create_table("customers", customers_df)
        retrieved_df = mock_db.read_table("customers")
        print(f"  - Mock database: {retrieved_df.count()} rows retrieved")

        print("\n🎯 Testing Benefits:")
        print("  ✅ Comprehensive test coverage - unit, integration, performance")
        print("  ✅ Data quality validation - automated quality checks")
        print("  ✅ Mock dependencies - isolated testing environment")
        print("  ✅ Performance benchmarking - scalability testing")
        print("  ✅ Data contracts - schema and business rule validation")
        print("  ✅ Integration testing - end-to-end pipeline validation")

        print("\n📈 Testing Best Practices Demonstrated:")
        print("  • Realistic test data generation")
        print("  • Automated quality validation")
        print("  • Schema evolution compatibility")
        print("  • Performance regression detection")
        print("  • Error condition simulation")
        print("  • Test environment isolation")

    finally:
        spark.stop()

    print("\n🎉 Solution demonstration completed!")
    print("🧪 This framework provides enterprise-grade testing for Spark applications!")


if __name__ == "__main__":
    run_complete_solution()