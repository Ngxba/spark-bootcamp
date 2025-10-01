"""
Unit tests for data quality validation functionality.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class TestDataQualityValidation:
    """Test cases for data quality validation functions."""

    def test_completeness_validation_all_complete(self, spark_session, customer_schema, sample_customer_data):
        """Test completeness validation with complete data."""
        # Create DataFrame with complete data
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Test completeness - all columns should be 100% complete
        total_rows = df.count()
        for column in df.columns:
            non_null_count = df.filter(df[column].isNotNull()).count()
            completeness = (non_null_count / total_rows) * 100
            assert completeness == 100.0, f"Column {column} should be 100% complete"

    def test_completeness_validation_with_nulls(self, spark_session, customer_schema, corrupted_customer_data):
        """Test completeness validation with null values."""
        # Create DataFrame with null values
        df = spark_session.createDataFrame(corrupted_customer_data, customer_schema)

        # Test completeness - some columns should have nulls
        total_rows = df.count()

        # Check 'name' column (should have 1 null out of 5 records = 80% complete)
        name_non_null = df.filter(df["name"].isNotNull()).count()
        name_completeness = (name_non_null / total_rows) * 100
        assert name_completeness == 80.0, f"Name column should be 80% complete, got {name_completeness}%"

        # Check 'age' column (should have 1 null out of 5 records = 80% complete)
        age_non_null = df.filter(df["age"].isNotNull()).count()
        age_completeness = (age_non_null / total_rows) * 100
        assert age_completeness == 80.0, f"Age column should be 80% complete, got {age_completeness}%"

    def test_uniqueness_validation_unique_data(self, spark_session, customer_schema, sample_customer_data):
        """Test uniqueness validation with unique data."""
        # Create DataFrame with unique customer IDs
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Test uniqueness - customer_id should be unique
        total_count = df.count()
        unique_count = df.select("customer_id").distinct().count()
        assert total_count == unique_count, "Customer IDs should be unique"

    def test_uniqueness_validation_duplicate_data(self, spark_session, customer_schema, corrupted_customer_data):
        """Test uniqueness validation with duplicate data."""
        # Create DataFrame with duplicate customer IDs
        df = spark_session.createDataFrame(corrupted_customer_data, customer_schema)

        # Test uniqueness - customer_id should have duplicates
        total_count = df.count()
        unique_count = df.select("customer_id").distinct().count()
        duplicate_count = total_count - unique_count
        assert duplicate_count > 0, "Should detect duplicate customer IDs"
        assert duplicate_count == 1, f"Should have exactly 1 duplicate, found {duplicate_count}"

    def test_data_type_validation_correct_types(self, spark_session, customer_schema, sample_customer_data):
        """Test data type validation with correct types."""
        # Create DataFrame with correct schema
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Verify schema matches expected types
        actual_schema = df.schema
        expected_schema = customer_schema

        assert len(actual_schema.fields) == len(expected_schema.fields)

        for actual_field, expected_field in zip(actual_schema.fields, expected_schema.fields):
            assert actual_field.name == expected_field.name
            assert type(actual_field.dataType) == type(expected_field.dataType)

    def test_range_validation_valid_ranges(self, spark_session, customer_schema, sample_customer_data):
        """Test range validation with values in valid ranges."""
        # Create DataFrame
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Test age range (should be between 18 and 120)
        invalid_ages = df.filter((df["age"] < 18) | (df["age"] > 120)).count()
        assert invalid_ages == 0, "All ages should be in valid range (18-120)"

        # Test income range (should be positive)
        invalid_incomes = df.filter(df["income"] < 0).count()
        assert invalid_incomes == 0, "All incomes should be positive"

    def test_range_validation_invalid_ranges(self, spark_session, customer_schema, corrupted_customer_data):
        """Test range validation with values outside valid ranges."""
        # Create DataFrame with invalid ranges
        df = spark_session.createDataFrame(corrupted_customer_data, customer_schema)

        # Test age range violations
        invalid_ages = df.filter((df["age"] < 18) | (df["age"] > 120)).count()
        assert invalid_ages > 0, "Should detect invalid age values"

        # Test income range violations
        invalid_incomes = df.filter(df["income"] < 0).count()
        assert invalid_incomes > 0, "Should detect negative income values"

    def test_email_format_validation_valid_emails(self, spark_session, customer_schema, sample_customer_data):
        """Test email format validation with valid emails."""
        # Create DataFrame
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Test email format (should contain @ and .)
        valid_emails = df.filter(df["email"].rlike(r'^[^@]+@[^@]+\.[^@]+$')).count()
        total_emails = df.filter(df["email"].isNotNull()).count()

        assert valid_emails == total_emails, "All emails should be in valid format"

    def test_email_format_validation_invalid_emails(self, spark_session, customer_schema, corrupted_customer_data):
        """Test email format validation with invalid emails."""
        # Create DataFrame with invalid emails
        df = spark_session.createDataFrame(corrupted_customer_data, customer_schema)

        # Test email format violations
        invalid_emails = df.filter(
            df["email"].isNotNull() & ~df["email"].rlike(r'^[^@]+@[^@]+\.[^@]+$')
        ).count()

        assert invalid_emails > 0, "Should detect invalid email formats"

    def test_comprehensive_quality_score(self, spark_session, customer_schema, sample_customer_data):
        """Test comprehensive data quality scoring."""
        # Create DataFrame with good quality data
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Calculate quality metrics
        total_rows = df.count()
        quality_checks = []

        # Completeness check (should be 100%)
        for column in df.columns:
            non_null_count = df.filter(df[column].isNotNull()).count()
            completeness = (non_null_count / total_rows) * 100
            quality_checks.append(completeness >= 95)  # 95% threshold

        # Uniqueness check for customer_id
        unique_count = df.select("customer_id").distinct().count()
        quality_checks.append(total_rows == unique_count)

        # Range checks
        valid_ages = df.filter((df["age"] >= 18) & (df["age"] <= 120)).count()
        quality_checks.append(valid_ages == total_rows)

        valid_incomes = df.filter(df["income"] >= 0).count()
        quality_checks.append(valid_incomes == total_rows)

        # Format checks
        valid_emails = df.filter(df["email"].rlike(r'^[^@]+@[^@]+\.[^@]+$')).count()
        quality_checks.append(valid_emails == total_rows)

        # Calculate overall quality score
        passed_checks = sum(quality_checks)
        total_checks = len(quality_checks)
        quality_score = (passed_checks / total_checks) * 100

        assert quality_score == 100.0, f"Quality score should be 100%, got {quality_score}%"

    def test_empty_dataframe_validation(self, spark_session, customer_schema):
        """Test validation behavior with empty DataFrame."""
        # Create empty DataFrame
        df = spark_session.createDataFrame([], customer_schema)

        # Test that validation handles empty data gracefully
        assert df.count() == 0, "DataFrame should be empty"

        # Completeness should return 0 for empty DataFrame
        for column in df.columns:
            non_null_count = df.filter(df[column].isNotNull()).count()
            assert non_null_count == 0, f"Empty DataFrame should have 0 non-null values in {column}"

    def test_schema_validation_missing_columns(self, spark_session, sample_customer_data):
        """Test schema validation with missing columns."""
        # Create schema missing some columns
        incomplete_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
            # Missing email, income, city, registration_date
        ])

        # Create DataFrame with incomplete schema
        incomplete_data = [(row[0], row[1], row[2]) for row in sample_customer_data]
        df = spark_session.createDataFrame(incomplete_data, incomplete_schema)

        # Verify missing columns
        expected_columns = {"customer_id", "name", "age", "email", "income", "city", "registration_date"}
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns

        assert len(missing_columns) == 4, f"Should have 4 missing columns, found {len(missing_columns)}"
        assert "email" in missing_columns, "Email column should be missing"
        assert "income" in missing_columns, "Income column should be missing"

    def test_data_profiling_statistics(self, spark_session, customer_schema, sample_customer_data):
        """Test data profiling and statistical analysis."""
        # Create DataFrame
        df = spark_session.createDataFrame(sample_customer_data, customer_schema)

        # Test basic statistics for numeric columns
        age_stats = df.select("age").describe().collect()
        income_stats = df.select("income").describe().collect()

        # Verify statistics are calculated
        assert len(age_stats) == 5, "Should have 5 statistical measures (count, mean, stddev, min, max)"
        assert len(income_stats) == 5, "Should have 5 statistical measures"

        # Check specific statistics
        age_values = [row.age for row in df.collect()]
        expected_min_age = min(age_values)
        expected_max_age = max(age_values)

        # Find min and max from describe output
        age_min = float([row for row in age_stats if row.summary == "min"][0].age)
        age_max = float([row for row in age_stats if row.summary == "max"][0].age)

        assert age_min == expected_min_age, f"Min age should be {expected_min_age}, got {age_min}"
        assert age_max == expected_max_age, f"Max age should be {expected_max_age}, got {age_max}"

    def test_referential_integrity(self, spark_session, customer_schema, sample_customer_data,
                                  sales_schema, sample_sales_data):
        """Test referential integrity between related datasets."""
        # Create customer and sales DataFrames
        customers_df = spark_session.createDataFrame(sample_customer_data, customer_schema)
        sales_df = spark_session.createDataFrame(sample_sales_data, sales_schema)

        # Test referential integrity - all customer_ids in sales should exist in customers
        orphaned_sales = sales_df.join(
            customers_df.select("customer_id"),
            "customer_id",
            "left_anti"
        )

        orphaned_count = orphaned_sales.count()
        assert orphaned_count == 0, f"Found {orphaned_count} sales records with invalid customer_ids"

        # Test referential integrity in reverse - customers with sales
        customers_with_sales = customers_df.join(
            sales_df.select("customer_id").distinct(),
            "customer_id",
            "inner"
        )

        customers_with_sales_count = customers_with_sales.count()
        assert customers_with_sales_count > 0, "Should find customers with sales records"