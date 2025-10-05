"""
Comprehensive test cases for all exercise solutions
==================================================

These tests validate the solutions for all exercises.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

# Import all solution functions
from solutions.exercise_1_solution import *
from solutions.exercise_2_solution import *
from solutions.exercise_3_solution import *
from solutions.exercise_4_solution import *
from solutions.exercise_5_solution import *
from solutions.exercise_6_solution import *
from solutions.exercise_7_solution import *


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("TestAllExercises")
        .master("local[4]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestExercise1Solutions:
    """Test Exercise 1 solutions."""

    def test_all_exercise_1_functions(self, spark):
        """Test all Exercise 1 functions work correctly."""
        # Test data
        data = [("Alice", 25), ("Bob", 30)]
        columns = ["name", "age"]

        # Test 1a
        result_1a = exercise_1a(spark, data, columns)
        assert result_1a.count() == 2

        # Test 1b
        dict_data = [{"name": "Alice", "age": 25, "salary": 50000}]
        result_1b, schema = exercise_1b(spark, dict_data)
        assert result_1b.count() == 1
        assert "struct<" in schema.lower()

        # Test 1c
        tuple_data = [("Alice", 25, 80000.0)]
        result_1c = exercise_1c(spark, tuple_data)
        assert result_1c.schema.fields[2].dataType == DoubleType()


class TestExercise2Solutions:
    """Test Exercise 2 solutions."""

    def test_exercise_2a_column_transformations(self, spark):
        """Test column transformations."""
        employee_data = [("Alice Johnson", 28, "Engineering", 85000)]
        result = exercise_2a(spark, employee_data)

        assert "name_upper" in result.columns
        assert "salary_bonus" in result.columns
        assert "total_compensation" in result.columns

        row = result.collect()[0]
        assert row["name_upper"] == "ALICE JOHNSON"
        assert row["salary_bonus"] == 8500.0  # 10% of 85000

    def test_exercise_2b_conditional_logic(self, spark):
        """Test conditional transformations."""
        product_data = [("Laptop Pro", "Electronics", 1200.00, 25)]
        result = exercise_2b(spark, product_data)

        assert "price_category" in result.columns
        assert "stock_status" in result.columns

        row = result.collect()[0]
        assert row["price_category"] == "Premium"  # > 500
        assert row["stock_status"] == "In Stock"  # >= 10


class TestExercise3Solutions:
    """Test Exercise 3 solutions."""

    def test_exercise_3a_basic_sql(self, spark):
        """Test basic SQL queries."""
        employee_data = [
            ("Alice", "Engineering", 85000, 28),
            ("Bob", "Marketing", 65000, 34),
        ]
        result = exercise_3a(spark, employee_data)

        # Should filter for Engineering only
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_exercise_3b_sql_aggregations(self, spark):
        """Test SQL aggregations."""
        sales_data = [
            ("Laptop", "Electronics", 1200, "North"),
            ("Mouse", "Electronics", 25, "South"),
        ]
        result = exercise_3b(spark, sales_data)

        rows = result.collect()
        assert len(rows) == 1  # One category (Electronics)
        assert rows[0]["category"] == "Electronics"
        assert rows[0]["product_count"] == 2


class TestExercise4Solutions:
    """Test Exercise 4 solutions."""

    def test_exercise_4a_inner_join(self, spark):
        """Test inner join functionality."""
        employees_data = [(1, "Alice", 10, 85000)]
        departments_data = [(10, "Engineering", "Building A")]

        result = exercise_4a(spark, employees_data, departments_data)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["dept_name"] == "Engineering"

    def test_exercise_4b_left_join(self, spark):
        """Test left join with aggregation."""
        customers_data = [(1, "Alice", "alice@email.com")]
        orders_data = [(101, 1, 250.0, "2023-01-15")]

        result = exercise_4b(spark, customers_data, orders_data)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["customer_name"] == "Alice"
        assert rows[0]["total_spent"] == 250.0


class TestExercise5Solutions:
    """Test Exercise 5 solutions."""

    def test_exercise_5a_string_functions(self, spark):
        """Test string manipulation functions."""
        people_data = [("John Smith", "john@email.com", "(555) 123-4567")]
        result = exercise_5a(spark, people_data)

        assert "first_name" in result.columns
        assert "email_domain" in result.columns
        assert "phone_clean" in result.columns

        row = result.collect()[0]
        assert row["first_name"] == "John"
        assert row["email_domain"] == "email.com"
        assert row["phone_clean"] == "5551234567"

    def test_exercise_5b_numeric_functions(self, spark):
        """Test numeric calculations."""
        sales_data = [("Laptop", 2, 999.99, 0.1)]
        result = exercise_5b(spark, sales_data)

        assert "gross_amount" in result.columns
        assert "discount_amount" in result.columns
        assert "price_category" in result.columns

        row = result.collect()[0]
        assert abs(row["gross_amount"] - 1999.98) < 0.01
        assert row["price_category"] == "Premium"  # > 100


class TestExercise6Solutions:
    """Test Exercise 6 solutions."""

    def test_exercise_6a_statistical_analysis(self, spark):
        """Test statistical analysis functions."""
        sales_data = [
            ("Electronics", "North", 1500.00, 3),
            ("Electronics", "South", 2200.00, 4),
        ]
        result = exercise_6a(spark, sales_data)

        assert "total_sales" in result.columns
        assert "avg_sales" in result.columns
        assert "std_sales" in result.columns

        row = result.collect()[0]
        assert row["product_category"] == "Electronics"
        assert row["total_sales"] == 3700.0
        assert row["sales_count"] == 2

    def test_exercise_6b_window_functions(self, spark):
        """Test window functions for ranking."""
        employee_data = [
            ("Alice", "Engineering", 95000, 8),
            ("Bob", "Engineering", 87000, 5),
        ]
        result = exercise_6b(spark, employee_data)

        assert "salary_rank_overall" in result.columns
        assert "salary_rank_dept" in result.columns

        rows = result.collect()
        # Alice should rank higher (rank 1) due to higher salary
        alice_row = next(row for row in rows if row["name"] == "Alice")
        assert alice_row["salary_rank_overall"] == 1


class TestExercise7Solutions:
    """Test Exercise 7 solutions."""

    def test_exercise_7a_rdd_vs_dataframe(self, spark):
        """Test RDD vs DataFrame performance comparison."""
        data = [("Alice", 28, 85000), ("Bob", 34, 75000)]

        # Test RDD approach
        result_rdd = exercise_7a_rdd(spark, data)
        assert len(result_rdd) == 3  # count, avg_age, avg_salary

        # Test DataFrame approach
        result_df = exercise_7a_dataframe(spark, data)
        assert len(result_df) == 3

        # Results should be approximately equal
        assert abs(result_rdd[1] - result_df[1]) < 0.01  # avg_age
        assert abs(result_rdd[2] - result_df[2]) < 0.01  # avg_salary

    def test_exercise_7c_join_comparison(self, spark):
        """Test join operations in RDD vs DataFrame."""
        data1 = [(1, "Alice"), (2, "Bob")]
        data2 = [(1, 85000), (2, 75000)]

        # Test RDD join
        result_rdd = exercise_7c_rdd(spark, data1, data2)
        assert len(result_rdd) == 2

        # Test DataFrame join
        result_df = exercise_7c_dataframe(spark, data1, data2)
        assert len(result_df) == 2

        # Both should have same results
        rdd_ids = sorted([row[0] for row in result_rdd])
        df_ids = sorted([row[0] for row in result_df])
        assert rdd_ids == df_ids


class TestDatasetIntegrity:
    """Test dataset integrity and data quality."""

    def test_no_null_critical_fields(self, spark):
        """Test that critical fields don't have unexpected nulls."""
        # Test employee data
        emp_data = [("Alice", "Engineering", 85000, 28)]
        df = spark.createDataFrame(emp_data, ["name", "department", "salary", "age"])

        null_counts = (
            df.select([col(c).isNull().cast("int").alias(c) for c in df.columns])
            .agg(*[spark_sum(c).alias(c) for c in df.columns])
            .collect()[0]
        )

        # No nulls expected in this test data
        assert all(count == 0 for count in null_counts.asDict().values())

    def test_data_type_consistency(self, spark):
        """Test that data types are consistent across operations."""
        # Create DataFrame with explicit schema
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("value", DoubleType(), False),
            ]
        )

        data = [(1, 100.5), (2, 200.7)]
        df = spark.createDataFrame(data, schema)

        # Verify types are maintained
        assert df.schema.fields[0].dataType == IntegerType()
        assert df.schema.fields[1].dataType == DoubleType()


class TestPerformanceAndOptimization:
    """Test performance characteristics and optimizations."""

    @pytest.mark.slow
    def test_caching_performance(self, spark):
        """Test that caching improves performance for repeated operations."""
        # Generate larger dataset
        data = [(i, i * 2, i * 3) for i in range(1000)]

        result = exercise_7d_caching_comparison(spark, data)

        assert "rdd_no_cache" in result
        assert "rdd_with_cache" in result
        assert "df_no_cache" in result
        assert "df_with_cache" in result

        # All operations should complete successfully
        assert all(time > 0 for time in result.values())

    def test_execution_plans(self, spark):
        """Test that execution plans are generated correctly."""
        data = [("Alice", 28, "Engineering", 85000)]
        result = exercise_7e_explain_plans(spark, data)

        assert "simple_filter_plan" in result
        assert "complex_aggregation_plan" in result
        assert "join_plan" in result

        # Plans should contain expected Spark SQL elements
        for plan in result.values():
            assert len(plan) > 0
            assert "==" in plan  # Spark plan formatting


@pytest.mark.integration
class TestCompleteWorkflows:
    """Test complete end-to-end workflows."""

    def test_etl_pipeline(self, spark):
        """Test a complete ETL pipeline using multiple exercises."""
        # Extract: Create raw data
        raw_data = [
            ("Alice Johnson", "alice@company.com", "Engineering", "85000"),
            ("Bob Smith", "bob@email.org", "Marketing", "65000"),
        ]

        # Transform: Clean and process data
        df = spark.createDataFrame(
            raw_data, ["name", "email", "department", "salary_str"]
        )

        # Clean data types
        df_clean = (
            df.withColumn("salary", col("salary_str").cast(IntegerType()))
            .withColumn("email_domain", regexp_extract(col("email"), r"@(.+)", 1))
            .drop("salary_str")
        )

        # Aggregate by department
        dept_stats = df_clean.groupBy("department").agg(
            avg("salary").alias("avg_salary"), count("*").alias("employee_count")
        )

        # Load: Verify results
        results = dept_stats.collect()
        assert len(results) == 2  # Two departments

        engineering = next(r for r in results if r["department"] == "Engineering")
        assert engineering["avg_salary"] == 85000
        assert engineering["employee_count"] == 1

    def test_data_quality_validation(self, spark):
        """Test data quality validation across exercises."""
        # Test data with potential quality issues
        test_data = [
            ("Alice", 28, "Engineering", 85000),
            ("", 30, "Marketing", 65000),  # Empty name
            ("Charlie", -5, "Sales", 70000),  # Invalid age
            ("Diana", 31, "", 72000),  # Empty department
            ("Eve", 26, "Engineering", 0),  # Zero salary
        ]

        df = spark.createDataFrame(test_data, ["name", "age", "department", "salary"])

        # Data quality checks
        quality_report = df.select(
            [
                count("*").alias("total_records"),
                spark_sum(when(col("name") == "", 1).otherwise(0)).alias("empty_names"),
                spark_sum(when(col("age") < 0, 1).otherwise(0)).alias("invalid_ages"),
                spark_sum(when(col("department") == "", 1).otherwise(0)).alias(
                    "empty_departments"
                ),
                spark_sum(when(col("salary") <= 0, 1).otherwise(0)).alias(
                    "invalid_salaries"
                ),
            ]
        ).collect()[0]

        # Verify quality issues are detected
        assert quality_report["total_records"] == 5
        assert quality_report["empty_names"] == 1
        assert quality_report["invalid_ages"] == 1
        assert quality_report["empty_departments"] == 1
        assert quality_report["invalid_salaries"] == 1
