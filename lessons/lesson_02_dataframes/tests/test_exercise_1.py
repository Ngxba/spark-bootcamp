"""
Test cases for Exercise 1: DataFrame Creation and Basic Operations
=================================================================

These tests validate the solutions for Exercise 1.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from solutions.exercise_1_solution import (
    exercise_1a,
    exercise_1b,
    exercise_1c,
    exercise_1d,
    exercise_1e,
    exercise_1f,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("TestExercise1").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


class TestExercise1:
    """Test Exercise 1 functions."""

    def test_exercise_1a_solution(self, spark):
        """Test DataFrame creation from tuples."""
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]

        result = exercise_1a(spark, data, columns)

        assert result is not None
        assert result.count() == 3
        assert result.columns == ["name", "age"]

        # Check data content
        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25

    def test_exercise_1b_solution(self, spark):
        """Test DataFrame creation from dictionaries."""
        dict_data = [
            {"name": "Alice", "age": 25, "salary": 50000},
            {"name": "Bob", "age": 30, "salary": 60000},
        ]

        result_df, schema_string = exercise_1b(spark, dict_data)

        assert result_df is not None
        assert schema_string is not None
        assert result_df.count() == 2
        assert "name" in result_df.columns
        assert "age" in result_df.columns
        assert "salary" in result_df.columns

    def test_exercise_1c_solution(self, spark):
        """Test DataFrame creation with explicit schema."""
        data = [("Alice", 25, 80000.0), ("Bob", 30, 75000.0)]

        result = exercise_1c(spark, data)

        assert result is not None
        assert result.count() == 2

        # Check schema types
        schema = result.schema
        name_field = next(field for field in schema.fields if field.name == "name")
        age_field = next(field for field in schema.fields if field.name == "age")
        salary_field = next(field for field in schema.fields if field.name == "salary")

        assert isinstance(name_field.dataType, StringType)
        assert isinstance(age_field.dataType, IntegerType)
        assert isinstance(salary_field.dataType, DoubleType)

    def test_exercise_1d_solution(self, spark):
        """Test filter and select operations."""
        test_data = [
            ("Alice", 25, "Engineering", 80000),
            ("Bob", 30, "Marketing", 75000),
            ("Charlie", 35, "Engineering", 85000),
            ("Diana", 28, "Sales", 70000),
        ]
        test_df = spark.createDataFrame(
            test_data, ["name", "age", "department", "salary"]
        )

        result = exercise_1d(spark, test_df)

        assert result is not None
        rows = result.collect()

        # Should only have Engineering employees
        assert len(rows) == 2
        expected_names = {"Alice", "Charlie"}
        actual_names = {row["name"] for row in rows}
        assert actual_names == expected_names

        # Should be ordered by salary descending
        assert rows[0]["salary"] >= rows[1]["salary"]

    def test_exercise_1e_solution(self, spark):
        """Test calculated columns."""
        test_data = [("Alice", 25, 80000), ("Bob", 35, 75000)]
        test_df = spark.createDataFrame(test_data, ["name", "age", "salary"])

        result = exercise_1e(spark, test_df)

        assert result is not None
        assert "annual_salary" in result.columns
        assert "age_category" in result.columns
        assert "name_length" in result.columns

        rows = result.collect()
        # Check annual salary calculation
        assert rows[0]["annual_salary"] == 80000 * 12

        # Check age category
        alice_row = next(row for row in rows if row["name"] == "Alice")
        bob_row = next(row for row in rows if row["name"] == "Bob")

        assert alice_row["age_category"] == "Young"  # 25 < 30
        assert bob_row["age_category"] == "Senior"  # 35 >= 30

    def test_exercise_1f_solution(self, spark):
        """Test aggregations."""
        test_data = [
            ("Engineering", 80000),
            ("Engineering", 85000),
            ("Marketing", 75000),
            ("Sales", 70000),
            ("Sales", 72000),
        ]
        test_df = spark.createDataFrame(test_data, ["department", "salary"])

        result = exercise_1f(spark, test_df)

        assert result is not None
        assert "employee_count" in result.columns
        assert "avg_salary" in result.columns
        assert "max_salary" in result.columns
        assert "total_payroll" in result.columns

        rows = result.collect()
        assert len(rows) == 3  # Three departments

        # Find Engineering department
        eng_row = next(row for row in rows if row["department"] == "Engineering")
        assert eng_row["employee_count"] == 2
        assert eng_row["total_payroll"] == 165000  # 80000 + 85000

    def test_data_types_and_nulls(self, spark):
        """Test handling of different data types and null values."""
        # Test with null values
        data_with_nulls = [("Alice", None), ("Bob", 30)]
        df = spark.createDataFrame(data_with_nulls, ["name", "age"])

        assert df.count() == 2
        rows = df.collect()
        assert rows[0]["age"] is None
        assert rows[1]["age"] == 30

    def test_empty_dataframe(self, spark):
        """Test behavior with empty DataFrames."""
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        empty_df = spark.createDataFrame([], schema)

        assert empty_df.count() == 0
        assert empty_df.columns == ["name", "age"]

    @pytest.mark.integration
    def test_complete_workflow(self, spark):
        """Test a complete workflow using multiple Exercise 1 functions."""
        # Create initial data
        data = [("Alice", 25, 80000), ("Bob", 30, 75000), ("Charlie", 35, 85000)]

        # Test workflow
        df = spark.createDataFrame(data, ["name", "age", "salary"])

        # Add calculated columns
        df_with_calcs = df.withColumn("annual_salary", df.salary * 12)

        # Filter and aggregate
        result = (
            df_with_calcs.filter(df_with_calcs.age >= 30)
            .groupBy()
            .agg({"annual_salary": "avg"})
        )

        assert result.count() == 1
        avg_annual = result.collect()[0]["avg(annual_salary)"]
        expected_avg = ((75000 + 85000) * 12) / 2  # Bob and Charlie
        assert abs(avg_annual - expected_avg) < 0.01
