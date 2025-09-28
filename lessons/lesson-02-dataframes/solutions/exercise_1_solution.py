"""
Exercise 1 Solutions: DataFrame Creation and Basic Operations
============================================================

This file contains the complete solutions for Exercise 1.
Study these solutions to understand DataFrame creation and basic operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, length, when, count, avg, sum as spark_sum, max as spark_max, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import List, Dict, Any
import os


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise1-Solutions") \
        .master("local[*]") \
        .getOrCreate()


def exercise_1a(spark: SparkSession, data: List[tuple], columns: List[str]):
    """Create a DataFrame from a list of tuples and column names."""
    # Create DataFrame using spark.createDataFrame()
    df = spark.createDataFrame(data, columns)
    return df


def exercise_1b(spark: SparkSession, dict_data: List[Dict[str, Any]]):
    """Create a DataFrame from a list of dictionaries and display its schema."""
    # Create DataFrame from dict_data
    df = spark.createDataFrame(dict_data)

    # Get schema as string
    schema_string = df.schema.simpleString()

    return (df, schema_string)


def exercise_1c(spark: SparkSession, data: List[tuple]):
    """Create a DataFrame with an explicit schema."""
    # Define explicit schema
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True)
    ])

    # Create DataFrame with the schema
    df = spark.createDataFrame(data, schema)

    return df


def exercise_1d(spark: SparkSession, df):
    """Perform basic DataFrame operations: select and filter."""
    # Filter for Engineering department, select name and salary, order by salary desc
    result = df.filter(col("department") == "Engineering") \
               .select("name", "salary") \
               .orderBy(col("salary").desc())

    return result


def exercise_1e(spark: SparkSession, df):
    """Add calculated columns to a DataFrame."""
    # Add calculated columns
    result = df.withColumn("annual_salary", col("salary") * 12) \
               .withColumn("age_category",
                          when(col("age") < 30, "Young")
                          .otherwise("Senior")) \
               .withColumn("name_length", length(col("name")))

    return result


def exercise_1f(spark: SparkSession, df):
    """Perform aggregations on the DataFrame."""
    # Group by department and calculate aggregations
    result = df.groupBy("department") \
               .agg(
                   count("*").alias("employee_count"),
                   avg("salary").alias("avg_salary"),
                   spark_max("salary").alias("max_salary"),
                   spark_sum("salary").alias("total_payroll")
               ) \
               .orderBy(col("avg_salary").desc())

    return result


def run_solutions():
    \"\"\"Run all solutions and display results.\"\"\"
    spark = setup_spark()

    print(\"✅ Exercise 1 Solutions\")
    print(\"=\" * 30)

    # Solution 1a
    print(\"\\n1a. Create DataFrame from Tuples\")
    data = [(\"Alice\", 25), (\"Bob\", 30), (\"Charlie\", 35)]
    columns = [\"name\", \"age\"]
    result_1a = exercise_1a(spark, data, columns)
    result_1a.show()

    # Solution 1b
    print(\"\\n1b. Create DataFrame from Dictionaries\")
    dict_data = [
        {\"name\": \"Alice\", \"age\": 25, \"salary\": 50000},
        {\"name\": \"Bob\", \"age\": 30, \"salary\": 60000}
    ]
    df, schema_str = exercise_1b(spark, dict_data)
    df.show()
    print(f\"Schema: {schema_str}\")

    # Solution 1c
    print(\"\\n1c. Create DataFrame with Explicit Schema\")
    data = [(\"Alice\", 25, 80000.0), (\"Bob\", 30, 75000.0)]
    result_1c = exercise_1c(spark, data)
    result_1c.show()
    result_1c.printSchema()

    # Solution 1d
    print(\"\\n1d. Filter and Select Operations\")
    test_data = [
        (\"Alice\", 25, \"Engineering\", 80000),
        (\"Bob\", 30, \"Marketing\", 75000),
        (\"Charlie\", 35, \"Engineering\", 85000),
        (\"Diana\", 28, \"Sales\", 70000)
    ]
    test_df = spark.createDataFrame(test_data, [\"name\", \"age\", \"department\", \"salary\"])
    result_1d = exercise_1d(spark, test_df)
    result_1d.show()

    # Solution 1e
    print(\"\\n1e. Add Calculated Columns\")
    test_data = [(\"Alice\", 25, 80000), (\"Bob\", 35, 75000)]
    test_df = spark.createDataFrame(test_data, [\"name\", \"age\", \"salary\"])
    result_1e = exercise_1e(spark, test_df)
    result_1e.show()

    # Solution 1f
    print(\"\\n1f. Group By Aggregations\")
    test_data = [
        (\"Engineering\", 80000),
        (\"Engineering\", 85000),
        (\"Marketing\", 75000),
        (\"Sales\", 70000),
        (\"Sales\", 72000)
    ]
    test_df = spark.createDataFrame(test_data, [\"department\", \"salary\"])
    result_1f = exercise_1f(spark, test_df)
    result_1f.show()

    spark.stop()
    print(\"\\n🎓 All solutions demonstrated successfully!\")


if __name__ == \"__main__\":
    run_solutions()"