"""
Exercise 5 Solutions: Column Functions and Data Transformations
==============================================================

This file contains the complete solutions for Exercise 5.
Study these solutions to understand advanced column functions and data transformations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, lower, length, substring, concat, concat_ws,
    round as spark_round, abs as spark_abs, sqrt,
    current_date, current_timestamp, date_add, year, month, dayofmonth, datediff,
    split, regexp_replace, regexp_extract, trim,
    array, create_map, explode, size,
    cast, to_date, date_format, avg, sum as spark_sum, isnotnull
)
from pyspark.sql.types import IntegerType, DoubleType, DateType
from typing import List, Tuple


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise5-Solutions") \
        .master("local[*]") \
        .getOrCreate()


def exercise_5a(spark: SparkSession, people_data: List[tuple]):
    """Apply string functions to transform and analyze text data."""
    # Create DataFrame
    df = spark.createDataFrame(people_data, ["full_name", "email", "phone"])

    # Apply string transformations
    result = df.withColumn("first_name", split(col("full_name"), " ").getItem(0)) \
               .withColumn("last_name", split(col("full_name"), " ").getItem(1)) \
               .withColumn("name_length", length(col("full_name"))) \
               .withColumn("email_domain", regexp_extract(col("email"), r"@(.+)", 1)) \
               .withColumn("phone_clean", regexp_replace(col("phone"), r"[^\d]", ""))

    return result


def exercise_5b(spark: SparkSession, sales_data: List[tuple]):
    """Apply numeric functions and calculations."""
    # Create DataFrame
    df = spark.createDataFrame(sales_data, ["product", "quantity", "unit_price", "discount_rate"])

    # Apply numeric calculations
    result = df.withColumn("gross_amount", col("quantity") * col("unit_price")) \
               .withColumn("discount_amount",
                          spark_round(col("quantity") * col("unit_price") * col("discount_rate"), 2)) \
               .withColumn("net_amount",
                          spark_round(col("quantity") * col("unit_price") * (1 - col("discount_rate")), 2)) \
               .withColumn("price_category",
                          when(col("unit_price") > 100, "Premium")
                          .when(col("unit_price") > 50, "Standard")
                          .otherwise("Budget")) \
               .withColumn("quantity_group",
                          when(col("quantity") >= 10, "High")
                          .when(col("quantity") >= 5, "Medium")
                          .otherwise("Low"))

    return result


def exercise_5c(spark: SparkSession, events_data: List[tuple]):
    """Work with date functions and transformations."""
    # Create DataFrame
    df = spark.createDataFrame(events_data, ["event_name", "event_date_str", "duration_days"])

    # Apply date transformations
    result = df.withColumn("event_date", to_date(col("event_date_str"), "yyyy-MM-dd")) \
               .withColumn("event_year", year(col("event_date"))) \
               .withColumn("event_month", month(col("event_date"))) \
               .withColumn("event_day", dayofmonth(col("event_date"))) \
               .withColumn("end_date", date_add(col("event_date"), col("duration_days"))) \
               .withColumn("days_until_event", datediff(col("event_date"), current_date())) \
               .withColumn("is_future_event", col("event_date") > current_date())

    return result


def exercise_5d(spark: SparkSession, products_data: List[tuple]):
    """Work with arrays and complex data transformations."""
    # Create DataFrame
    df = spark.createDataFrame(products_data, ["product_name", "tags_str", "ratings_str"])

    # Array and complex data transformations
    result = df.withColumn("tags_array", split(col("tags_str"), ",")) \
               .withColumn("ratings_array", split(col("ratings_str"), ",").cast("array<double>")) \
               .withColumn("tag_count", size(split(col("tags_str"), ","))) \
               .withColumn("avg_rating",
                          spark_round(
                              expr("aggregate(split(ratings_str, ','), 0.0, (acc, x) -> acc + cast(x as double)) / size(split(ratings_str, ','))"),
                              1
                          )) \
               .withColumn("top_tag", split(col("tags_str"), ",").getItem(0)) \
               .withColumn("has_mobile_tag", array_contains(split(col("tags_str"), ","), "mobile"))

    # Alternative simpler approach for avg_rating using expr
    from pyspark.sql.functions import expr, array_contains

    result = df.withColumn("tags_array", split(col("tags_str"), ",")) \
               .withColumn("ratings_array",
                          expr("transform(split(ratings_str, ','), x -> cast(x as double))")) \
               .withColumn("tag_count", size(split(col("tags_str"), ","))) \
               .withColumn("avg_rating",
                          spark_round(expr("aggregate(transform(split(ratings_str, ','), x -> cast(x as double)), 0.0, (acc, x) -> acc + x) / size(split(ratings_str, ','))"), 1)) \
               .withColumn("top_tag", split(col("tags_str"), ",").getItem(0)) \
               .withColumn("has_mobile_tag", array_contains(split(col("tags_str"), ","), "mobile"))

    return result


def exercise_5e(spark: SparkSession, customers_data: List[tuple]):
    """Apply conditional logic and data categorization."""
    # Create DataFrame
    df = spark.createDataFrame(customers_data, ["customer_id", "age", "income", "purchase_count", "last_purchase_days"])

    # Apply conditional logic and categorization
    result = df.withColumn("age_group",
                          when(col("age") < 30, "Young")
                          .when(col("age") <= 50, "Middle")
                          .otherwise("Senior")) \
               .withColumn("income_level",
                          when(col("income") > 75000, "High")
                          .when(col("income") > 45000, "Medium")
                          .otherwise("Low")) \
               .withColumn("customer_value",
                          when((col("income") > 75000) & (col("purchase_count") > 10), "High")
                          .when((col("income") > 45000) & (col("purchase_count") > 5), "Medium")
                          .otherwise("Low")) \
               .withColumn("retention_risk",
                          when(col("last_purchase_days") > 90, "High")
                          .when(col("last_purchase_days") > 30, "Medium")
                          .otherwise("Low")) \
               .withColumn("marketing_segment",
                          concat(
                              when(col("age") < 30, "Young")
                              .when(col("age") <= 50, "Middle")
                              .otherwise("Senior"),
                              lit("_"),
                              when(col("income") > 75000, "High")
                              .when(col("income") > 45000, "Medium")
                              .otherwise("Low")
                          ))

    return result


def exercise_5f(spark: SparkSession, orders_data: List[tuple]):
    """Combine multiple transformations in a data processing pipeline."""
    # Create DataFrame
    df = spark.createDataFrame(orders_data, ["order_id", "customer_email", "order_total_str", "order_date_str", "status"])

    # Complete data processing pipeline
    result = df.withColumn("customer_domain", regexp_extract(col("customer_email"), r"@(.+)", 1)) \
               .withColumn("order_total", col("order_total_str").cast(DoubleType())) \
               .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd")) \
               .withColumn("order_month_year", date_format(col("order_date"), "yyyy-MM")) \
               .withColumn("total_category",
                          when(col("order_total") > 500, "Large")
                          .when(col("order_total") > 100, "Medium")
                          .otherwise("Small")) \
               .withColumn("status_clean", upper(trim(col("status")))) \
               .withColumn("is_completed", col("status_clean") == "COMPLETED") \
               .withColumn("order_age_days", datediff(current_date(), col("order_date"))) \
               .filter((col("order_total") > 0) & isnotnull(col("customer_email"))) \
               .select("order_id", "customer_domain", "order_total", "order_date",
                      "order_month_year", "total_category", "status_clean",
                      "is_completed", "order_age_days")

    return result


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 5 Solutions")
    print("=" * 30)

    # Solution 5a
    print("\n5a. String Functions")
    people_data = [
        ("John Smith", "john.smith@email.com", "(555) 123-4567"),
        ("Jane Doe", "jane.doe@company.org", "555.987.6543"),
        ("Bob Johnson", "bob@test.net", "555-111-2222")
    ]
    result_5a = exercise_5a(spark, people_data)
    result_5a.show(truncate=False)

    # Solution 5b
    print("\n5b. Numeric Functions")
    sales_data = [
        ("Laptop", 2, 999.99, 0.1),
        ("Mouse", 5, 25.50, 0.05),
        ("Keyboard", 12, 75.00, 0.15),
        ("Monitor", 1, 299.99, 0.08)
    ]
    result_5b = exercise_5b(spark, sales_data)
    result_5b.show()

    # Solution 5c
    print("\n5c. Date Functions")
    events_data = [
        ("Conference", "2024-06-15", 3),
        ("Workshop", "2023-12-01", 1),
        ("Meeting", "2024-01-30", 0)
    ]
    result_5c = exercise_5c(spark, events_data)
    result_5c.show()

    # Solution 5d
    print("\n5d. Arrays and Complex Data")
    products_data = [
        ("iPhone", "electronics,mobile,smartphone", "4.5,4.2,4.8"),
        ("Book", "education,reading", "4.0,4.1"),
        ("Laptop", "electronics,computer,mobile", "4.3,4.6,4.4,4.5")
    ]
    result_5d = exercise_5d(spark, products_data)
    result_5d.show(truncate=False)

    # Solution 5e
    print("\n5e. Conditional Logic and Categorization")
    customers_data = [
        (1, 25, 85000, 15, 20),
        (2, 45, 55000, 8, 45),
        (3, 60, 35000, 3, 120),
        (4, 35, 95000, 25, 10)
    ]
    result_5e = exercise_5e(spark, customers_data)
    result_5e.show()

    # Solution 5f
    print("\n5f. Complete Data Processing Pipeline")
    orders_data = [
        ("ORD001", "alice@company.com", "250.50", "2023-11-15", " completed "),
        ("ORD002", "bob@email.org", "750.00", "2023-12-01", "PENDING"),
        ("ORD003", None, "0.00", "2023-10-20", "cancelled"),
        ("ORD004", "charlie@test.net", "125.75", "2023-11-30", "COMPLETED")
    ]
    result_5f = exercise_5f(spark, orders_data)
    result_5f.show()
    print(f"Filtered rows: {result_5f.count()} (should exclude invalid orders)")

    spark.stop()
    print("\n🎓 All Exercise 5 solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()