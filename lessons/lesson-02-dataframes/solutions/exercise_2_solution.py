"""
Exercise 2 Solutions: DataFrame Transformations and Column Functions
===================================================================

This file contains the complete solutions for Exercise 2.
Study these solutions to understand DataFrame transformations and column functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, lower, length, when, concat, lit, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import List, Dict, Any


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise2-Solutions") \
        .master("local[*]") \
        .getOrCreate()


def exercise_2a(spark: SparkSession, employee_data: List[tuple]):
    """Add new columns with various transformations."""
    # Create DataFrame
    df = spark.createDataFrame(employee_data, ["name", "age", "department", "salary"])

    # Add transformed columns
    result = df.withColumn("name_upper", upper(col("name"))) \
               .withColumn("name_length", length(col("name"))) \
               .withColumn("salary_bonus", col("salary") * 0.1) \
               .withColumn("total_compensation", col("salary") + col("salary_bonus"))

    return result


def exercise_2b(spark: SparkSession, product_data: List[tuple]):
    """Apply conditional transformations using when()."""
    # Create DataFrame
    df = spark.createDataFrame(product_data, ["product_name", "category", "price", "stock"])

    # Add conditional columns
    result = df.withColumn("price_category",
                          when(col("price") > 500, "Premium")
                          .when(col("price") > 100, "Standard")
                          .otherwise("Budget")) \
               .withColumn("stock_status",
                          when(col("stock") == 0, "Out of Stock")
                          .when(col("stock") < 10, "Low Stock")
                          .otherwise("In Stock")) \
               .withColumn("discount_eligible",
                          when((col("price") > 200) & (col("stock") > 50), True)
                          .otherwise(False))

    return result


def exercise_2c(spark: SparkSession, sales_data: List[tuple]):
    """Combine multiple column operations in a single chain."""
    # Create DataFrame
    df = spark.createDataFrame(sales_data, ["product", "quantity", "unit_price", "discount"])

    # Chain multiple transformations
    result = df.withColumn("gross_total", col("quantity") * col("unit_price")) \
               .withColumn("discount_amount", col("gross_total") * col("discount")) \
               .withColumn("net_total", col("gross_total") - col("discount_amount")) \
               .withColumn("per_unit_net", spark_round(col("net_total") / col("quantity"), 2)) \
               .withColumn("volume_category",
                          when(col("quantity") >= 100, "Bulk")
                          .when(col("quantity") >= 10, "Medium")
                          .otherwise("Small"))

    return result


def exercise_2d(spark: SparkSession, customer_data: List[tuple]):
    """Transform and clean data columns."""
    # Create DataFrame
    df = spark.createDataFrame(customer_data, ["customer_id", "name", "email", "phone", "age"])

    # Transform and clean data
    result = df.withColumn("name_clean", upper(col("name"))) \
               .withColumn("email_domain",
                          when(col("email").contains("@"),
                               col("email").substr(col("email").rindex("@") + 2, 100))
                          .otherwise("unknown")) \
               .withColumn("age_group",
                          when(col("age") < 25, "Young")
                          .when(col("age") < 45, "Adult")
                          .when(col("age") < 65, "Middle-aged")
                          .otherwise("Senior")) \
               .withColumn("contact_preference",
                          when(col("email").isNotNull() & col("phone").isNotNull(), "Both")
                          .when(col("email").isNotNull(), "Email")
                          .when(col("phone").isNotNull(), "Phone")
                          .otherwise("None"))

    return result


def exercise_2e(spark: SparkSession, order_data: List[tuple]):
    """Complex transformations with multiple conditions."""
    # Create DataFrame
    df = spark.createDataFrame(order_data, ["order_id", "customer_id", "total_amount", "status", "region"])

    # Complex transformations
    result = df.withColumn("order_size",
                          when(col("total_amount") > 1000, "Large")
                          .when(col("total_amount") > 250, "Medium")
                          .otherwise("Small")) \
               .withColumn("priority",
                          when((col("total_amount") > 500) & (col("region") == "Premium"), "High")
                          .when(col("total_amount") > 500, "Medium")
                          .when(col("region") == "Premium", "Medium")
                          .otherwise("Low")) \
               .withColumn("processing_fee",
                          when(col("total_amount") > 1000, 0.0)
                          .when(col("total_amount") > 250, 5.0)
                          .otherwise(10.0)) \
               .withColumn("final_amount", col("total_amount") + col("processing_fee")) \
               .withColumn("status_category",
                          when(col("status").isin("Completed", "Shipped", "Delivered"), "Fulfilled")
                          .when(col("status").isin("Processing", "Pending"), "In Progress")
                          .otherwise("Issues"))

    return result


def exercise_2f(spark: SparkSession, inventory_data: List[tuple]):
    """Advanced column manipulations and calculations."""
    # Create DataFrame
    df = spark.createDataFrame(inventory_data, ["item_code", "item_name", "cost", "selling_price", "quantity"])

    # Advanced calculations
    result = df.withColumn("profit_per_unit", col("selling_price") - col("cost")) \
               .withColumn("profit_margin",
                          spark_round((col("selling_price") - col("cost")) / col("selling_price") * 100, 2)) \
               .withColumn("total_cost_value", col("cost") * col("quantity")) \
               .withColumn("total_selling_value", col("selling_price") * col("quantity")) \
               .withColumn("total_profit_potential", col("profit_per_unit") * col("quantity")) \
               .withColumn("profitability",
                          when(col("profit_margin") > 50, "High")
                          .when(col("profit_margin") > 25, "Medium")
                          .when(col("profit_margin") > 0, "Low")
                          .otherwise("Loss")) \
               .withColumn("inventory_value_category",
                          when(col("total_selling_value") > 10000, "High Value")
                          .when(col("total_selling_value") > 1000, "Medium Value")
                          .otherwise("Low Value"))

    return result


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 2 Solutions")
    print("=" * 30)

    # Solution 2a
    print("\n2a. Add New Columns with Transformations")
    employee_data = [
        ("Alice Johnson", 28, "Engineering", 85000),
        ("Bob Smith", 34, "Marketing", 65000),
        ("Charlie Brown", 29, "Engineering", 78000)
    ]
    result_2a = exercise_2a(spark, employee_data)
    result_2a.show(truncate=False)

    # Solution 2b
    print("\n2b. Conditional Transformations")
    product_data = [
        ("Laptop Pro", "Electronics", 1200.00, 25),
        ("Coffee Mug", "Kitchen", 15.99, 0),
        ("Smartphone", "Electronics", 699.99, 75),
        ("Desk Chair", "Furniture", 89.99, 5)
    ]
    result_2b = exercise_2b(spark, product_data)
    result_2b.show(truncate=False)

    # Solution 2c
    print("\n2c. Chained Column Operations")
    sales_data = [
        ("Widget A", 50, 10.0, 0.1),
        ("Widget B", 150, 25.0, 0.15),
        ("Widget C", 5, 100.0, 0.05)
    ]
    result_2c = exercise_2c(spark, sales_data)
    result_2c.show()

    # Solution 2d
    print("\n2d. Data Transformation and Cleaning")
    customer_data = [
        (1, "john doe", "john@email.com", "555-1234", 32),
        (2, "jane smith", "jane@company.org", None, 28),
        (3, "bob wilson", None, "555-5678", 45)
    ]
    result_2d = exercise_2d(spark, customer_data)
    result_2d.show(truncate=False)

    # Solution 2e
    print("\n2e. Complex Multi-condition Transformations")
    order_data = [
        ("ORD001", 101, 1250.00, "Completed", "Premium"),
        ("ORD002", 102, 75.00, "Pending", "Standard"),
        ("ORD003", 103, 800.00, "Shipped", "Standard"),
        ("ORD004", 104, 2000.00, "Processing", "Premium")
    ]
    result_2e = exercise_2e(spark, order_data)
    result_2e.show()

    # Solution 2f
    print("\n2f. Advanced Column Manipulations")
    inventory_data = [
        ("ITM001", "Gaming Laptop", 800.0, 1200.0, 10),
        ("ITM002", "Office Chair", 50.0, 120.0, 25),
        ("ITM003", "Coffee Maker", 30.0, 80.0, 15),
        ("ITM004", "Monitor", 200.0, 350.0, 8)
    ]
    result_2f = exercise_2f(spark, inventory_data)
    result_2f.show()

    spark.stop()
    print("\n🎓 All Exercise 2 solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()