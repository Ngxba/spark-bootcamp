"""
Exercise 4 Solutions: Joins and Data Combining
==============================================

This file contains the complete solutions for Exercise 4.
Study these solutions to understand different types of joins and data combining operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when, count, avg, sum as spark_sum
from typing import List


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return (
        SparkSession.builder.appName("Exercise4-Solutions")
        .master("local[*]")
        .getOrCreate()
    )


def exercise_4a(
    spark: SparkSession, employees_data: List[tuple], departments_data: List[tuple]
):
    """Perform inner join between employees and departments."""
    # Create DataFrames
    employees_df = spark.createDataFrame(
        employees_data, ["emp_id", "name", "dept_id", "salary"]
    )
    departments_df = spark.createDataFrame(
        departments_data, ["dept_id", "dept_name", "location"]
    )

    # Inner join
    result = (
        employees_df.join(departments_df, "dept_id", "inner")
        .select("emp_id", "name", "dept_name", "location", "salary")
        .orderBy("emp_id")
    )

    return result


def exercise_4b(
    spark: SparkSession, customers_data: List[tuple], orders_data: List[tuple]
):
    """Perform left join to include all customers, even without orders."""
    # Create DataFrames
    customers_df = spark.createDataFrame(
        customers_data, ["customer_id", "customer_name", "email"]
    )
    orders_df = spark.createDataFrame(
        orders_data, ["order_id", "customer_id", "order_amount", "order_date"]
    )

    # Left join with aggregation
    result = (
        customers_df.join(orders_df, "customer_id", "left")
        .groupBy("customer_id", "customer_name", "email")
        .agg(
            count(orders_df.order_id).alias("order_count"),
            coalesce(spark_sum(orders_df.order_amount), lit(0.0)).alias("total_spent"),
        )
        .orderBy("customer_id")
    )

    return result


def exercise_4c(
    spark: SparkSession, products_data: List[tuple], inventory_data: List[tuple]
):
    """Perform right join to show all inventory items."""
    # Create DataFrames
    products_df = spark.createDataFrame(
        products_data, ["product_id", "product_name", "category"]
    )
    inventory_df = spark.createDataFrame(
        inventory_data, ["product_id", "warehouse", "quantity", "last_updated"]
    )

    # Right join
    result = (
        products_df.join(inventory_df, "product_id", "right")
        .select(
            inventory_df.product_id,
            coalesce(products_df.product_name, lit("Unknown Product")).alias(
                "product_name"
            ),
            coalesce(products_df.category, lit("Unknown")).alias("category"),
            "warehouse",
            "quantity",
            "last_updated",
        )
        .orderBy("product_id", "warehouse")
    )

    return result


def exercise_4d(
    spark: SparkSession, online_sales: List[tuple], store_sales: List[tuple]
):
    """Perform full outer join to combine online and store sales."""
    # Create DataFrames
    online_df = spark.createDataFrame(
        online_sales, ["product_id", "online_sales", "online_units"]
    )
    store_df = spark.createDataFrame(
        store_sales, ["product_id", "store_sales", "store_units"]
    )

    # Full outer join
    result = (
        online_df.join(store_df, "product_id", "full_outer")
        .select(
            coalesce(online_df.product_id, store_df.product_id).alias("product_id"),
            coalesce(online_df.online_sales, lit(0.0)).alias("online_sales"),
            coalesce(online_df.online_units, lit(0)).alias("online_units"),
            coalesce(store_df.store_sales, lit(0.0)).alias("store_sales"),
            coalesce(store_df.store_units, lit(0)).alias("store_units"),
        )
        .withColumn("total_sales", col("online_sales") + col("store_sales"))
        .withColumn("total_units", col("online_units") + col("store_units"))
        .withColumn(
            "sales_channel",
            when((col("online_sales") > 0) & (col("store_sales") > 0), "Both")
            .when(col("online_sales") > 0, "Online Only")
            .when(col("store_sales") > 0, "Store Only")
            .otherwise("No Sales"),
        )
        .orderBy("product_id")
    )

    return result


def exercise_4e(
    spark: SparkSession,
    users_data: List[tuple],
    profiles_data: List[tuple],
    activities_data: List[tuple],
):
    """Perform multiple joins across three tables."""
    # Create DataFrames
    users_df = spark.createDataFrame(users_data, ["user_id", "username", "email"])
    profiles_df = spark.createDataFrame(
        profiles_data, ["user_id", "age", "location", "subscription_type"]
    )
    activities_df = spark.createDataFrame(
        activities_data, ["user_id", "activity_date", "activity_type", "duration"]
    )

    # Multiple joins
    result = (
        users_df.join(profiles_df, "user_id", "left")
        .join(
            activities_df.groupBy("user_id").agg(
                count("*").alias("activity_count"),
                spark_sum("duration").alias("total_duration"),
                avg("duration").alias("avg_duration"),
            ),
            "user_id",
            "left",
        )
        .select(
            "user_id",
            "username",
            "email",
            coalesce("age", lit(0)).alias("age"),
            coalesce("location", lit("Unknown")).alias("location"),
            coalesce("subscription_type", lit("Free")).alias("subscription_type"),
            coalesce("activity_count", lit(0)).alias("activity_count"),
            coalesce("total_duration", lit(0.0)).alias("total_duration"),
            round(coalesce("avg_duration", lit(0.0)), 2).alias("avg_duration"),
        )
        .withColumn(
            "user_engagement",
            when(col("activity_count") > 10, "High")
            .when(col("activity_count") > 5, "Medium")
            .when(col("activity_count") > 0, "Low")
            .otherwise("Inactive"),
        )
        .orderBy("user_id")
    )

    return result


def exercise_4f(spark: SparkSession, current_data: List[tuple], new_data: List[tuple]):
    """Implement upsert logic using joins."""
    # Create DataFrames
    current_df = spark.createDataFrame(
        current_data, ["id", "name", "value", "last_updated"]
    )
    new_df = spark.createDataFrame(new_data, ["id", "name", "value", "last_updated"])

    # Upsert logic: update existing records, insert new ones
    # First, find records that need updates (existing IDs in both datasets)
    updates = current_df.join(new_df, "id", "inner").select(
        new_df.id, new_df.name, new_df.value, new_df.last_updated
    )

    # Find records to insert (IDs in new_df but not in current_df)
    inserts = new_df.join(current_df, "id", "left_anti")

    # Find records to keep unchanged (IDs in current_df but not in new_df)
    unchanged = current_df.join(new_df, "id", "left_anti")

    # Union all parts for final result
    result = updates.union(inserts).union(unchanged).orderBy("id")

    return result, updates, inserts, unchanged


def run_solutions():
    """Run all solutions and display results."""
    spark = setup_spark()

    print("✅ Exercise 4 Solutions")
    print("=" * 30)

    # Solution 4a
    print("\n4a. Inner Join - Employees and Departments")
    employees_data = [
        (1, "Alice", 10, 85000),
        (2, "Bob", 20, 65000),
        (3, "Charlie", 10, 78000),
        (4, "Diana", 30, 72000),
        (5, "Eve", 10, 92000),
    ]
    departments_data = [
        (10, "Engineering", "Building A"),
        (20, "Marketing", "Building B"),
        (30, "Sales", "Building C"),
    ]
    result_4a = exercise_4a(spark, employees_data, departments_data)
    result_4a.show()

    # Solution 4b
    print("\n4b. Left Join - Customers and Orders")
    customers_data = [
        (1, "Alice Johnson", "alice@email.com"),
        (2, "Bob Smith", "bob@email.com"),
        (3, "Charlie Brown", "charlie@email.com"),
        (4, "Diana Wilson", "diana@email.com"),
    ]
    orders_data = [
        (101, 1, 250.0, "2023-01-15"),
        (102, 1, 150.0, "2023-01-20"),
        (103, 2, 300.0, "2023-01-18"),
        (104, 3, 75.0, "2023-01-22"),
        # Note: Customer 4 (Diana) has no orders
    ]
    result_4b = exercise_4b(spark, customers_data, orders_data)
    result_4b.show()

    # Solution 4c
    print("\n4c. Right Join - Products and Inventory")
    products_data = [
        ("PROD001", "Laptop", "Electronics"),
        ("PROD002", "Mouse", "Electronics"),
        ("PROD003", "Chair", "Furniture"),
        # Note: PROD004 in inventory but not in products
    ]
    inventory_data = [
        ("PROD001", "Warehouse A", 50, "2023-01-15"),
        ("PROD001", "Warehouse B", 30, "2023-01-15"),
        ("PROD002", "Warehouse A", 100, "2023-01-16"),
        ("PROD004", "Warehouse C", 25, "2023-01-17"),  # Unknown product
    ]
    result_4c = exercise_4c(spark, products_data, inventory_data)
    result_4c.show()

    # Solution 4d
    print("\n4d. Full Outer Join - Online vs Store Sales")
    online_sales = [
        ("PROD001", 5000.0, 50),
        ("PROD002", 1200.0, 60),
        ("PROD003", 800.0, 20),
        # PROD004 only in store sales
    ]
    store_sales = [
        ("PROD001", 3000.0, 30),
        ("PROD003", 1500.0, 25),
        ("PROD004", 2200.0, 40),
        # PROD002 only in online sales
    ]
    result_4d = exercise_4d(spark, online_sales, store_sales)
    result_4d.show()

    # Solution 4e
    print("\n4e. Multiple Joins - Users, Profiles, and Activities")
    users_data = [
        (1, "alice123", "alice@email.com"),
        (2, "bob456", "bob@email.com"),
        (3, "charlie789", "charlie@email.com"),
        (4, "diana101", "diana@email.com"),
    ]
    profiles_data = [
        (1, 25, "New York", "Premium"),
        (2, 30, "Los Angeles", "Basic"),
        (3, 35, "Chicago", "Premium"),
        # User 4 has no profile
    ]
    activities_data = [
        (1, "2023-01-15", "Login", 30.0),
        (1, "2023-01-16", "View", 45.0),
        (1, "2023-01-17", "Purchase", 120.0),
        (2, "2023-01-15", "Login", 25.0),
        (2, "2023-01-16", "View", 60.0),
        (3, "2023-01-15", "Login", 20.0),
        # User 4 has no activities
    ]
    result_4e = exercise_4e(spark, users_data, profiles_data, activities_data)
    result_4e.show()

    # Solution 4f
    print("\n4f. Upsert Logic using Joins")
    current_data = [
        (1, "Item A", 100, "2023-01-10"),
        (2, "Item B", 200, "2023-01-11"),
        (3, "Item C", 150, "2023-01-12"),
    ]
    new_data = [
        (2, "Item B Updated", 250, "2023-01-20"),  # Update existing
        (3, "Item C Updated", 175, "2023-01-21"),  # Update existing
        (4, "Item D", 300, "2023-01-22"),  # Insert new
        (5, "Item E", 400, "2023-01-23"),  # Insert new
    ]
    result_4f, updates, inserts, unchanged = exercise_4f(spark, current_data, new_data)

    print("Final upserted data:")
    result_4f.show()
    print(
        f"Updates: {updates.count()}, Inserts: {inserts.count()}, Unchanged: {unchanged.count()}"
    )

    spark.stop()
    print("\n🎓 All Exercise 4 solutions demonstrated successfully!")


if __name__ == "__main__":
    run_solutions()
