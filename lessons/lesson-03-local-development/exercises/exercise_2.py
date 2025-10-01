"""
Exercise 2: Modular Refactoring
===============================

This exercise focuses on transforming a monolithic Spark script into a modular,
maintainable application. You'll practice separating concerns, creating reusable
components, and implementing proper abstraction layers.

Learning Goals:
- Refactor monolithic code into modules
- Implement separation of concerns
- Create reusable utility functions
- Design testable components

Instructions:
1. Analyze the provided monolithic code
2. Break it down into logical modules
3. Create proper abstractions
4. Implement error handling and logging

Run this file: python exercise_2.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum, max as spark_max, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime


# MONOLITHIC CODE TO REFACTOR
def monolithic_sales_analysis():
    """
    Monolithic function that does everything in one place.
    This represents typical legacy Spark code that needs refactoring.
    """
    # Spark session creation
    spark = SparkSession.builder \
        .appName("SalesAnalysis") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Read sales data
    sales_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/raw/sales.csv")

    # Read customer data
    customers_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/raw/customers.csv")

    # Data cleaning and validation
    clean_sales = sales_df \
        .filter(col("amount") > 0) \
        .filter(col("quantity") > 0) \
        .filter(col("sale_date").isNotNull()) \
        .dropDuplicates(["transaction_id"])

    clean_customers = customers_df \
        .filter(col("age") >= 18) \
        .filter(col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')) \
        .dropDuplicates(["customer_id"])

    # Join sales with customers
    sales_with_customers = clean_sales.join(
        clean_customers,
        clean_sales.customer_id == clean_customers.customer_id,
        "inner"
    ).drop(clean_customers.customer_id)

    # Add derived columns
    enriched_sales = sales_with_customers \
        .withColumn("revenue", col("amount") * col("quantity")) \
        .withColumn("customer_tier",
                   when(col("age") < 30, "Young")
                   .when(col("age") < 50, "Middle")
                   .otherwise("Senior")) \
        .withColumn("purchase_category",
                   when(col("revenue") > 1000, "High")
                   .when(col("revenue") > 500, "Medium")
                   .otherwise("Low"))

    # Calculate customer statistics
    customer_stats = enriched_sales \
        .groupBy("customer_id", "name", "customer_tier") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("revenue").alias("total_revenue"),
            avg("revenue").alias("avg_revenue"),
            max("sale_date").alias("last_purchase_date")
        ) \
        .orderBy(desc("total_revenue"))

    # Calculate product statistics
    product_stats = enriched_sales \
        .groupBy("product_category") \
        .agg(
            count("transaction_id").alias("total_sales"),
            sum("revenue").alias("total_revenue"),
            avg("revenue").alias("avg_sale_amount"),
            count("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("total_revenue"))

    # Calculate daily sales trends
    daily_trends = enriched_sales \
        .groupBy("sale_date") \
        .agg(
            count("transaction_id").alias("transactions"),
            sum("revenue").alias("daily_revenue"),
            avg("revenue").alias("avg_transaction_amount")
        ) \
        .orderBy("sale_date")

    # Write results
    customer_stats.write.mode("overwrite").parquet("data/processed/customer_stats")
    product_stats.write.mode("overwrite").parquet("data/processed/product_stats")
    daily_trends.write.mode("overwrite").parquet("data/processed/daily_trends")

    spark.stop()

    return {
        "customers": customer_stats.count(),
        "products": product_stats.count(),
        "days": daily_trends.count()
    }


def exercise_2a_create_configuration_module() -> type:
    """
    Create a configuration class to manage all settings.

    Returns:
        SalesAnalysisConfig class with proper configuration management

    The configuration should include:
    - Input/output paths
    - Spark settings
    - Business rules (min age, email regex, etc.)
    - Output formats and options
    """
    # TODO: Implement this function
    # 1. Create a dataclass for configuration
    # 2. Include all hardcoded values from the monolithic function
    # 3. Add validation methods
    # 4. Support environment-specific overrides
    pass


def exercise_2b_create_spark_utilities() -> Dict[str, callable]:
    """
    Extract Spark session creation and utilities into a separate module.

    Returns:
        Dictionary of utility functions:
        - "create_spark_session": optimized Spark session creation
        - "read_csv_with_options": standardized CSV reading
        - "write_parquet_with_options": standardized Parquet writing
    """
    # TODO: Implement this function
    # 1. Extract Spark session creation logic
    # 2. Create reusable CSV reading function
    # 3. Create reusable Parquet writing function
    # 4. Add error handling and logging
    pass


def exercise_2c_create_data_validation_module() -> Dict[str, callable]:
    """
    Create a module for data validation and cleaning functions.

    Returns:
        Dictionary of validation functions:
        - "validate_sales_data": validate and clean sales data
        - "validate_customer_data": validate and clean customer data
        - "check_data_quality": general data quality checks
    """
    # TODO: Implement this function
    # 1. Extract validation logic from monolithic function
    # 2. Create separate functions for sales and customer validation
    # 3. Add comprehensive data quality checks
    # 4. Include detailed logging of validation results
    pass


def exercise_2d_create_transformation_module() -> Dict[str, callable]:
    """
    Create a module for data transformations and enrichment.

    Returns:
        Dictionary of transformation functions:
        - "enrich_sales_data": add calculated columns
        - "categorize_customers": add customer tiers
        - "calculate_revenue_metrics": add purchase categories
    """
    # TODO: Implement this function
    # 1. Extract transformation logic into separate functions
    # 2. Make transformations configurable
    # 3. Add proper error handling
    # 4. Create reusable transformation patterns
    pass


def exercise_2e_create_analytics_module() -> Dict[str, callable]:
    """
    Create a module for analytics and aggregation functions.

    Returns:
        Dictionary of analytics functions:
        - "calculate_customer_stats": customer-level analytics
        - "calculate_product_stats": product-level analytics
        - "calculate_daily_trends": time-series analytics
    """
    # TODO: Implement this function
    # 1. Extract aggregation logic into separate functions
    # 2. Make analytics configurable and reusable
    # 3. Add proper column naming and ordering
    # 4. Include data validation for results
    pass


def exercise_2f_create_refactored_job() -> type:
    """
    Create a refactored SalesAnalysisJob class using all the modules above.

    Returns:
        SalesAnalysisJob class that orchestrates the entire pipeline

    The job should:
    1. Use the configuration module
    2. Use utility functions for I/O
    3. Apply validation functions
    4. Use transformation functions
    5. Use analytics functions
    6. Include proper error handling and logging
    """
    # TODO: Implement this function
    # 1. Create a main job class
    # 2. Integrate all the modules created above
    # 3. Add comprehensive error handling
    # 4. Add logging throughout the pipeline
    # 5. Make the job configurable and testable
    pass


def exercise_2g_create_logging_and_monitoring() -> Dict[str, callable]:
    """
    Create logging and monitoring utilities.

    Returns:
        Dictionary of monitoring functions:
        - "setup_logging": configure logging for the application
        - "log_dataframe_info": log DataFrame statistics
        - "monitor_performance": track execution time and resources
    """
    # TODO: Implement this function
    # 1. Create logging configuration function
    # 2. Create DataFrame monitoring utilities
    # 3. Add performance monitoring decorators
    # 4. Include data lineage tracking
    pass


def compare_approaches():
    """
    Compare the monolithic approach vs modular approach.
    """
    print("\n🔍 Comparing Monolithic vs Modular Approaches")
    print("=" * 60)

    print("\n❌ Monolithic Approach Issues:")
    print("  • All logic in one function (100+ lines)")
    print("  • Hardcoded values throughout")
    print("  • No separation of concerns")
    print("  • Difficult to test individual components")
    print("  • No reusability")
    print("  • Hard to maintain and extend")
    print("  • No proper error handling")
    print("  • Limited configurability")

    print("\n✅ Modular Approach Benefits:")
    print("  • Separated concerns (config, I/O, validation, etc.)")
    print("  • Reusable components")
    print("  • Easy to test individual functions")
    print("  • Configurable and flexible")
    print("  • Proper error handling and logging")
    print("  • Maintainable and extensible")
    print("  • Clear responsibility boundaries")
    print("  • Better code organization")


def run_exercises():
    """Run all exercises and display results."""
    print("🚀 Running Exercise 2: Modular Refactoring")
    print("=" * 50)

    # Show the monolithic code analysis
    print("\n📊 Analyzing Monolithic Code")
    print("Length:", len(monolithic_sales_analysis.__doc__.split('\n')))
    print("Complexity: High - everything in one function")

    # Test Exercise 2a
    print("\n📝 Exercise 2a: Create Configuration Module")
    try:
        config_class = exercise_2a_create_configuration_module()

        if config_class:
            print(f"✅ Configuration class created: {config_class.__name__}")
            # Try to instantiate
            try:
                config = config_class()
                print(f"  Attributes: {[attr for attr in dir(config) if not attr.startswith('_')]}")
            except Exception as e:
                print(f"  ⚠️  Could not instantiate: {e}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2b
    print("\n📝 Exercise 2b: Create Spark Utilities")
    try:
        utilities = exercise_2b_create_spark_utilities()

        if utilities:
            print(f"✅ {len(utilities)} utility functions created:")
            for name in utilities.keys():
                print(f"  - {name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2c
    print("\n📝 Exercise 2c: Create Data Validation Module")
    try:
        validation_funcs = exercise_2c_create_data_validation_module()

        if validation_funcs:
            print(f"✅ {len(validation_funcs)} validation functions created:")
            for name in validation_funcs.keys():
                print(f"  - {name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2d
    print("\n📝 Exercise 2d: Create Transformation Module")
    try:
        transformation_funcs = exercise_2d_create_transformation_module()

        if transformation_funcs:
            print(f"✅ {len(transformation_funcs)} transformation functions created:")
            for name in transformation_funcs.keys():
                print(f"  - {name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2e
    print("\n📝 Exercise 2e: Create Analytics Module")
    try:
        analytics_funcs = exercise_2e_create_analytics_module()

        if analytics_funcs:
            print(f"✅ {len(analytics_funcs)} analytics functions created:")
            for name in analytics_funcs.keys():
                print(f"  - {name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2f
    print("\n📝 Exercise 2f: Create Refactored Job")
    try:
        job_class = exercise_2f_create_refactored_job()

        if job_class:
            print(f"✅ Refactored job class created: {job_class.__name__}")
            methods = [m for m in dir(job_class) if not m.startswith('_') and callable(getattr(job_class, m))]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 2g
    print("\n📝 Exercise 2g: Create Logging and Monitoring")
    try:
        monitoring_funcs = exercise_2g_create_logging_and_monitoring()

        if monitoring_funcs:
            print(f"✅ {len(monitoring_funcs)} monitoring functions created:")
            for name in monitoring_funcs.keys():
                print(f"  - {name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Show comparison
    compare_approaches()

    print("\n🎉 Exercise 2 completed!")
    print("💡 Great work on modular refactoring!")
    print("🔧 You've learned to break down complex code into maintainable modules!")
    print("🚀 Ready for Exercise 3!")


if __name__ == "__main__":
    run_exercises()