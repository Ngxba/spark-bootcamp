"""
Exercise 5: Column Functions and Data Transformations
=====================================================

This exercise focuses on advanced column functions, data transformations,
and working with different data types in Spark DataFrames.

Instructions:
1. Complete each function according to the docstring requirements
2. Use various Spark SQL functions for data transformation
3. Practice working with different data types and complex operations

Learning Goals:
- Master Spark SQL functions (string, numeric, date, conditional)
- Transform data using column operations
- Work with arrays, maps, and complex data types
- Handle data type conversions and formatting

Run this file: python exercise_5.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, lower, length, substring, concat, concat_ws,
    round as spark_round, abs as spark_abs, sqrt,
    current_date, current_timestamp, date_add, year, month, dayofmonth,
    split, regexp_replace, regexp_extract, trim,
    array, create_map, explode, size,
    cast, to_date, date_format
)
from pyspark.sql.types import IntegerType, DoubleType, DateType
from typing import List, Tuple


def setup_spark() -> SparkSession:
    """Create and return a Spark session for the exercises."""
    return SparkSession.builder \
        .appName("Exercise5-ColumnFunctions") \
        .master("local[*]") \
        .getOrCreate()


def exercise_5a(spark: SparkSession, people_data: List[tuple]):
    """
    Apply string functions to transform and analyze text data.

    Args:
        spark: SparkSession instance
        people_data: List of tuples (full_name, email, phone)

    Returns:
        DataFrame with additional columns:
        - first_name: first word from full_name
        - last_name: last word from full_name
        - name_length: total length of full_name
        - email_domain: domain part of email (after @)
        - phone_clean: phone number with only digits

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use split(), substring(), regexp_extract(), regexp_replace() functions
    # Hint: For email domain, use regexp_extract with pattern r'@(.+)'
    # Hint: For phone cleaning, use regexp_replace to remove non-digits
    pass


def exercise_5b(spark: SparkSession, sales_data: List[tuple]):
    """
    Apply numeric functions and calculations.

    Args:
        spark: SparkSession instance
        sales_data: List of tuples (product, quantity, unit_price, discount_rate)

    Returns:
        DataFrame with additional columns:
        - gross_amount: quantity * unit_price
        - discount_amount: gross_amount * discount_rate (rounded to 2 decimals)
        - net_amount: gross_amount - discount_amount (rounded to 2 decimals)
        - price_category: "Premium" if unit_price > 100, "Standard" if > 50, "Budget" otherwise
        - quantity_group: "High" if quantity >= 10, "Medium" if >= 5, "Low" otherwise

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use arithmetic operations, round(), and when() functions
    pass


def exercise_5c(spark: SparkSession, events_data: List[tuple]):
    """
    Work with date functions and transformations.

    Args:
        spark: SparkSession instance
        events_data: List of tuples (event_name, event_date_str, duration_days)

    Returns:
        DataFrame with additional columns:
        - event_date: event_date_str converted to DateType
        - event_year: year from event_date
        - event_month: month from event_date
        - event_day: day from event_date
        - end_date: event_date + duration_days
        - days_until_event: days between current_date and event_date
        - is_future_event: True if event_date > current_date, False otherwise

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use to_date(), year(), month(), dayofmonth(), date_add(), current_date()
    # For days_until_event, use datediff() function
    pass


def exercise_5d(spark: SparkSession, products_data: List[tuple]):
    """
    Work with arrays and complex data transformations.

    Args:
        spark: SparkSession instance
        products_data: List of tuples (product_name, tags_str, ratings_str)
        - tags_str: comma-separated tags like "electronics,mobile,smartphone"
        - ratings_str: comma-separated ratings like "4.5,4.2,4.8"

    Returns:
        DataFrame with additional columns:
        - tags_array: tags_str split into array
        - ratings_array: ratings_str split into array and cast to double
        - tag_count: number of tags
        - avg_rating: average of all ratings (rounded to 1 decimal)
        - top_tag: first tag from tags_array
        - has_mobile_tag: True if "mobile" is in tags, False otherwise

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use split(), cast(), size(), array functions
    # For avg_rating, you'll need to work with array elements
    pass


def exercise_5e(spark: SparkSession, customers_data: List[tuple]):
    """
    Apply conditional logic and data categorization.

    Args:
        spark: SparkSession instance
        customers_data: List of tuples (customer_id, age, income, purchase_count, last_purchase_days)

    Returns:
        DataFrame with additional columns:
        - age_group: "Young" (<30), "Middle" (30-50), "Senior" (>50)
        - income_level: "High" (>75000), "Medium" (>45000), "Low" (otherwise)
        - customer_value: "High" if income_level="High" AND purchase_count>10,
                         "Medium" if income_level!="Low" AND purchase_count>5,
                         "Low" otherwise
        - retention_risk: "High" if last_purchase_days>90, "Medium" if >30, "Low" otherwise
        - marketing_segment: combination of age_group and income_level (e.g., "Young_High")

    Original columns should be preserved.
    """
    # TODO: Implement this function
    # Use nested when() statements and concat() for complex conditions
    pass


def exercise_5f(spark: SparkSession, orders_data: List[tuple]):
    """
    Combine multiple transformations in a data processing pipeline.

    Args:
        spark: SparkSession instance
        orders_data: List of tuples (order_id, customer_email, order_total_str, order_date_str, status)

    Returns:
        DataFrame with processed and cleaned data:
        - order_id: original order_id
        - customer_domain: domain from customer_email
        - order_total: order_total_str converted to double
        - order_date: order_date_str converted to date
        - order_month_year: formatted as "YYYY-MM"
        - total_category: "Large" (>500), "Medium" (>100), "Small" (otherwise)
        - status_clean: status in uppercase and trimmed
        - is_completed: True if status_clean = "COMPLETED", False otherwise
        - order_age_days: days between order_date and current_date

    Only include orders with valid order_total > 0 and non-null customer_email.
    """
    # TODO: Implement this function
    # Combine multiple transformations and apply filters
    # Use date_format() for order_month_year formatting
    pass


def run_exercises():
    """Run all exercises and display results."""
    spark = setup_spark()

    print("🚀 Running Exercise 5: Column Functions and Data Transformations")
    print("=" * 65)

    # Test Exercise 5a
    print("\n📝 Exercise 5a: String Functions")
    try:
        people_data = [
            ("John Smith", "john.smith@email.com", "(555) 123-4567"),
            ("Jane Doe", "jane.doe@company.org", "555.987.6543"),
            ("Bob Johnson", "bob@test.net", "555-111-2222")
        ]

        result_5a = exercise_5a(spark, people_data)

        if result_5a is not None:
            print("String transformations result:")
            result_5a.show(truncate=False)
            expected_new_cols = ["first_name", "last_name", "name_length", "email_domain", "phone_clean"]
            actual_cols = result_5a.columns
            print(f"Expected new columns: {expected_new_cols}")
            print(f"All columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5b
    print("\n📝 Exercise 5b: Numeric Functions")
    try:
        sales_data = [
            ("Laptop", 2, 999.99, 0.1),
            ("Mouse", 5, 25.50, 0.05),
            ("Keyboard", 12, 75.00, 0.15),
            ("Monitor", 1, 299.99, 0.08)
        ]
        
        result_5b = exercise_5b(spark, sales_data)
        
        if result_5b is not None:
            print("Numeric calculations result:")
            result_5b.show()
            expected_new_cols = ["gross_amount", "discount_amount", "net_amount", "price_category", "quantity_group"]
            actual_cols = result_5b.columns
            print(f"Expected new columns: {expected_new_cols}")
            print(f"All columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5c
    print("\n📝 Exercise 5c: Date Functions")
    try:
        events_data = [
            ("Conference", "2024-06-15", 3),
            ("Workshop", "2023-12-01", 1),
            ("Meeting", "2024-01-30", 0)
        ]
        
        result_5c = exercise_5c(spark, events_data)
        
        if result_5c is not None:
            print("Date transformations result:")
            result_5c.show()
            expected_new_cols = ["event_date", "event_year", "event_month", "event_day", "end_date", "days_until_event", "is_future_event"]
            actual_cols = result_5c.columns
            print(f"Expected new columns: {expected_new_cols}")
            print(f"All columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5d
    print("\n📝 Exercise 5d: Arrays and Complex Data")
    try:
        products_data = [
            ("iPhone", "electronics,mobile,smartphone", "4.5,4.2,4.8"),
            ("Book", "education,reading", "4.0,4.1"),
            ("Laptop", "electronics,computer,mobile", "4.3,4.6,4.4,4.5")
        ]
        
        result_5d = exercise_5d(spark, products_data)
        
        if result_5d is not None:
            print("Array and complex data transformations:")
            result_5d.show(truncate=False)
            expected_new_cols = ["tags_array", "ratings_array", "tag_count", "avg_rating", "top_tag", "has_mobile_tag"]
            actual_cols = result_5d.columns
            print(f"Expected new columns: {expected_new_cols}")
            print(f"All columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5e
    print("\n📝 Exercise 5e: Conditional Logic and Categorization")
    try:
        customers_data = [
            (1, 25, 85000, 15, 20),
            (2, 45, 55000, 8, 45),
            (3, 60, 35000, 3, 120),
            (4, 35, 95000, 25, 10)
        ]
        
        result_5e = exercise_5e(spark, customers_data)
        
        if result_5e is not None:
            print("Customer categorization result:")
            result_5e.show()
            expected_new_cols = ["age_group", "income_level", "customer_value", "retention_risk", "marketing_segment"]
            actual_cols = result_5e.columns
            print(f"Expected new columns: {expected_new_cols}")
            print(f"All columns: {actual_cols}")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5f
    print("\n📝 Exercise 5f: Complete Data Processing Pipeline")
    try:
        orders_data = [
            ("ORD001", "alice@company.com", "250.50", "2023-11-15", " completed "),
            ("ORD002", "bob@email.org", "750.00", "2023-12-01", "PENDING"),
            ("ORD003", None, "0.00", "2023-10-20", "cancelled"),
            ("ORD004", "charlie@test.net", "125.75", "2023-11-30", "COMPLETED")
        ]
        
        result_5f = exercise_5f(spark, orders_data)
        
        if result_5f is not None:
            print("Complete processing pipeline result:")
            result_5f.show()
            expected_cols = ["order_id", "customer_domain", "order_total", "order_date", "order_month_year", "total_category", "status_clean", "is_completed", "order_age_days"]
            actual_cols = result_5f.columns
            print(f"Expected columns: {expected_cols}")
            print(f"Actual columns: {actual_cols}")
            print(f"Row count: {result_5f.count()} (should exclude invalid orders)")
            print(f"✅ Function executed successfully!")
        else:
            print(f"❌ Function returned None")
    except Exception as e:
        print(f"❌ Error: {e}")

    spark.stop()
    print("\n🎉 Exercise 5 completed!")
    print("💡 Outstanding work with column functions and data transformations!")
    print("🔧 You've mastered string, numeric, date, and complex data operations!")
    print("📊 Ready to move on to Exercise 6 for analytics!")


if __name__ == "__main__":
    run_exercises()