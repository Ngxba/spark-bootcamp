"""
Exercise 1: Production Project Template
=====================================

This exercise focuses on creating a comprehensive project structure for a
customer analytics ETL pipeline. You'll organize code into modules, implement
configuration management, and create a professional project layout.

Learning Goals:
- Create modular project structure
- Implement separation of concerns
- Design reusable components
- Build configuration-driven applications

Instructions:
1. Create a project structure for customer analytics
2. Implement modular components for data processing
3. Add configuration management
4. Create proper abstractions

Run this file: python exercise_1.py
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import tempfile
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@dataclass
class ProjectConfig:
    """Configuration for the customer analytics project"""
    project_name: str = "customer-analytics"
    input_path: str = "data/raw/customers.csv"
    output_path: str = "data/processed/customer_insights"
    min_age: int = 18
    spark_app_name: str = "CustomerAnalytics"


def exercise_1a_create_project_structure(base_path: Path) -> Dict[str, Path]:
    """
    Create a professional project structure for a customer analytics project.

    Args:
        base_path: Base directory to create the project

    Returns:
        Dictionary mapping component names to their paths

    Required structure:
    customer-analytics/
    ├── README.md
    ├── pyproject.toml
    ├── src/
    │   ├── __init__.py
    │   ├── config/
    │   │   ├── __init__.py
    │   │   └── settings.py
    │   ├── jobs/
    │   │   ├── __init__.py
    │   │   └── customer_job.py
    │   ├── transformations/
    │   │   ├── __init__.py
    │   │   ├── cleaning.py
    │   │   └── analytics.py
    │   ├── utils/
    │   │   ├── __init__.py
    │   │   └── spark_utils.py
    │   └── schemas/
    │       ├── __init__.py
    │       └── customer_schema.py
    ├── tests/
    │   ├── __init__.py
    │   └── test_transformations.py
    └── data/
        ├── raw/
        └── processed/
    """
    # TODO: Implement this function
    # 1. Create the directory structure as specified above
    # 2. Create __init__.py files in all Python packages
    # 3. Create placeholder files for each component
    # 4. Return a dictionary mapping component names to their paths
    pass


def exercise_1b_implement_base_job_class() -> type:
    """
    Create an abstract base class for Spark jobs.

    Returns:
        BaseSparkJob class with abstract methods

    Required methods:
    - __init__(self, config: ProjectConfig)
    - _create_spark_session(self) -> SparkSession
    - extract(self) -> DataFrame (abstract)
    - transform(self, df: DataFrame) -> DataFrame (abstract)
    - load(self, df: DataFrame) -> None (abstract)
    - run(self) -> None
    """
    # TODO: Implement this function
    # 1. Create abstract base class inheriting from ABC
    # 2. Implement __init__ method that takes ProjectConfig
    # 3. Implement _create_spark_session method
    # 4. Define abstract methods for extract, transform, load
    # 5. Implement run method that orchestrates the ETL pipeline
    pass


def exercise_1c_implement_customer_schema() -> StructType:
    """
    Define a Spark schema for customer data.

    Returns:
        StructType schema with the following fields:
        - customer_id: StringType
        - name: StringType
        - age: IntegerType
        - email: StringType
        - income: DoubleType
        - city: StringType
        - registration_date: StringType
    """
    # TODO: Implement this function
    # 1. Create StructType with StructField definitions
    # 2. Use appropriate data types for each field
    # 3. Set nullable appropriately (all should be nullable=True)
    pass


def exercise_1d_implement_data_transformations() -> Dict[str, callable]:
    """
    Implement data transformation functions.

    Returns:
        Dictionary of transformation functions:
        - "clean_customers": function to clean customer data
        - "categorize_customers": function to add customer categories
        - "calculate_city_stats": function to calculate city-level statistics
    """
    # TODO: Implement this function
    # 1. Create clean_customers function that:
    #    - Filters customers with age >= min_age
    #    - Removes invalid email addresses (must contain @)
    #    - Removes duplicate customer_ids
    # 2. Create categorize_customers function that adds:
    #    - age_group: "Young" (<30), "Middle" (30-50), "Senior" (>50)
    #    - income_tier: "High" (>75000), "Medium" (>40000), "Low" (<=40000)
    # 3. Create calculate_city_stats function that:
    #    - Groups by city
    #    - Calculates count, avg_age, avg_income, total_income
    #    - Orders by total_income descending
    pass


def exercise_1e_implement_spark_utilities() -> Dict[str, callable]:
    """
    Implement Spark utility functions.

    Returns:
        Dictionary of utility functions:
        - "create_spark_session": function to create optimized Spark session
        - "read_csv_with_schema": function to read CSV with schema
        - "write_parquet": function to write DataFrame as Parquet
    """
    # TODO: Implement this function
    # 1. Create create_spark_session function that:
    #    - Takes app_name parameter
    #    - Sets local[*] master for local development
    #    - Enables adaptive query execution
    #    - Sets appropriate configurations
    # 2. Create read_csv_with_schema function that:
    #    - Takes spark session, file path, and schema
    #    - Reads CSV with header and schema
    # 3. Create write_parquet function that:
    #    - Takes DataFrame and output path
    #    - Writes as Parquet with overwrite mode
    #    - Uses snappy compression
    pass


def exercise_1f_create_complete_job() -> type:
    """
    Create a complete CustomerAnalyticsJob class that inherits from BaseSparkJob.

    Returns:
        CustomerAnalyticsJob class implementing all abstract methods

    The job should:
    1. Extract customer data from CSV file
    2. Apply cleaning and categorization transformations
    3. Calculate city-level statistics
    4. Save results to Parquet format
    """
    # TODO: Implement this function
    # 1. Create CustomerAnalyticsJob class inheriting from BaseSparkJob
    # 2. Implement extract method to read customer data
    # 3. Implement transform method to apply all transformations
    # 4. Implement load method to save results
    # 5. Use the utility functions and schemas created above
    pass


def run_exercises():
    """Run all exercises and display results."""
    print("🚀 Running Exercise 1: Production Project Template")
    print("=" * 60)

    # Create temporary directory for testing
    temp_dir = Path(tempfile.mkdtemp())
    project_dir = temp_dir / "customer-analytics"

    try:
        # Test Exercise 1a
        print("\n📝 Exercise 1a: Create Project Structure")
        try:
            structure = exercise_1a_create_project_structure(project_dir)

            if structure:
                print("✅ Project structure created:")
                for component, path in structure.items():
                    exists = "✓" if path.exists() else "✗"
                    print(f"  {exists} {component}: {path}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 1b
        print("\n📝 Exercise 1b: Implement Base Job Class")
        try:
            BaseJobClass = exercise_1b_implement_base_job_class()

            if BaseJobClass:
                # Check if it's an abstract class
                if hasattr(BaseJobClass, '__abstractmethods__'):
                    print(f"✅ Abstract base class created with {len(BaseJobClass.__abstractmethods__)} abstract methods")
                    print(f"  Abstract methods: {list(BaseJobClass.__abstractmethods__)}")
                else:
                    print("⚠️  Class created but may not be abstract")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 1c
        print("\n📝 Exercise 1c: Implement Customer Schema")
        try:
            schema = exercise_1c_implement_customer_schema()

            if schema:
                print(f"✅ Schema created with {len(schema.fields)} fields:")
                for field in schema.fields:
                    print(f"  - {field.name}: {field.dataType}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 1d
        print("\n📝 Exercise 1d: Implement Data Transformations")
        try:
            transformations = exercise_1d_implement_data_transformations()

            if transformations:
                print(f"✅ {len(transformations)} transformation functions created:")
                for name in transformations.keys():
                    print(f"  - {name}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 1e
        print("\n📝 Exercise 1e: Implement Spark Utilities")
        try:
            utilities = exercise_1e_implement_spark_utilities()

            if utilities:
                print(f"✅ {len(utilities)} utility functions created:")
                for name in utilities.keys():
                    print(f"  - {name}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 1f
        print("\n📝 Exercise 1f: Create Complete Job")
        try:
            JobClass = exercise_1f_create_complete_job()

            if JobClass:
                print(f"✅ Job class created: {JobClass.__name__}")
                print(f"  Methods: {[m for m in dir(JobClass) if not m.startswith('_')]}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("\n🎉 Exercise 1 completed!")
    print("💡 Focus on creating clean, modular, and maintainable code structure!")
    print("🚀 Ready for Exercise 2!")


if __name__ == "__main__":
    run_exercises()