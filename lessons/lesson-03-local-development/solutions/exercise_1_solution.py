"""
Solution 1: Production Project Template
======================================

Complete solution for Exercise 1 demonstrating how to create a professional
project structure for a customer analytics ETL pipeline.

This solution shows:
- Modular project organization
- Separation of concerns
- Configuration-driven design
- Professional abstractions
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import tempfile
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum, regexp_extract
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
    SOLUTION: Create a professional project structure for a customer analytics project.
    """
    project_path = base_path / "customer-analytics"

    # Define the complete directory structure
    directories = [
        "src",
        "src/config",
        "src/jobs",
        "src/transformations",
        "src/utils",
        "src/schemas",
        "tests",
        "data",
        "data/raw",
        "data/processed"
    ]

    # Create all directories
    for directory in directories:
        (project_path / directory).mkdir(parents=True, exist_ok=True)

    # Create __init__.py files for Python packages
    python_packages = [
        "src",
        "src/config",
        "src/jobs",
        "src/transformations",
        "src/utils",
        "src/schemas",
        "tests"
    ]

    for package in python_packages:
        init_file = project_path / package / "__init__.py"
        init_file.write_text("# Package initialization\\n")

    # Create main project files
    files_to_create = {
        "README.md": """# Customer Analytics Project

A modular Spark application for customer data analysis.

## Setup
```bash
pip install -r requirements.txt
```

## Usage
```bash
python -m src.jobs.customer_job
```
""",
        "pyproject.toml": """[project]
name = "customer-analytics"
version = "0.1.0"
dependencies = ["pyspark>=3.5.0"]
""",
        "src/config/settings.py": "# Configuration settings",
        "src/jobs/customer_job.py": "# Main customer analytics job",
        "src/transformations/cleaning.py": "# Data cleaning functions",
        "src/transformations/analytics.py": "# Analytics functions",
        "src/utils/spark_utils.py": "# Spark utilities",
        "src/schemas/customer_schema.py": "# Customer data schema",
        "tests/test_transformations.py": "# Transformation tests",
        "data/raw/.gitkeep": "# Keep directory in git",
        "data/processed/.gitkeep": "# Keep directory in git"
    }

    for file_path, content in files_to_create.items():
        full_path = project_path / file_path
        full_path.write_text(content)

    # Return dictionary mapping component names to their paths
    return {
        "project_root": project_path,
        "src": project_path / "src",
        "config": project_path / "src" / "config",
        "jobs": project_path / "src" / "jobs",
        "transformations": project_path / "src" / "transformations",
        "utils": project_path / "src" / "utils",
        "schemas": project_path / "src" / "schemas",
        "tests": project_path / "tests",
        "data": project_path / "data",
        "readme": project_path / "README.md",
        "pyproject": project_path / "pyproject.toml"
    }


def exercise_1b_implement_base_job_class() -> type:
    """
    SOLUTION: Create an abstract base class for Spark jobs.
    """

    class BaseSparkJob(ABC):
        """Abstract base class for Spark jobs with ETL pattern"""

        def __init__(self, config: ProjectConfig):
            self.config = config
            self.spark: Optional[SparkSession] = None

        def _create_spark_session(self) -> SparkSession:
            """Create optimized Spark session"""
            return (SparkSession.builder
                   .appName(self.config.spark_app_name)
                   .master("local[*]")
                   .config("spark.sql.adaptive.enabled", "true")
                   .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                   .getOrCreate())

        @abstractmethod
        def extract(self) -> DataFrame:
            """Extract data from source - to be implemented by subclasses"""
            pass

        @abstractmethod
        def transform(self, df: DataFrame) -> DataFrame:
            """Transform the data - to be implemented by subclasses"""
            pass

        @abstractmethod
        def load(self, df: DataFrame) -> None:
            """Load data to destination - to be implemented by subclasses"""
            pass

        def run(self) -> None:
            """Execute the complete ETL pipeline"""
            try:
                print(f"🚀 Starting {self.config.spark_app_name}")

                # Initialize Spark session
                self.spark = self._create_spark_session()
                print(f"✅ Spark session created: {self.spark.sparkContext.appName}")

                # Execute ETL pipeline
                raw_data = self.extract()
                print(f"📥 Extracted {raw_data.count():,} records")

                transformed_data = self.transform(raw_data)
                print(f"🔄 Transformed data ready")

                self.load(transformed_data)
                print(f"💾 Data loaded successfully")

                print(f"✅ Job completed successfully!")

            except Exception as e:
                print(f"❌ Job failed: {str(e)}")
                raise
            finally:
                if self.spark:
                    self.spark.stop()
                    print("🛑 Spark session stopped")

    return BaseSparkJob


def exercise_1c_implement_customer_schema() -> StructType:
    """
    SOLUTION: Define a Spark schema for customer data.
    """
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("income", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("registration_date", StringType(), True)
    ])


def exercise_1d_implement_data_transformations() -> Dict[str, callable]:
    """
    SOLUTION: Implement data transformation functions.
    """

    def clean_customers(df: DataFrame, min_age: int = 18) -> DataFrame:
        """Clean customer data by removing invalid records"""
        return (df
                .filter(col("age") >= min_age)  # Filter by minimum age
                .filter(col("email").rlike(r'^[^@]+@[^@]+\\.[^@]+$'))  # Valid email
                .filter(col("name").isNotNull())  # Non-null names
                .dropDuplicates(["customer_id"]))  # Remove duplicates

    def categorize_customers(df: DataFrame) -> DataFrame:
        """Add customer categorization columns"""
        return (df
                .withColumn("age_group",
                           when(col("age") < 30, "Young")
                           .when(col("age") <= 50, "Middle")
                           .otherwise("Senior"))
                .withColumn("income_tier",
                           when(col("income") > 75000, "High")
                           .when(col("income") > 40000, "Medium")
                           .otherwise("Low")))

    def calculate_city_stats(df: DataFrame) -> DataFrame:
        """Calculate city-level statistics"""
        return (df
                .groupBy("city")
                .agg(
                    count("customer_id").alias("customer_count"),
                    avg("age").alias("avg_age"),
                    avg("income").alias("avg_income"),
                    sum("income").alias("total_income")
                )
                .orderBy(col("total_income").desc()))

    return {
        "clean_customers": clean_customers,
        "categorize_customers": categorize_customers,
        "calculate_city_stats": calculate_city_stats
    }


def exercise_1e_implement_spark_utilities() -> Dict[str, callable]:
    """
    SOLUTION: Implement Spark utility functions.
    """

    def create_spark_session(app_name: str) -> SparkSession:
        """Create optimized Spark session for local development"""
        return (SparkSession.builder
               .appName(app_name)
               .master("local[*]")
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
               .config("spark.sql.adaptive.skewJoin.enabled", "true")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .getOrCreate())

    def read_csv_with_schema(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
        """Read CSV file with provided schema"""
        return (spark.read
               .schema(schema)
               .option("header", "true")
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
               .option("dateFormat", "yyyy-MM-dd")
               .csv(file_path))

    def write_parquet(df: DataFrame, output_path: str) -> None:
        """Write DataFrame as Parquet with optimization"""
        (df.write
         .mode("overwrite")
         .option("compression", "snappy")
         .option("mergeSchema", "true")
         .parquet(output_path))

    return {
        "create_spark_session": create_spark_session,
        "read_csv_with_schema": read_csv_with_schema,
        "write_parquet": write_parquet
    }


def exercise_1f_create_complete_job() -> type:
    """
    SOLUTION: Create a complete CustomerAnalyticsJob class.
    """

    # Get the base class and utilities
    BaseSparkJob = exercise_1b_implement_base_job_class()
    customer_schema = exercise_1c_implement_customer_schema()
    transformations = exercise_1d_implement_data_transformations()
    utilities = exercise_1e_implement_spark_utilities()

    class CustomerAnalyticsJob(BaseSparkJob):
        """Complete customer analytics job implementation"""

        def extract(self) -> DataFrame:
            """Extract customer data from CSV file"""
            return utilities["read_csv_with_schema"](
                self.spark,
                self.config.input_path,
                customer_schema
            )

        def transform(self, df: DataFrame) -> DataFrame:
            """Apply all transformations to customer data"""
            # Apply cleaning
            clean_df = transformations["clean_customers"](df, self.config.min_age)
            print(f"🧹 Cleaned data: {clean_df.count():,} records")

            # Apply categorization
            categorized_df = transformations["categorize_customers"](clean_df)
            print("📊 Added customer categories")

            # Calculate city statistics
            city_stats = transformations["calculate_city_stats"](categorized_df)
            print(f"🏙️  Calculated stats for {city_stats.count()} cities")

            return city_stats

        def load(self, df: DataFrame) -> None:
            """Save results to Parquet format"""
            utilities["write_parquet"](df, self.config.output_path)
            print(f"💾 Results saved to {self.config.output_path}")

    return CustomerAnalyticsJob


def run_complete_solution():
    """Run the complete solution demonstration"""
    print("🚀 Running Complete Solution: Production Project Template")
    print("=" * 65)

    # Create temporary directory for demonstration
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Step 1: Create project structure
        print("\\n📁 Step 1: Creating Project Structure")
        structure = exercise_1a_create_project_structure(temp_dir)

        print("✅ Project structure created:")
        for component, path in structure.items():
            exists = "✓" if path.exists() else "✗"
            print(f"  {exists} {component}: {path.name}")

        # Step 2: Demonstrate base job class
        print("\\n🏗️  Step 2: Base Job Class")
        BaseJobClass = exercise_1b_implement_base_job_class()
        print(f"✅ BaseSparkJob created with abstract methods: {list(BaseJobClass.__abstractmethods__)}")

        # Step 3: Show customer schema
        print("\\n📋 Step 3: Customer Schema")
        schema = exercise_1c_implement_customer_schema()
        print(f"✅ Schema with {len(schema.fields)} fields:")
        for field in schema.fields:
            print(f"  - {field.name}: {field.dataType}")

        # Step 4: Show transformations
        print("\\n🔄 Step 4: Data Transformations")
        transformations = exercise_1d_implement_data_transformations()
        print(f"✅ {len(transformations)} transformation functions:")
        for name in transformations.keys():
            print(f"  - {name}")

        # Step 5: Show utilities
        print("\\n🛠️  Step 5: Spark Utilities")
        utilities = exercise_1e_implement_spark_utilities()
        print(f"✅ {len(utilities)} utility functions:")
        for name in utilities.keys():
            print(f"  - {name}")

        # Step 6: Complete job demonstration
        print("\\n🎯 Step 6: Complete Job Implementation")
        CustomerJobClass = exercise_1f_create_complete_job()
        print(f"✅ {CustomerJobClass.__name__} created")

        # Show the methods
        methods = [m for m in dir(CustomerJobClass) if not m.startswith('_') and callable(getattr(CustomerJobClass, m))]
        print(f"  Methods: {methods}")

        print("\\n📊 Architecture Benefits:")
        print("  ✅ Modular design - easy to test and maintain")
        print("  ✅ Separation of concerns - clear responsibilities")
        print("  ✅ Configuration-driven - flexible deployment")
        print("  ✅ Reusable components - utilities and transformations")
        print("  ✅ Professional structure - scalable and extensible")

    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("\\n🎉 Solution demonstration completed!")
    print("💡 This structure provides a solid foundation for production Spark applications!")


if __name__ == "__main__":
    run_complete_solution()