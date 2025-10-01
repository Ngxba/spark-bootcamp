"""
Solution 2: Modular Refactoring
===============================

Complete solution for Exercise 2 demonstrating how to refactor a monolithic
Spark application into a modular, maintainable structure.

This solution shows:
- Breaking down monolithic code
- Creating reusable modules
- Implementing proper abstractions
- Adding error handling and logging
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum, max as spark_max, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from typing import Dict, List, Any, Optional, Callable
import logging
from datetime import datetime
from dataclasses import dataclass
import tempfile
import pandas as pd


def exercise_2a_create_configuration_module() -> type:
    """
    SOLUTION: Create a configuration class to manage all settings.
    """

    @dataclass
    class SalesAnalysisConfig:
        """Configuration for sales analysis application"""

        # Application settings
        app_name: str = "SalesAnalysis"
        version: str = "1.0.0"
        debug: bool = False

        # Input/output paths
        sales_input_path: str = "data/raw/sales.csv"
        customers_input_path: str = "data/raw/customers.csv"
        customer_stats_output: str = "data/processed/customer_stats"
        product_stats_output: str = "data/processed/product_stats"
        daily_trends_output: str = "data/processed/daily_trends"

        # Spark settings
        spark_master: str = "local[*]"
        spark_sql_shuffle_partitions: int = 200
        spark_adaptive_enabled: bool = True

        # Business rules
        min_customer_age: int = 18
        email_regex: str = r'^[^@]+@[^@]+\\.[^@]+$'
        min_amount: float = 0.0
        min_quantity: int = 0

        # Output settings
        output_format: str = "parquet"
        compression: str = "snappy"

        def validate(self) -> bool:
            """Validate configuration settings"""
            if self.min_customer_age < 0:
                raise ValueError("Minimum customer age cannot be negative")
            if self.min_amount < 0:
                raise ValueError("Minimum amount cannot be negative")
            if self.min_quantity < 0:
                raise ValueError("Minimum quantity cannot be negative")
            return True

        def get_spark_configs(self) -> Dict[str, str]:
            """Get Spark configuration as dictionary"""
            return {
                "spark.sql.shuffle.partitions": str(self.spark_sql_shuffle_partitions),
                "spark.sql.adaptive.enabled": str(self.spark_adaptive_enabled).lower(),
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }

    return SalesAnalysisConfig


def exercise_2b_create_spark_utilities() -> Dict[str, callable]:
    """
    SOLUTION: Extract Spark session creation and utilities into a separate module.
    """

    def create_spark_session(app_name: str, master: str = "local[*]",
                           configs: Dict[str, str] = None) -> SparkSession:
        """Create optimized Spark session"""
        builder = (SparkSession.builder
                  .appName(app_name)
                  .master(master))

        # Add custom configurations
        if configs:
            for key, value in configs.items():
                builder = builder.config(key, value)

        # Add default optimizations
        default_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }

        for key, value in default_configs.items():
            if not configs or key not in configs:
                builder = builder.config(key, value)

        spark = builder.getOrCreate()

        # Set log level for cleaner output
        spark.sparkContext.setLogLevel("WARN")

        logging.info(f"Spark session created: {app_name}")
        return spark

    def read_csv_with_options(spark: SparkSession, file_path: str,
                             header: bool = True, infer_schema: bool = True) -> DataFrame:
        """Standardized CSV reading with error handling"""
        try:
            df = (spark.read
                  .option("header", str(header).lower())
                  .option("inferSchema", str(infer_schema).lower())
                  .csv(file_path))

            logging.info(f"Successfully read CSV: {file_path}, rows: {df.count()}")
            return df

        except Exception as e:
            logging.error(f"Failed to read CSV {file_path}: {str(e)}")
            raise

    def write_parquet_with_options(df: DataFrame, output_path: str,
                                  mode: str = "overwrite",
                                  compression: str = "snappy") -> None:
        """Standardized Parquet writing with error handling"""
        try:
            (df.write
             .mode(mode)
             .option("compression", compression)
             .parquet(output_path))

            logging.info(f"Successfully wrote Parquet: {output_path}")

        except Exception as e:
            logging.error(f"Failed to write Parquet {output_path}: {str(e)}")
            raise

    return {
        "create_spark_session": create_spark_session,
        "read_csv_with_options": read_csv_with_options,
        "write_parquet_with_options": write_parquet_with_options
    }


def exercise_2c_create_data_validation_module() -> Dict[str, callable]:
    """
    SOLUTION: Create a module for data validation and cleaning functions.
    """

    def validate_sales_data(df: DataFrame, min_amount: float = 0.0,
                           min_quantity: int = 0) -> DataFrame:
        """Validate and clean sales data"""
        initial_count = df.count()

        # Apply validation filters
        cleaned_df = (df
                     .filter(col("amount") > min_amount)
                     .filter(col("quantity") > min_quantity)
                     .filter(col("sale_date").isNotNull())
                     .filter(col("transaction_id").isNotNull())
                     .dropDuplicates(["transaction_id"]))

        final_count = cleaned_df.count()
        removed_count = initial_count - final_count

        logging.info(f"Sales validation: {initial_count} -> {final_count} records "
                    f"({removed_count} removed)")

        return cleaned_df

    def validate_customer_data(df: DataFrame, min_age: int = 18,
                              email_regex: str = r'^[^@]+@[^@]+\\.[^@]+$') -> DataFrame:
        """Validate and clean customer data"""
        initial_count = df.count()

        # Apply validation filters
        cleaned_df = (df
                     .filter(col("age") >= min_age)
                     .filter(col("email").rlike(email_regex))
                     .filter(col("customer_id").isNotNull())
                     .dropDuplicates(["customer_id"]))

        final_count = cleaned_df.count()
        removed_count = initial_count - final_count

        logging.info(f"Customer validation: {initial_count} -> {final_count} records "
                    f"({removed_count} removed)")

        return cleaned_df

    def check_data_quality(df: DataFrame, df_name: str) -> Dict[str, Any]:
        """Perform comprehensive data quality checks"""
        total_rows = df.count()
        total_cols = len(df.columns)

        quality_report = {
            "dataset": df_name,
            "total_rows": total_rows,
            "total_columns": total_cols,
            "null_counts": {},
            "duplicate_rows": 0
        }

        # Check for null values in each column
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            quality_report["null_counts"][column] = null_count

        # Check for duplicate rows
        duplicate_count = total_rows - df.dropDuplicates().count()
        quality_report["duplicate_rows"] = duplicate_count

        logging.info(f"Data quality check for {df_name}: {quality_report}")

        return quality_report

    return {
        "validate_sales_data": validate_sales_data,
        "validate_customer_data": validate_customer_data,
        "check_data_quality": check_data_quality
    }


def exercise_2d_create_transformation_module() -> Dict[str, callable]:
    """
    SOLUTION: Create a module for data transformations and enrichment.
    """

    def enrich_sales_data(df: DataFrame) -> DataFrame:
        """Add calculated columns to sales data"""
        enriched_df = df.withColumn("revenue", col("amount") * col("quantity"))

        logging.info("Sales data enriched with revenue column")
        return enriched_df

    def categorize_customers(df: DataFrame) -> DataFrame:
        """Add customer tier categorization"""
        categorized_df = df.withColumn(
            "customer_tier",
            when(col("age") < 30, "Young")
            .when(col("age") < 50, "Middle")
            .otherwise("Senior")
        )

        logging.info("Customer data enriched with tier categorization")
        return categorized_df

    def calculate_revenue_metrics(df: DataFrame) -> DataFrame:
        """Add purchase category based on revenue"""
        metrics_df = df.withColumn(
            "purchase_category",
            when(col("revenue") > 1000, "High")
            .when(col("revenue") > 500, "Medium")
            .otherwise("Low")
        )

        logging.info("Revenue metrics calculated")
        return metrics_df

    return {
        "enrich_sales_data": enrich_sales_data,
        "categorize_customers": categorize_customers,
        "calculate_revenue_metrics": calculate_revenue_metrics
    }


def exercise_2e_create_analytics_module() -> Dict[str, callable]:
    """
    SOLUTION: Create a module for analytics and aggregation functions.
    """

    def calculate_customer_stats(df: DataFrame) -> DataFrame:
        """Calculate customer-level analytics"""
        customer_stats = (df
                         .groupBy("customer_id", "name", "customer_tier")
                         .agg(
                             count("transaction_id").alias("total_transactions"),
                             sum("revenue").alias("total_revenue"),
                             avg("revenue").alias("avg_revenue"),
                             max("sale_date").alias("last_purchase_date")
                         )
                         .orderBy(desc("total_revenue")))

        logging.info(f"Customer statistics calculated for {customer_stats.count()} customers")
        return customer_stats

    def calculate_product_stats(df: DataFrame) -> DataFrame:
        """Calculate product-level analytics"""
        product_stats = (df
                        .groupBy("product_category")
                        .agg(
                            count("transaction_id").alias("total_sales"),
                            sum("revenue").alias("total_revenue"),
                            avg("revenue").alias("avg_sale_amount"),
                            count("customer_id").alias("unique_customers")
                        )
                        .orderBy(desc("total_revenue")))

        logging.info(f"Product statistics calculated for {product_stats.count()} categories")
        return product_stats

    def calculate_daily_trends(df: DataFrame) -> DataFrame:
        """Calculate time-series analytics"""
        daily_trends = (df
                       .groupBy("sale_date")
                       .agg(
                           count("transaction_id").alias("transactions"),
                           sum("revenue").alias("daily_revenue"),
                           avg("revenue").alias("avg_transaction_amount")
                       )
                       .orderBy("sale_date"))

        logging.info(f"Daily trends calculated for {daily_trends.count()} days")
        return daily_trends

    return {
        "calculate_customer_stats": calculate_customer_stats,
        "calculate_product_stats": calculate_product_stats,
        "calculate_daily_trends": calculate_daily_trends
    }


def exercise_2f_create_refactored_job() -> type:
    """
    SOLUTION: Create a refactored SalesAnalysisJob class using all the modules above.
    """

    # Get all the modules
    SalesAnalysisConfig = exercise_2a_create_configuration_module()
    spark_utils = exercise_2b_create_spark_utilities()
    validation_funcs = exercise_2c_create_data_validation_module()
    transformation_funcs = exercise_2d_create_transformation_module()
    analytics_funcs = exercise_2e_create_analytics_module()

    class SalesAnalysisJob:
        """Refactored sales analysis job with modular design"""

        def __init__(self, config: SalesAnalysisConfig):
            self.config = config
            self.spark: Optional[SparkSession] = None
            self._setup_logging()

        def _setup_logging(self):
            """Setup logging configuration"""
            log_level = logging.DEBUG if self.config.debug else logging.INFO
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.logger = logging.getLogger(__name__)

        def _initialize_spark(self):
            """Initialize Spark session"""
            self.spark = spark_utils["create_spark_session"](
                self.config.app_name,
                self.config.spark_master,
                self.config.get_spark_configs()
            )

        def _load_and_validate_data(self) -> tuple[DataFrame, DataFrame]:
            """Load and validate input data"""
            self.logger.info("Loading input data...")

            # Load raw data
            sales_df = spark_utils["read_csv_with_options"](
                self.spark, self.config.sales_input_path
            )
            customers_df = spark_utils["read_csv_with_options"](
                self.spark, self.config.customers_input_path
            )

            # Validate data
            clean_sales = validation_funcs["validate_sales_data"](
                sales_df, self.config.min_amount, self.config.min_quantity
            )
            clean_customers = validation_funcs["validate_customer_data"](
                customers_df, self.config.min_customer_age, self.config.email_regex
            )

            # Data quality checks
            validation_funcs["check_data_quality"](clean_sales, "sales")
            validation_funcs["check_data_quality"](clean_customers, "customers")

            return clean_sales, clean_customers

        def _join_and_enrich_data(self, sales_df: DataFrame,
                                customers_df: DataFrame) -> DataFrame:
            """Join sales with customers and apply enrichments"""
            self.logger.info("Joining and enriching data...")

            # Join datasets
            joined_df = sales_df.join(
                customers_df,
                sales_df.customer_id == customers_df.customer_id,
                "inner"
            ).drop(customers_df.customer_id)

            # Apply transformations
            enriched_df = transformation_funcs["enrich_sales_data"](joined_df)
            categorized_df = transformation_funcs["categorize_customers"](enriched_df)
            final_df = transformation_funcs["calculate_revenue_metrics"](categorized_df)

            return final_df

        def _calculate_analytics(self, df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
            """Calculate all analytics"""
            self.logger.info("Calculating analytics...")

            customer_stats = analytics_funcs["calculate_customer_stats"](df)
            product_stats = analytics_funcs["calculate_product_stats"](df)
            daily_trends = analytics_funcs["calculate_daily_trends"](df)

            return customer_stats, product_stats, daily_trends

        def _save_results(self, customer_stats: DataFrame,
                         product_stats: DataFrame, daily_trends: DataFrame):
            """Save all results"""
            self.logger.info("Saving results...")

            spark_utils["write_parquet_with_options"](
                customer_stats, self.config.customer_stats_output,
                compression=self.config.compression
            )
            spark_utils["write_parquet_with_options"](
                product_stats, self.config.product_stats_output,
                compression=self.config.compression
            )
            spark_utils["write_parquet_with_options"](
                daily_trends, self.config.daily_trends_output,
                compression=self.config.compression
            )

        def run(self) -> Dict[str, Any]:
            """Execute the complete sales analysis pipeline"""
            try:
                self.logger.info(f"Starting {self.config.app_name} v{self.config.version}")

                # Validate configuration
                self.config.validate()

                # Initialize Spark
                self._initialize_spark()

                # Load and validate data
                sales_df, customers_df = self._load_and_validate_data()

                # Join and enrich data
                enriched_df = self._join_and_enrich_data(sales_df, customers_df)

                # Calculate analytics
                customer_stats, product_stats, daily_trends = self._calculate_analytics(enriched_df)

                # Save results
                self._save_results(customer_stats, product_stats, daily_trends)

                # Return summary
                result = {
                    "status": "success",
                    "customers": customer_stats.count(),
                    "products": product_stats.count(),
                    "days": daily_trends.count(),
                    "total_records_processed": enriched_df.count()
                }

                self.logger.info(f"Job completed successfully: {result}")
                return result

            except Exception as e:
                self.logger.error(f"Job failed: {str(e)}")
                raise
            finally:
                if self.spark:
                    self.spark.stop()
                    self.logger.info("Spark session stopped")

    return SalesAnalysisJob


def exercise_2g_create_logging_and_monitoring() -> Dict[str, callable]:
    """
    SOLUTION: Create logging and monitoring utilities.
    """

    def setup_logging(log_level: str = "INFO", log_format: str = None) -> None:
        """Configure application logging"""
        if log_format is None:
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f'Invalid log level: {log_level}')

        logging.basicConfig(level=numeric_level, format=log_format)

    def log_dataframe_info(df: DataFrame, df_name: str) -> None:
        """Log comprehensive DataFrame information"""
        count = df.count()
        columns = len(df.columns)

        logging.info(f"DataFrame '{df_name}': {count:,} rows, {columns} columns")
        logging.debug(f"Schema for '{df_name}': {df.columns}")

    def monitor_performance(func: Callable) -> Callable:
        """Decorator to monitor function performance"""
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            function_name = func.__name__

            logging.info(f"Starting {function_name}")

            try:
                result = func(*args, **kwargs)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                logging.info(f"Completed {function_name} in {duration:.2f} seconds")
                return result

            except Exception as e:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                logging.error(f"Failed {function_name} after {duration:.2f} seconds: {str(e)}")
                raise

        return wrapper

    return {
        "setup_logging": setup_logging,
        "log_dataframe_info": log_dataframe_info,
        "monitor_performance": monitor_performance
    }


def run_complete_solution():
    """Run the complete refactored solution"""
    print("🚀 Running Complete Solution: Modular Refactoring")
    print("=" * 55)

    # Create sample data for demonstration
    print("\\n📊 Creating sample data...")

    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    data_dir = temp_dir / "data"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"

    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    try:
        # Create sample sales data
        sales_data = pd.DataFrame({
            'transaction_id': ['T001', 'T002', 'T003', 'T004', 'T005'],
            'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
            'product_category': ['Electronics', 'Clothing', 'Electronics', 'Books', 'Clothing'],
            'amount': [99.99, 49.99, 149.99, 29.99, 79.99],
            'quantity': [1, 2, 1, 3, 1],
            'sale_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']
        })
        sales_data.to_csv(raw_dir / "sales.csv", index=False)

        # Create sample customer data
        customers_data = pd.DataFrame({
            'customer_id': ['C001', 'C002', 'C003'],
            'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown'],
            'age': [28, 35, 42],
            'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com']
        })
        customers_data.to_csv(raw_dir / "customers.csv", index=False)

        print("✅ Sample data created")

        # Test the refactored modules
        print("\\n🧪 Testing Refactored Modules:")

        # Test configuration
        SalesAnalysisConfig = exercise_2a_create_configuration_module()
        config = SalesAnalysisConfig(
            sales_input_path=str(raw_dir / "sales.csv"),
            customers_input_path=str(raw_dir / "customers.csv"),
            customer_stats_output=str(processed_dir / "customer_stats"),
            product_stats_output=str(processed_dir / "product_stats"),
            daily_trends_output=str(processed_dir / "daily_trends")
        )
        print(f"✅ Configuration: {config.app_name} v{config.version}")

        # Test the complete refactored job
        SalesAnalysisJob = exercise_2f_create_refactored_job()
        job = SalesAnalysisJob(config)

        print("\\n🔄 Running refactored job...")
        result = job.run()

        print(f"\\n📈 Job Results:")
        for key, value in result.items():
            print(f"  {key}: {value}")

        print("\\n✅ Refactoring Benefits Demonstrated:")
        print("  🔧 Modular design - easy to test and maintain")
        print("  📊 Comprehensive logging - full visibility")
        print("  ⚙️  Configuration-driven - flexible deployment")
        print("  🧪 Testable components - isolated functionality")
        print("  🔒 Error handling - robust execution")
        print("  📈 Monitoring - performance tracking")

    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("\\n🎉 Modular refactoring solution completed!")


if __name__ == "__main__":
    run_complete_solution()