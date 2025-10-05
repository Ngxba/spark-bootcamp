"""
ETL job implementation demonstrating common data processing patterns.
This module shows how to build production-ready ETL pipelines with Spark.
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, regexp_replace, trim, upper

from .base_job import BaseSparkJob
from ..transformations.customer_transformations import CustomerTransformations
from ..transformations.product_transformations import ProductTransformations
from ..transformations.sales_transformations import SalesTransformations
from ..schemas.sales_schema import SalesSchema
from ..schemas.customer_schema import CustomerSchema
from ..schemas.product_schema import ProductSchema
from ..schemas.base_schema import SchemaUtils
from ..utils.file_utils import FileUtils


class ETLJob(BaseSparkJob):
    """
    ETL job for processing sales data.

    This job demonstrates:
    - Data ingestion with predefined schemas (no inferSchema)
    - Schema validation and enforcement
    - Data cleaning and validation
    - Data transformation and aggregation
    - Data quality checks
    - Output to multiple formats
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__("SalesETLJob", config)
        self.input_paths = config.get("input_paths", {})
        self.output_path = config.get("output_path", "data/output")
        self.customer_transformer = CustomerTransformations()
        self.product_transformer = ProductTransformations()
        self.sales_transformer = SalesTransformations()

    def read_source_data(self) -> Dict[str, DataFrame]:
        """
        Read data from source files using predefined schemas.

        Returns:
            Dictionary of DataFrames by source name
        """
        self.logger.info("Reading source data with schema validation")

        dataframes = {}

        # Read customers data with predefined schema
        customers_path = self.input_paths.get("customers", "data/customers.csv")
        if FileUtils.file_exists(customers_path):
            self.logger.info(f"Reading customers data from {customers_path}")
            dataframes["customers"] = (
                self.spark.read.option("header", "true")
                .schema(CustomerSchema.get_schema())
                .csv(customers_path)
            )
            self.logger.info(f"Read {dataframes['customers'].count()} customer records")

        # Read products data with predefined schema
        products_path = self.input_paths.get("products", "data/products.csv")
        if FileUtils.file_exists(products_path):
            self.logger.info(f"Reading products data from {products_path}")
            dataframes["products"] = (
                self.spark.read.option("header", "true")
                .schema(ProductSchema.get_schema())
                .csv(products_path)
            )
            self.logger.info(f"Read {dataframes['products'].count()} product records")

        # Read sales data with predefined schema
        sales_path = self.input_paths.get("sales", "data/sales.csv")
        if FileUtils.file_exists(sales_path):
            self.logger.info(f"Reading sales data from {sales_path}")
            dataframes["sales"] = (
                self.spark.read.option("header", "true")
                .schema(SalesSchema.get_schema())
                .csv(sales_path)
            )
            self.logger.info(f"Read {dataframes['sales'].count()} sales records")

        return dataframes

    def validate_schemas(self, dataframes: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        Validate that DataFrames conform to expected schemas.

        Args:
            dataframes: DataFrames to validate

        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating data schemas")

        validation_results = {}
        schema_mappings = {
            "customers": CustomerSchema.get_schema(),
            "products": ProductSchema.get_schema(),
            "sales": SalesSchema.get_schema(),
        }

        for dataset_name, df in dataframes.items():
            if dataset_name in schema_mappings:
                expected_schema = schema_mappings[dataset_name]
                is_valid, differences = SchemaUtils.validate_dataframe_schema(
                    df, expected_schema
                )

                validation_results[dataset_name] = {
                    "is_valid": is_valid,
                    "differences": differences,
                    "expected_fields": len(expected_schema.fields),
                    "actual_fields": len(df.schema.fields),
                }

                if is_valid:
                    self.logger.info(f"✅ Schema validation passed for {dataset_name}")
                else:
                    self.logger.warning(
                        f"⚠️ Schema validation failed for {dataset_name}"
                    )
                    for diff in differences:
                        self.logger.warning(f"  - {diff}")
            else:
                self.logger.warning(
                    f"No schema mapping found for dataset: {dataset_name}"
                )

        return validation_results

    def clean_data(self, dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """
        Clean and validate data.

        Args:
            dataframes: Raw DataFrames

        Returns:
            Cleaned DataFrames
        """
        self.logger.info("Cleaning data")

        cleaned_dfs = {}

        # Clean customers data using domain-specific transformations
        if "customers" in dataframes:
            customers_df = dataframes["customers"]
            cleaned_dfs["customers"] = self.customer_transformer.clean_customer_data(
                customers_df
            )

        # Clean products data using domain-specific transformations
        if "products" in dataframes:
            products_df = dataframes["products"]
            cleaned_dfs["products"] = self.product_transformer.clean_product_data(
                products_df
            )

        # Clean sales data using domain-specific transformations
        if "sales" in dataframes:
            sales_df = dataframes["sales"]
            cleaned_dfs["sales"] = self.sales_transformer.clean_sales_data(sales_df)

        return cleaned_dfs

    def transform_data(self, cleaned_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """
        Apply business transformations.

        Args:
            cleaned_dfs: Cleaned DataFrames

        Returns:
            Transformed DataFrames
        """
        self.logger.info("Transforming data")

        transformed_dfs = {}

        # Create enriched sales data
        if all(df in cleaned_dfs for df in ["sales", "customers", "products"]):
            sales_enriched = self._enrich_sales_data(
                cleaned_dfs["sales"], cleaned_dfs["customers"], cleaned_dfs["products"]
            )
            transformed_dfs["sales_enriched"] = sales_enriched

            # Generate domain-specific analytics
            # Customer analytics
            transformed_dfs["customer_analytics"] = (
                self.customer_transformer.calculate_customer_analytics(sales_enriched)
            )
            transformed_dfs["customer_segmentation"] = (
                self.customer_transformer.create_rfm_segmentation(sales_enriched)
            )

            # Product analytics
            transformed_dfs["product_analytics"] = (
                self.product_transformer.calculate_product_analytics(sales_enriched)
            )
            transformed_dfs["product_performance"] = (
                self.product_transformer.create_product_performance_ranking(
                    transformed_dfs["product_analytics"]
                )
            )
            transformed_dfs["category_analytics"] = (
                self.product_transformer.analyze_category_performance(sales_enriched)
            )

            # Sales analytics
            sales_time_analysis = (
                self.sales_transformer.create_time_based_sales_analysis(sales_enriched)
            )
            transformed_dfs.update(sales_time_analysis)

            # Additional sales insights
            transformed_dfs["transaction_patterns"] = (
                self.sales_transformer.analyze_transaction_patterns(sales_enriched)
            )

        return transformed_dfs

    def _enrich_sales_data(
        self, sales_df: DataFrame, customers_df: DataFrame, products_df: DataFrame
    ) -> DataFrame:
        """
        Enrich sales data with customer and product information.

        Args:
            sales_df: Sales DataFrame
            customers_df: Customers DataFrame
            products_df: Products DataFrame

        Returns:
            Enriched sales DataFrame
        """
        self.logger.info("Enriching sales data with customer and product details")

        # Join with customers
        enriched = (
            sales_df.alias("s")
            .join(
                customers_df.alias("c"),
                col("s.customer_id") == col("c.customer_id"),
                "left",
            )
            .join(
                products_df.alias("p"),
                col("s.product_id") == col("p.product_id"),
                "left",
            )
        )

        # Select and rename columns for clarity
        enriched = enriched.select(
            col("s.transaction_id"),
            col("s.customer_id"),
            col("s.product_id"),
            col("s.quantity"),
            col("s.unit_price"),
            col("s.total_amount"),
            col("s.transaction_date"),
            col("c.customer_name"),
            col("c.age"),
            col("c.gender"),
            col("c.city"),
            col("c.customer_segment"),
            col("p.product_name"),
            col("p.category"),
            col("p.brand"),
            col("p.supplier"),
        )

        return enriched

    def perform_data_quality_checks(
        self, dataframes: Dict[str, DataFrame]
    ) -> Dict[str, Any]:
        """
        Perform data quality checks.

        Args:
            dataframes: DataFrames to check

        Returns:
            Quality check results
        """
        self.logger.info("Performing data quality checks")

        quality_results = {}

        for name, df in dataframes.items():
            row_count = df.count()
            null_counts = {}

            for column in df.columns:
                null_count = df.filter(
                    col(column).isNull() | isnan(col(column))
                ).count()
                null_counts[column] = null_count

            quality_results[name] = {
                "row_count": row_count,
                "null_counts": null_counts,
                "columns": df.columns,
            }

            self.logger.info(
                f"Quality check for {name}: {row_count} rows, "
                f"Null values: {sum(null_counts.values())}"
            )

        return quality_results

    def save_results(self, dataframes: Dict[str, DataFrame]) -> None:
        """
        Save processed data to output location.

        Args:
            dataframes: DataFrames to save
        """
        self.logger.info(f"Saving results to {self.output_path}")

        for name, df in dataframes.items():
            output_path = f"{self.output_path}/{name}"

            # Save as Parquet for efficient storage
            df.coalesce(1).write.mode("overwrite").option("header", "true").parquet(
                output_path
            )

            self.logger.info(f"Saved {name} with {df.count()} records to {output_path}")

            # Also save as CSV for easy inspection
            csv_path = f"{self.output_path}/{name}_csv"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
                csv_path
            )

    def run(self) -> None:
        """Execute the ETL pipeline with schema validation."""
        try:
            # Step 1: Read source data
            raw_dataframes = self.read_source_data()

            if not raw_dataframes:
                self.logger.warning("No source data found")
                return

            # Step 2: Validate schemas
            validation_results = self.validate_schemas(raw_dataframes)

            # Check if any critical schema validations failed
            critical_failures = [
                dataset
                for dataset, result in validation_results.items()
                if not result.get("is_valid", True)
            ]

            if critical_failures:
                self.logger.error(
                    f"Schema validation failed for critical datasets: {critical_failures}"
                )
                self.logger.error("ETL pipeline cannot continue with invalid schemas")
                raise ValueError(
                    f"Schema validation failed for: {', '.join(critical_failures)}"
                )

            # Step 3: Clean data
            cleaned_dataframes = self.clean_data(raw_dataframes)

            # Step 4: Transform data
            transformed_dataframes = self.transform_data(cleaned_dataframes)

            # Step 5: Data quality checks
            quality_results = self.perform_data_quality_checks(transformed_dataframes)

            # Step 6: Save results
            self.save_results(transformed_dataframes)

            # Step 7: Log summary
            self._log_job_summary(quality_results, validation_results)

        except Exception as e:
            self.logger.error(f"ETL job failed: {str(e)}")
            raise

    def _log_job_summary(
        self, quality_results: Dict[str, Any], validation_results: Dict[str, Any]
    ) -> None:
        """Log job execution summary."""
        self.logger.info("=== ETL Job Summary ===")

        # Schema validation summary
        self.logger.info("Schema Validation Results:")
        for dataset_name, results in validation_results.items():
            status = "✅ PASSED" if results["is_valid"] else "❌ FAILED"
            self.logger.info(f"  {dataset_name}: {status}")

        # Data processing summary
        self.logger.info("Data Processing Results:")
        for dataset_name, results in quality_results.items():
            self.logger.info(f"  {dataset_name}: {results['row_count']} rows processed")

        self.logger.info("=======================")
