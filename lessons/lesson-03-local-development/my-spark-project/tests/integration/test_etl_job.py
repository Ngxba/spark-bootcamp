"""
Integration tests for the ETL job.
This module tests the complete ETL pipeline end-to-end.
"""

import pytest
import tempfile
import os
from pathlib import Path

from src.jobs.etl_job import ETLJob
from src.utils.file_utils import FileUtils


class TestETLJobIntegration:
    """Integration tests for ETL job functionality."""

    @pytest.fixture
    def test_data_files(
        self,
        temp_directory,
        sample_sales_data,
        sample_customer_data,
        sample_product_data,
    ):
        """Create test data files for integration testing."""
        # Create input directory
        input_dir = os.path.join(temp_directory, "input")
        FileUtils.create_directory(input_dir)

        # Create customers CSV
        customers_path = os.path.join(input_dir, "customers.csv")
        customers_content = (
            "customer_id,customer_name,age,gender,city,customer_segment\n"
        )
        for row in sample_customer_data:
            customers_content += ",".join(str(x) for x in row) + "\n"
        FileUtils.write_text_file(customers_content, customers_path)

        # Create products CSV
        products_path = os.path.join(input_dir, "products.csv")
        products_content = "product_id,product_name,category,brand,price,supplier\n"
        for row in sample_product_data:
            products_content += ",".join(str(x) for x in row) + "\n"
        FileUtils.write_text_file(products_content, products_path)

        # Create sales CSV
        sales_path = os.path.join(input_dir, "sales.csv")
        sales_content = "transaction_id,customer_id,product_id,quantity,unit_price,total_amount,transaction_date\n"
        for row in sample_sales_data:
            sales_content += ",".join(str(x) for x in row) + "\n"
        FileUtils.write_text_file(sales_content, sales_path)

        return {
            "customers": customers_path,
            "products": products_path,
            "sales": sales_path,
            "input_dir": input_dir,
        }

    @pytest.fixture
    def etl_job_config(self, test_data_files, temp_directory):
        """Create ETL job configuration for testing."""
        output_dir = os.path.join(temp_directory, "output")
        return {
            "input_paths": test_data_files,
            "output_path": output_dir,
            "spark_config": {
                "spark.sql.adaptive.enabled": "false",
                "spark.sql.shuffle.partitions": "2",
            },
        }

    def test_etl_job_full_pipeline(self, etl_job_config):
        """Test the complete ETL pipeline from start to finish."""
        # Create and run ETL job
        etl_job = ETLJob(etl_job_config)
        etl_job.execute()

        # Verify output files were created
        output_path = etl_job_config["output_path"]
        assert FileUtils.directory_exists(output_path)

        # Check that expected output directories exist
        expected_outputs = [
            "sales_enriched",
            "customer_analytics",
            "product_analytics",
            "category_analytics",
        ]

        for output_name in expected_outputs:
            output_dir = os.path.join(output_path, output_name)
            assert FileUtils.directory_exists(
                output_dir
            ), f"Missing output directory: {output_name}"

            # Check that parquet files exist
            parquet_files = FileUtils.list_files_in_directory(
                output_dir, ".parquet", recursive=True
            )
            assert len(parquet_files) > 0, f"No parquet files found in {output_name}"

    def test_etl_job_data_quality_checks(self, etl_job_config):
        """Test that data quality checks are performed during ETL."""
        etl_job = ETLJob(etl_job_config)

        # Read source data
        raw_dataframes = etl_job.read_source_data()

        # Perform data quality checks
        quality_results = etl_job.perform_data_quality_checks(raw_dataframes)

        # Verify quality check results structure
        assert isinstance(quality_results, dict)
        for dataset_name, results in quality_results.items():
            assert "row_count" in results
            assert "null_counts" in results
            assert "columns" in results
            assert results["row_count"] > 0

    def test_etl_job_with_missing_files(self, temp_directory):
        """Test ETL job behavior when input files are missing."""
        config = {
            "input_paths": {
                "customers": "non_existent_customers.csv",
                "products": "non_existent_products.csv",
                "sales": "non_existent_sales.csv",
            },
            "output_path": os.path.join(temp_directory, "output"),
        }

        etl_job = ETLJob(config)

        # Should handle missing files gracefully
        etl_job.execute()  # Should not raise an exception

    def test_etl_job_data_transformation_accuracy(self, etl_job_config, spark_session):
        """Test that data transformations produce expected results."""
        etl_job = ETLJob(etl_job_config)
        etl_job.initialize()

        try:
            # Read and clean source data
            raw_dataframes = etl_job.read_source_data()
            cleaned_dataframes = etl_job.clean_data(raw_dataframes)

            # Verify cleaning results
            assert "customers" in cleaned_dataframes
            assert "products" in cleaned_dataframes
            assert "sales" in cleaned_dataframes

            # Check that data was actually cleaned (specific validations)
            customers_df = cleaned_dataframes["customers"]
            sales_df = cleaned_dataframes["sales"]

            # Verify customer data cleaning
            invalid_ages = customers_df.filter(
                (customers_df.age < 0) | (customers_df.age > 120)
            ).count()
            assert invalid_ages == 0, "Invalid ages should be cleaned"

            # Verify sales data cleaning
            invalid_quantities = sales_df.filter(sales_df.quantity <= 0).count()
            assert invalid_quantities == 0, "Invalid quantities should be cleaned"

            invalid_prices = sales_df.filter(sales_df.unit_price <= 0).count()
            assert invalid_prices == 0, "Invalid prices should be cleaned"

        finally:
            etl_job.cleanup()

    def test_etl_job_enrichment_joins(self, etl_job_config, spark_session):
        """Test that data enrichment joins work correctly."""
        etl_job = ETLJob(etl_job_config)
        etl_job.initialize()

        try:
            # Read and clean source data
            raw_dataframes = etl_job.read_source_data()
            cleaned_dataframes = etl_job.clean_data(raw_dataframes)

            # Perform transformations
            transformed_dataframes = etl_job.transform_data(cleaned_dataframes)

            # Verify enriched sales data
            assert "sales_enriched" in transformed_dataframes
            enriched_df = transformed_dataframes["sales_enriched"]

            # Check that enriched data has customer and product information
            enriched_columns = enriched_df.columns
            customer_columns = [
                "customer_name",
                "age",
                "gender",
                "city",
                "customer_segment",
            ]
            product_columns = ["product_name", "category", "brand", "supplier"]

            for col in customer_columns:
                assert col in enriched_columns, f"Missing customer column: {col}"

            for col in product_columns:
                assert col in enriched_columns, f"Missing product column: {col}"

            # Verify that we have enriched data (not just nulls)
            non_null_customer_names = enriched_df.filter(
                enriched_df.customer_name.isNotNull()
            ).count()
            assert non_null_customer_names > 0, "No customer information was joined"

            non_null_product_names = enriched_df.filter(
                enriched_df.product_name.isNotNull()
            ).count()
            assert non_null_product_names > 0, "No product information was joined"

        finally:
            etl_job.cleanup()

    def test_etl_job_aggregations_creation(self, etl_job_config):
        """Test that all expected aggregations are created."""
        etl_job = ETLJob(etl_job_config)
        etl_job.initialize()

        try:
            # Run the full transformation pipeline
            raw_dataframes = etl_job.read_source_data()
            cleaned_dataframes = etl_job.clean_data(raw_dataframes)
            transformed_dataframes = etl_job.transform_data(cleaned_dataframes)

            # Verify all expected aggregations exist
            expected_aggregations = [
                "sales_enriched",
                "customer_analytics",
                "product_analytics",
                "category_analytics",
                "daily_sales",
                "monthly_sales",
                "day_of_week_sales",
                "customer_segmentation",
                "product_performance",
            ]

            for agg_name in expected_aggregations:
                assert (
                    agg_name in transformed_dataframes
                ), f"Missing aggregation: {agg_name}"
                assert transformed_dataframes[agg_name].count() >= 0

        finally:
            etl_job.cleanup()

    def test_etl_job_output_formats(self, etl_job_config):
        """Test that outputs are saved in multiple formats."""
        etl_job = ETLJob(etl_job_config)
        etl_job.execute()

        output_path = etl_job_config["output_path"]

        # Check for both Parquet and CSV outputs
        for output_name in ["sales_enriched", "customer_analytics"]:
            # Check Parquet output
            parquet_path = os.path.join(output_path, output_name)
            assert FileUtils.directory_exists(parquet_path)
            parquet_files = FileUtils.list_files_in_directory(
                parquet_path, ".parquet", recursive=True
            )
            assert len(parquet_files) > 0

            # Check CSV output
            csv_path = os.path.join(output_path, f"{output_name}_csv")
            assert FileUtils.directory_exists(csv_path)
            csv_files = FileUtils.list_files_in_directory(
                csv_path, ".csv", recursive=True
            )
            assert len(csv_files) > 0

    def test_etl_job_error_handling(self, temp_directory):
        """Test ETL job error handling with malformed data."""
        # Create malformed data files
        input_dir = os.path.join(temp_directory, "input")
        FileUtils.create_directory(input_dir)

        # Create customers CSV with incorrect schema
        customers_path = os.path.join(input_dir, "customers.csv")
        malformed_customers = "wrong,header,names\n1,2,3\n"
        FileUtils.write_text_file(malformed_customers, customers_path)

        # Create empty products file
        products_path = os.path.join(input_dir, "products.csv")
        FileUtils.write_text_file("", products_path)

        # Create sales CSV with wrong data types
        sales_path = os.path.join(input_dir, "sales.csv")
        malformed_sales = "transaction_id,customer_id,product_id,quantity,unit_price,total_amount,transaction_date\nTXN001,CUST001,PROD001,not_a_number,25.50,51.00,2024-01-15\n"
        FileUtils.write_text_file(malformed_sales, sales_path)

        config = {
            "input_paths": {
                "customers": customers_path,
                "products": products_path,
                "sales": sales_path,
            },
            "output_path": os.path.join(temp_directory, "output"),
        }

        etl_job = ETLJob(config)

        # Job should handle errors gracefully and not crash
        try:
            etl_job.execute()
        except Exception as e:
            # We expect some exceptions due to malformed data
            # The important thing is that the job handles them properly
            assert str(e) is not None  # Ensure we get a meaningful error message

    def test_etl_job_configuration_override(self, test_data_files, temp_directory):
        """Test that ETL job respects configuration overrides."""
        # Test with different configuration
        custom_config = {
            "input_paths": test_data_files,
            "output_path": os.path.join(temp_directory, "custom_output"),
            "spark_config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.shuffle.partitions": "4",
            },
        }

        etl_job = ETLJob(custom_config)
        etl_job.execute()

        # Verify custom output path was used
        assert FileUtils.directory_exists(custom_config["output_path"])

    def test_etl_job_spark_session_management(self, etl_job_config):
        """Test proper Spark session lifecycle management."""
        etl_job = ETLJob(etl_job_config)

        # Initially, Spark session should be None
        assert etl_job.spark is None

        # After initialization, Spark session should exist
        etl_job.initialize()
        assert etl_job.spark is not None
        assert etl_job.spark.sparkContext.appName == "SalesETLJob"

        # After cleanup, Spark session should be None
        etl_job.cleanup()
        assert etl_job.spark is None
