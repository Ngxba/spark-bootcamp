"""
Integration tests for ETL pipeline functionality.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class MockETLJob:
    """Mock ETL job for testing pipeline integration."""

    def __init__(self, config, metrics=None):
        self.config = config
        self.metrics = metrics or Mock()
        self.spark = None

    def extract(self) -> DataFrame:
        """Mock extraction that returns test data."""
        # This would normally read from external sources
        # For testing, we return a simple DataFrame
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()

        data = [
            ("C001", "Alice", 28, "alice@email.com", 75000.0),
            ("C002", "Bob", 35, "bob@email.com", 65000.0),
            ("C003", "Charlie", 42, "charlie@email.com", 85000.0),
        ]

        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("income", DoubleType(), True)
        ])

        return spark.createDataFrame(data, schema)

    def transform(self, df: DataFrame) -> DataFrame:
        """Mock transformation that applies basic cleaning and enrichment."""
        # Apply basic transformations
        cleaned_df = (df
                     .filter(col("age") >= 18)
                     .filter(col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$'))
                     .filter(col("name").isNotNull()))

        # Add derived columns
        from pyspark.sql.functions import when
        enriched_df = (cleaned_df
                      .withColumn("age_group",
                                 when(col("age") < 30, "Young")
                                 .when(col("age") <= 50, "Middle")
                                 .otherwise("Senior"))
                      .withColumn("income_tier",
                                 when(col("income") > 80000, "High")
                                 .when(col("income") > 50000, "Medium")
                                 .otherwise("Low")))

        return enriched_df

    def load(self, df: DataFrame) -> dict:
        """Mock loading that simulates saving data."""
        # In a real implementation, this would save to external storage
        # For testing, we just return metrics
        record_count = df.count()

        return {
            "records_loaded": record_count,
            "output_format": "parquet",
            "success": True
        }


class TestETLPipelineIntegration:
    """Integration tests for complete ETL pipeline execution."""

    def test_complete_etl_pipeline_execution(self, spark_session, sample_config):
        """Test complete ETL pipeline from extract to load."""
        # Create mock ETL job
        job = MockETLJob(sample_config)
        job.spark = spark_session

        # Execute extract stage
        raw_data = job.extract()
        assert raw_data.count() > 0, "Extract stage should return data"
        assert "customer_id" in raw_data.columns, "Extract should return customer data"

        # Execute transform stage
        transformed_data = job.transform(raw_data)
        assert transformed_data.count() > 0, "Transform stage should return data"
        assert "age_group" in transformed_data.columns, "Transform should add age_group column"
        assert "income_tier" in transformed_data.columns, "Transform should add income_tier column"

        # Verify transformations
        age_groups = [row.age_group for row in transformed_data.collect()]
        assert all(group in ["Young", "Middle", "Senior"] for group in age_groups)

        income_tiers = [row.income_tier for row in transformed_data.collect()]
        assert all(tier in ["High", "Medium", "Low"] for tier in income_tiers)

        # Execute load stage
        load_result = job.load(transformed_data)
        assert load_result["success"] is True, "Load stage should succeed"
        assert load_result["records_loaded"] == transformed_data.count()

    def test_etl_pipeline_data_validation(self, spark_session, sample_config):
        """Test ETL pipeline with data validation checks."""
        job = MockETLJob(sample_config)
        job.spark = spark_session

        # Extract data
        raw_data = job.extract()

        # Validate input data
        assert raw_data.count() > 0, "Should have input data"

        # Check data quality before transformation
        total_records = raw_data.count()

        # Completeness check
        for column in raw_data.columns:
            null_count = raw_data.filter(col(column).isNull()).count()
            completeness = ((total_records - null_count) / total_records) * 100
            assert completeness == 100.0, f"Column {column} should be 100% complete"

        # Email format validation
        valid_emails = raw_data.filter(
            raw_data["email"].rlike(r'^[^@]+@[^@]+\.[^@]+$')
        ).count()
        assert valid_emails == total_records, "All emails should be valid format"

        # Transform data
        transformed_data = job.transform(raw_data)

        # Validate transformed data
        assert transformed_data.count() == raw_data.count(), "No records should be lost in transformation"

        # Validate derived columns
        age_group_nulls = transformed_data.filter(col("age_group").isNull()).count()
        assert age_group_nulls == 0, "Age group should not have nulls"

        income_tier_nulls = transformed_data.filter(col("income_tier").isNull()).count()
        assert income_tier_nulls == 0, "Income tier should not have nulls"

    def test_etl_pipeline_error_handling(self, spark_session, sample_config):
        """Test ETL pipeline error handling and recovery."""

        class FailingETLJob(MockETLJob):
            """ETL job that fails during transformation for testing."""

            def __init__(self, config, fail_stage=None):
                super().__init__(config)
                self.fail_stage = fail_stage

            def extract(self) -> DataFrame:
                if self.fail_stage == "extract":
                    raise Exception("Simulated extraction failure")
                return super().extract()

            def transform(self, df: DataFrame) -> DataFrame:
                if self.fail_stage == "transform":
                    raise Exception("Simulated transformation failure")
                return super().transform(df)

            def load(self, df: DataFrame) -> dict:
                if self.fail_stage == "load":
                    raise Exception("Simulated loading failure")
                return super().load(df)

        # Test extraction failure
        failing_job = FailingETLJob(sample_config, fail_stage="extract")
        failing_job.spark = spark_session

        with pytest.raises(Exception, match="Simulated extraction failure"):
            failing_job.extract()

        # Test transformation failure
        failing_job = FailingETLJob(sample_config, fail_stage="transform")
        failing_job.spark = spark_session

        raw_data = failing_job.extract()  # This should succeed
        assert raw_data.count() > 0

        with pytest.raises(Exception, match="Simulated transformation failure"):
            failing_job.transform(raw_data)

        # Test loading failure
        failing_job = FailingETLJob(sample_config, fail_stage="load")
        failing_job.spark = spark_session

        raw_data = failing_job.extract()
        transformed_data = failing_job.transform(raw_data)
        assert transformed_data.count() > 0

        with pytest.raises(Exception, match="Simulated loading failure"):
            failing_job.load(transformed_data)

    def test_etl_pipeline_with_file_io(self, spark_session, sample_config, temp_dir):
        """Test ETL pipeline with actual file I/O operations."""

        class FileBasedETLJob(MockETLJob):
            """ETL job that reads/writes actual files."""

            def __init__(self, config, temp_dir):
                super().__init__(config)
                self.temp_dir = temp_dir

            def extract(self) -> DataFrame:
                # Create test input file
                input_file = self.temp_dir / "input.csv"

                import csv
                with open(input_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["customer_id", "name", "age", "email", "income"])
                    writer.writerow(["C001", "Alice", "28", "alice@email.com", "75000.0"])
                    writer.writerow(["C002", "Bob", "35", "bob@email.com", "65000.0"])
                    writer.writerow(["C003", "Charlie", "42", "charlie@email.com", "85000.0"])

                # Read from file
                return (spark_session.read
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .csv(str(input_file)))

            def load(self, df: DataFrame) -> dict:
                # Write to file
                output_path = self.temp_dir / "output"
                df.write.mode("overwrite").parquet(str(output_path))

                # Verify file was created
                assert output_path.exists(), "Output file should be created"

                # Verify data can be read back
                saved_df = spark_session.read.parquet(str(output_path))
                record_count = saved_df.count()

                return {
                    "records_loaded": record_count,
                    "output_path": str(output_path),
                    "output_format": "parquet",
                    "success": True
                }

        # Execute file-based ETL job
        file_job = FileBasedETLJob(sample_config, temp_dir)
        file_job.spark = spark_session

        # Test extract from file
        raw_data = file_job.extract()
        assert raw_data.count() == 3, "Should read 3 records from CSV"
        assert "customer_id" in raw_data.columns, "Should have customer_id column"

        # Test transform
        transformed_data = file_job.transform(raw_data)
        assert transformed_data.count() == 3, "All records should pass transformation"

        # Test load to file
        load_result = file_job.load(transformed_data)
        assert load_result["success"] is True, "Load should succeed"
        assert load_result["records_loaded"] == 3, "Should load 3 records"

        # Verify output file exists and is readable
        output_path = Path(load_result["output_path"])
        assert output_path.exists(), "Output file should exist"

        # Read back and verify data
        saved_df = spark_session.read.parquet(str(output_path))
        assert saved_df.count() == 3, "Saved file should contain 3 records"
        assert "age_group" in saved_df.columns, "Saved data should include derived columns"

    def test_etl_pipeline_performance_metrics(self, spark_session, sample_config):
        """Test ETL pipeline performance metrics collection."""
        import time

        class MetricsETLJob(MockETLJob):
            """ETL job that collects detailed performance metrics."""

            def __init__(self, config):
                super().__init__(config)
                self.metrics = {
                    "extract_time": 0,
                    "transform_time": 0,
                    "load_time": 0,
                    "total_time": 0,
                    "records_processed": 0
                }

            def extract(self) -> DataFrame:
                start_time = time.time()
                df = super().extract()
                self.metrics["extract_time"] = time.time() - start_time
                return df

            def transform(self, df: DataFrame) -> DataFrame:
                start_time = time.time()
                result = super().transform(df)
                self.metrics["transform_time"] = time.time() - start_time
                self.metrics["records_processed"] = result.count()
                return result

            def load(self, df: DataFrame) -> dict:
                start_time = time.time()
                result = super().load(df)
                self.metrics["load_time"] = time.time() - start_time
                return result

            def run_with_metrics(self):
                """Run ETL job and collect comprehensive metrics."""
                total_start = time.time()

                # Execute pipeline
                raw_data = self.extract()
                transformed_data = self.transform(raw_data)
                load_result = self.load(transformed_data)

                self.metrics["total_time"] = time.time() - total_start

                return {
                    "success": True,
                    "metrics": self.metrics,
                    "load_result": load_result
                }

        # Execute job with metrics
        metrics_job = MetricsETLJob(sample_config)
        metrics_job.spark = spark_session

        result = metrics_job.run_with_metrics()

        # Verify metrics were collected
        assert result["success"] is True
        metrics = result["metrics"]

        assert metrics["extract_time"] > 0, "Extract time should be recorded"
        assert metrics["transform_time"] > 0, "Transform time should be recorded"
        assert metrics["load_time"] > 0, "Load time should be recorded"
        assert metrics["total_time"] > 0, "Total time should be recorded"
        assert metrics["records_processed"] > 0, "Records processed should be recorded"

        # Verify time relationships
        sum_of_stages = (metrics["extract_time"] +
                        metrics["transform_time"] +
                        metrics["load_time"])
        assert metrics["total_time"] >= sum_of_stages, "Total time should be >= sum of stage times"

    def test_etl_pipeline_data_lineage(self, spark_session, sample_config):
        """Test ETL pipeline data lineage tracking."""

        class LineageETLJob(MockETLJob):
            """ETL job that tracks data lineage."""

            def __init__(self, config):
                super().__init__(config)
                self.lineage = {
                    "source_info": {},
                    "transformation_steps": [],
                    "output_info": {}
                }

            def extract(self) -> DataFrame:
                df = super().extract()

                # Record source information
                self.lineage["source_info"] = {
                    "source_type": "mock_data",
                    "record_count": df.count(),
                    "columns": df.columns,
                    "schema": str(df.schema)
                }

                return df

            def transform(self, df: DataFrame) -> DataFrame:
                # Record transformation steps
                initial_count = df.count()

                # Step 1: Filter by age
                df_filtered = df.filter(col("age") >= 18)
                self.lineage["transformation_steps"].append({
                    "step": "age_filter",
                    "input_records": initial_count,
                    "output_records": df_filtered.count(),
                    "operation": "filter age >= 18"
                })

                # Step 2: Add derived columns
                df_enriched = super().transform(df_filtered)
                self.lineage["transformation_steps"].append({
                    "step": "enrichment",
                    "input_records": df_filtered.count(),
                    "output_records": df_enriched.count(),
                    "operation": "add age_group and income_tier columns"
                })

                return df_enriched

            def load(self, df: DataFrame) -> dict:
                result = super().load(df)

                # Record output information
                self.lineage["output_info"] = {
                    "record_count": df.count(),
                    "columns": df.columns,
                    "output_format": result.get("output_format", "unknown")
                }

                return result

        # Execute job with lineage tracking
        lineage_job = LineageETLJob(sample_config)
        lineage_job.spark = spark_session

        raw_data = lineage_job.extract()
        transformed_data = lineage_job.transform(raw_data)
        load_result = lineage_job.load(transformed_data)

        # Verify lineage information was captured
        lineage = lineage_job.lineage

        # Check source info
        assert lineage["source_info"]["record_count"] > 0
        assert "customer_id" in lineage["source_info"]["columns"]

        # Check transformation steps
        assert len(lineage["transformation_steps"]) == 2
        assert lineage["transformation_steps"][0]["step"] == "age_filter"
        assert lineage["transformation_steps"][1]["step"] == "enrichment"

        # Check output info
        assert lineage["output_info"]["record_count"] > 0
        assert "age_group" in lineage["output_info"]["columns"]
        assert "income_tier" in lineage["output_info"]["columns"]

        # Verify data consistency through pipeline
        source_count = lineage["source_info"]["record_count"]
        final_count = lineage["output_info"]["record_count"]
        assert final_count <= source_count, "Output records should not exceed input records"