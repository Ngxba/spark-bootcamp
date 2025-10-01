"""
Base ETL job class providing the foundation for all ETL operations.
"""

import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from etl_pipeline.utils.spark import create_spark_session
from etl_pipeline.utils.logging import setup_logging
from etl_pipeline.utils.metrics import MetricsCollector

import structlog

logger = structlog.get_logger(__name__)


class BaseETLJob(ABC):
    """
    Abstract base class for all ETL jobs.

    Provides common functionality including:
    - Spark session management
    - Configuration handling
    - Error handling and retry logic
    - Metrics collection
    - Logging
    """

    def __init__(self, config: Dict[str, Any], metrics: Optional[MetricsCollector] = None):
        """
        Initialize the ETL job.

        Args:
            config: Job configuration dictionary
            metrics: Optional metrics collector instance
        """
        self.config = config
        self.metrics = metrics or MetricsCollector()
        self.spark: Optional[SparkSession] = None
        self.job_id = f"{self.__class__.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Setup logging
        setup_logging(config.get("logging", {}).get("level", "INFO"))

        logger.info("ETL job initialized",
                   job_class=self.__class__.__name__,
                   job_id=self.job_id)

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.

        Returns:
            Dictionary containing execution results and metrics
        """
        start_time = time.time()

        try:
            logger.info("Starting ETL job execution", job_id=self.job_id)

            # Initialize Spark session
            self._initialize_spark()

            # Execute ETL pipeline
            result = self._execute_pipeline()

            end_time = time.time()
            duration = end_time - start_time

            # Collect final metrics
            final_result = {
                "success": True,
                "job_id": self.job_id,
                "duration": duration,
                "start_time": datetime.fromtimestamp(start_time).isoformat(),
                "end_time": datetime.fromtimestamp(end_time).isoformat(),
                **result
            }

            self.metrics.record("job_duration", duration)
            self.metrics.record("job_success", 1)

            logger.info("ETL job completed successfully",
                       job_id=self.job_id,
                       duration=duration,
                       records_processed=result.get("records_processed", 0))

            return final_result

        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time

            self.metrics.record("job_duration", duration)
            self.metrics.record("job_failure", 1)

            logger.error("ETL job failed",
                        job_id=self.job_id,
                        error=str(e),
                        duration=duration,
                        exc_info=True)

            return {
                "success": False,
                "job_id": self.job_id,
                "duration": duration,
                "error": str(e),
                "start_time": datetime.fromtimestamp(start_time).isoformat(),
                "end_time": datetime.fromtimestamp(end_time).isoformat(),
            }

        finally:
            self._cleanup()

    def _initialize_spark(self) -> None:
        """Initialize Spark session with job-specific configuration."""
        try:
            self.spark = create_spark_session(self.config)

            # Set additional Spark configurations specific to this job
            spark_config = self._get_spark_config()
            for key, value in spark_config.items():
                self.spark.conf.set(key, value)

            logger.info("Spark session initialized",
                       app_name=self.spark.sparkContext.appName,
                       master=self.spark.sparkContext.master)

        except Exception as e:
            logger.error("Failed to initialize Spark session", error=str(e))
            raise

    def _get_spark_config(self) -> Dict[str, str]:
        """
        Get job-specific Spark configuration.

        Returns:
            Dictionary of Spark configuration parameters
        """
        return {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
        }

    def _execute_pipeline(self) -> Dict[str, Any]:
        """
        Execute the ETL pipeline stages.

        Returns:
            Dictionary containing execution results
        """
        results = {}

        # Extract stage
        logger.info("Starting extraction stage", job_id=self.job_id)
        extract_start = time.time()

        raw_data = self.extract()
        extract_count = raw_data.count()

        extract_duration = time.time() - extract_start
        self.metrics.record("extract_duration", extract_duration)
        self.metrics.record("extract_records", extract_count)

        logger.info("Extraction completed",
                   records_extracted=extract_count,
                   duration=extract_duration)

        results["extract"] = {
            "records": extract_count,
            "duration": extract_duration
        }

        # Transform stage
        logger.info("Starting transformation stage", job_id=self.job_id)
        transform_start = time.time()

        transformed_data = self.transform(raw_data)
        transform_count = transformed_data.count()

        transform_duration = time.time() - transform_start
        self.metrics.record("transform_duration", transform_duration)
        self.metrics.record("transform_records", transform_count)

        logger.info("Transformation completed",
                   records_transformed=transform_count,
                   duration=transform_duration)

        results["transform"] = {
            "records": transform_count,
            "duration": transform_duration
        }

        # Load stage
        logger.info("Starting loading stage", job_id=self.job_id)
        load_start = time.time()

        load_result = self.load(transformed_data)

        load_duration = time.time() - load_start
        self.metrics.record("load_duration", load_duration)

        logger.info("Loading completed",
                   duration=load_duration,
                   result=load_result)

        results["load"] = {
            "duration": load_duration,
            **load_result
        }

        # Overall results
        results["records_processed"] = extract_count

        return results

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source systems.

        Returns:
            Raw DataFrame containing extracted data
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform the extracted data.

        Args:
            df: Raw DataFrame from extraction stage

        Returns:
            Transformed DataFrame ready for loading
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> Dict[str, Any]:
        """
        Load transformed data to target systems.

        Args:
            df: Transformed DataFrame from transformation stage

        Returns:
            Dictionary containing load operation results
        """
        pass

    def _cleanup(self) -> None:
        """Clean up resources after job execution."""
        if self.spark:
            try:
                # Clear cache to free up memory
                self.spark.catalog.clearCache()

                # Stop Spark session
                self.spark.stop()

                logger.info("Spark session stopped", job_id=self.job_id)

            except Exception as e:
                logger.warning("Error during cleanup", error=str(e))

    def validate_input(self, df: DataFrame, stage: str = "input") -> bool:
        """
        Validate input data quality.

        Args:
            df: DataFrame to validate
            stage: Stage name for logging

        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Basic validation
            if df.count() == 0:
                logger.warning("Empty dataset detected", stage=stage)
                return False

            # Schema validation
            if len(df.columns) == 0:
                logger.error("Dataset has no columns", stage=stage)
                return False

            # Check for all null columns
            total_rows = df.count()
            for column in df.columns:
                null_count = df.filter(df[column].isNull()).count()
                null_percentage = (null_count / total_rows) * 100

                if null_percentage == 100:
                    logger.error("Column is completely null",
                                column=column,
                                stage=stage)
                    return False
                elif null_percentage > 50:
                    logger.warning("High null percentage in column",
                                  column=column,
                                  null_percentage=null_percentage,
                                  stage=stage)

            logger.info("Input validation passed",
                       stage=stage,
                       rows=total_rows,
                       columns=len(df.columns))

            return True

        except Exception as e:
            logger.error("Input validation failed",
                        stage=stage,
                        error=str(e))
            return False

    def checkpoint_data(self, df: DataFrame, checkpoint_name: str) -> DataFrame:
        """
        Checkpoint DataFrame to improve fault tolerance.

        Args:
            df: DataFrame to checkpoint
            checkpoint_name: Name for the checkpoint

        Returns:
            Checkpointed DataFrame
        """
        try:
            checkpoint_path = self.config.get("data", {}).get("checkpoint_path")

            if checkpoint_path:
                full_checkpoint_path = f"{checkpoint_path}/{self.job_id}/{checkpoint_name}"

                # Cache the DataFrame
                df.cache()

                # Write checkpoint
                df.write.mode("overwrite").parquet(full_checkpoint_path)

                logger.info("Data checkpointed",
                           checkpoint_name=checkpoint_name,
                           path=full_checkpoint_path,
                           records=df.count())

            return df

        except Exception as e:
            logger.warning("Failed to checkpoint data",
                          checkpoint_name=checkpoint_name,
                          error=str(e))
            return df