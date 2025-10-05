"""
Schema definitions for data quality and monitoring structures.
This module provides Spark SQL schemas for data quality reports and job monitoring.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


class DataQualitySchema:
    """
    Schema definitions for data quality and monitoring structures.

    This class provides schema definitions for:
    - Data quality reports
    - Data lineage tracking
    - Job execution metrics
    """

    @staticmethod
    def get_data_quality_report_schema() -> StructType:
        """
        Get the schema for data quality reports.

        Returns:
            StructType schema for data quality reports
        """
        return StructType(
            [
                StructField("dataset_name", StringType(), False),
                StructField("table_name", StringType(), False),
                StructField("column_name", StringType(), False),
                StructField("quality_check", StringType(), False),
                StructField("check_result", StringType(), False),
                StructField("record_count", LongType(), False),
                StructField("null_count", LongType(), False),
                StructField("null_percentage", DoubleType(), False),
                StructField("check_timestamp", TimestampType(), False),
            ]
        )

    @staticmethod
    def get_job_metrics_schema() -> StructType:
        """
        Get the schema for job execution metrics.

        Returns:
            StructType schema for job metrics
        """
        return StructType(
            [
                StructField("job_name", StringType(), False),
                StructField("job_id", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("end_time", TimestampType(), True),
                StructField("duration_seconds", LongType(), True),
                StructField("status", StringType(), False),
                StructField("records_processed", LongType(), True),
                StructField("records_output", LongType(), True),
                StructField("error_message", StringType(), True),
                StructField("environment", StringType(), False),
            ]
        )

    @staticmethod
    def get_data_lineage_schema() -> StructType:
        """
        Get the schema for data lineage tracking.

        Returns:
            StructType schema for data lineage
        """
        return StructType(
            [
                StructField("lineage_id", StringType(), False),
                StructField("source_table", StringType(), False),
                StructField("target_table", StringType(), False),
                StructField("transformation_type", StringType(), False),
                StructField("job_name", StringType(), False),
                StructField("execution_timestamp", TimestampType(), False),
                StructField("source_record_count", LongType(), True),
                StructField("target_record_count", LongType(), True),
                StructField("transformation_logic", StringType(), True),
            ]
        )
