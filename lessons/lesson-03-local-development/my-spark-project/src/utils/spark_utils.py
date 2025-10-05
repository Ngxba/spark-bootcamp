"""
Spark utility functions for common operations and optimizations.
This module provides helper functions to improve Spark job performance and maintainability.
"""

from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import StructType
import logging


class SparkUtils:
    """
    Utility class for Spark operations and optimizations.

    This class provides:
    - DataFrame operations
    - Performance optimizations
    - Spark configuration utilities
    - Debugging helpers
    """

    @staticmethod
    def create_optimized_spark_session(
        app_name: str, master: str = "local[*]", config: Optional[Dict[str, str]] = None
    ) -> SparkSession:
        """
        Create an optimized Spark session with performance configurations.

        Args:
            app_name: Name of the Spark application
            master: Spark master URL
            config: Additional Spark configurations

        Returns:
            Configured SparkSession
        """
        builder = SparkSession.builder.appName(app_name).master(master)

        # Default optimized configurations
        default_config = {
            # Adaptive Query Execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            # Memory Management
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.executor.memoryFraction": "0.8",
            # Serialization
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            # Arrow optimization for Pandas
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            # Warehouse directory
            "spark.sql.warehouse.dir": "./spark-warehouse",
            # UI settings
            "spark.ui.showConsoleProgress": "true",
            # Shuffle optimization
            "spark.sql.shuffle.partitions": "200",
        }

        # Apply default configurations
        for key, value in default_config.items():
            builder = builder.config(key, value)

        # Apply custom configurations
        if config:
            for key, value in config.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        return spark

    @staticmethod
    def optimize_dataframe_partitions(
        df: DataFrame, target_partition_size_mb: int = 128
    ) -> DataFrame:
        """
        Optimize DataFrame partitions based on data size.

        Args:
            df: Input DataFrame
            target_partition_size_mb: Target partition size in MB

        Returns:
            Repartitioned DataFrame
        """
        # Estimate DataFrame size (rough calculation)
        row_count = df.count()
        if row_count == 0:
            return df

        # Estimate size per row (rough calculation based on schema)
        estimated_row_size = len(df.schema) * 50  # Rough estimate: 50 bytes per field
        estimated_size_mb = (row_count * estimated_row_size) / (1024 * 1024)

        # Calculate optimal partitions
        optimal_partitions = max(1, int(estimated_size_mb / target_partition_size_mb))

        current_partitions = df.rdd.getNumPartitions()

        if optimal_partitions != current_partitions:
            if optimal_partitions < current_partitions:
                return df.coalesce(optimal_partitions)
            else:
                return df.repartition(optimal_partitions)

        return df

    @staticmethod
    def broadcast_small_dataframe(df: DataFrame, threshold_mb: int = 200) -> DataFrame:
        """
        Broadcast DataFrame if it's smaller than threshold.

        Args:
            df: DataFrame to potentially broadcast
            threshold_mb: Size threshold in MB for broadcasting

        Returns:
            Potentially broadcasted DataFrame
        """
        # For demo purposes, we'll broadcast based on partition count
        # In production, you'd want to estimate actual size
        partition_count = df.rdd.getNumPartitions()

        if partition_count <= 2:  # Assume small DataFrames have few partitions
            return broadcast(df)

        return df

    @staticmethod
    def analyze_dataframe(df: DataFrame, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Analyze DataFrame to get insights about data distribution and quality.

        Args:
            df: DataFrame to analyze
            sample_size: Number of rows to sample for analysis

        Returns:
            Dictionary with analysis results
        """
        analysis = {
            "total_rows": df.count(),
            "total_columns": len(df.columns),
            "partition_count": df.rdd.getNumPartitions(),
            "schema": df.schema.json(),
            "columns": df.columns,
        }

        # Sample data for further analysis
        sample_df = df.limit(sample_size)

        # Null analysis
        null_counts = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = {
                "null_count": null_count,
                "null_percentage": (
                    (null_count / analysis["total_rows"]) * 100
                    if analysis["total_rows"] > 0
                    else 0
                ),
            }

        analysis["null_analysis"] = null_counts

        return analysis

    @staticmethod
    def write_dataframe_optimized(
        df: DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        coalesce_partitions: int = 1,
    ) -> None:
        """
        Write DataFrame with optimizations.

        Args:
            df: DataFrame to write
            path: Output path
            format: Output format (parquet, delta, json, csv)
            mode: Write mode (overwrite, append, etc.)
            partition_by: Columns to partition by
            coalesce_partitions: Number of files to write
        """
        writer = df.coalesce(coalesce_partitions).write.mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if format.lower() == "parquet":
            writer.parquet(path)
        elif format.lower() == "csv":
            writer.option("header", "true").csv(path)
        elif format.lower() == "json":
            writer.json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    @staticmethod
    def join_dataframes_optimized(
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        join_type: str = "inner",
        broadcast_threshold: int = 200,
    ) -> DataFrame:
        """
        Perform optimized DataFrame joins.

        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: List of join keys
            join_type: Type of join (inner, left, right, outer)
            broadcast_threshold: Threshold for broadcasting smaller DataFrame

        Returns:
            Joined DataFrame
        """
        # Determine which DataFrame to potentially broadcast
        left_partitions = left_df.rdd.getNumPartitions()
        right_partitions = right_df.rdd.getNumPartitions()

        # Simple heuristic: broadcast the DataFrame with fewer partitions
        if left_partitions <= 2 and left_partitions < right_partitions:
            left_df = broadcast(left_df)
        elif right_partitions <= 2 and right_partitions < left_partitions:
            right_df = broadcast(right_df)

        # Perform join
        if len(join_keys) == 1:
            return left_df.join(right_df, join_keys[0], join_type)
        else:
            return left_df.join(right_df, join_keys, join_type)

    @staticmethod
    def cache_if_reused(
        df: DataFrame, storage_level: str = "MEMORY_AND_DISK"
    ) -> DataFrame:
        """
        Cache DataFrame if it will be reused multiple times.

        Args:
            df: DataFrame to cache
            storage_level: Storage level for caching

        Returns:
            Cached DataFrame
        """
        from pyspark import StorageLevel

        storage_levels = {
            "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
            "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
            "DISK_ONLY": StorageLevel.DISK_ONLY,
            "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY_SER,
            "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER,
        }

        level = storage_levels.get(storage_level, StorageLevel.MEMORY_AND_DISK)
        return df.persist(level)

    @staticmethod
    def get_spark_ui_url(spark: SparkSession) -> str:
        """
        Get Spark UI URL for monitoring.

        Args:
            spark: SparkSession

        Returns:
            Spark UI URL
        """
        app_id = spark.sparkContext.applicationId
        ui_web_url = spark.sparkContext.uiWebUrl
        return f"{ui_web_url}" if ui_web_url else f"http://localhost:4040"

    @staticmethod
    def log_dataframe_info(df: DataFrame, name: str, logger: logging.Logger) -> None:
        """
        Log useful information about a DataFrame.

        Args:
            df: DataFrame to log information about
            name: Name to use in log messages
            logger: Logger instance
        """
        try:
            row_count = df.count()
            partition_count = df.rdd.getNumPartitions()
            columns = df.columns

            logger.info(f"DataFrame '{name}' Info:")
            logger.info(f"  - Rows: {row_count:,}")
            logger.info(f"  - Columns: {len(columns)}")
            logger.info(f"  - Partitions: {partition_count}")
            logger.info(f"  - Column names: {', '.join(columns)}")

        except Exception as e:
            logger.error(f"Failed to log DataFrame info for '{name}': {str(e)}")

    @staticmethod
    def validate_dataframe_schema(
        df: DataFrame, expected_schema: StructType
    ) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema

        Returns:
            Tuple of (is_valid, list_of_differences)
        """
        differences = []
        actual_schema = df.schema

        # Check field count
        if len(actual_schema.fields) != len(expected_schema.fields):
            differences.append(
                f"Field count mismatch: expected {len(expected_schema.fields)}, "
                f"got {len(actual_schema.fields)}"
            )

        # Check individual fields
        expected_fields = {field.name: field for field in expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}

        for field_name, expected_field in expected_fields.items():
            if field_name not in actual_fields:
                differences.append(f"Missing field: {field_name}")
            else:
                actual_field = actual_fields[field_name]
                if actual_field.dataType != expected_field.dataType:
                    differences.append(
                        f"Data type mismatch for field '{field_name}': "
                        f"expected {expected_field.dataType}, got {actual_field.dataType}"
                    )

        # Check for extra fields
        for field_name in actual_fields:
            if field_name not in expected_fields:
                differences.append(f"Unexpected field: {field_name}")

        return len(differences) == 0, differences
