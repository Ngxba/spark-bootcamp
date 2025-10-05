"""
Configuration management for the Spark project.
This module handles loading and managing application configuration from multiple sources.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from ..utils.file_utils import FileUtils


@dataclass
class SparkConfig:
    """Configuration for Spark session settings."""

    app_name: str = "MySparkProject"
    master: str = "local[*]"
    executor_memory: str = "2g"
    driver_memory: str = "1g"
    max_result_size: str = "1g"
    adaptive_enabled: bool = True
    adaptive_coalesce_partitions: bool = True
    arrow_enabled: bool = True
    warehouse_dir: str = "./spark-warehouse"
    ui_show_console_progress: bool = True
    log_level: str = "WARN"

    def to_dict(self) -> Dict[str, str]:
        """Convert configuration to Spark config dictionary."""
        return {
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.driver.maxResultSize": self.max_result_size,
            "spark.sql.adaptive.enabled": str(self.adaptive_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(
                self.adaptive_coalesce_partitions
            ).lower(),
            "spark.sql.execution.arrow.pyspark.enabled": str(
                self.arrow_enabled
            ).lower(),
            "spark.sql.warehouse.dir": self.warehouse_dir,
            "spark.ui.showConsoleProgress": str(self.ui_show_console_progress).lower(),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }


@dataclass
class DataConfig:
    """Configuration for data sources and outputs."""

    input_base_path: str = "data"
    output_base_path: str = "data/output"
    checkpoint_path: str = "data/checkpoints"
    customers_file: str = "customers.csv"
    products_file: str = "products.csv"
    sales_file: str = "sales.csv"
    file_format: str = "csv"
    output_format: str = "parquet"
    partition_columns: list = field(default_factory=list)
    compression: str = "snappy"

    @property
    def customers_path(self) -> str:
        """Get full path to customers file."""
        return FileUtils.join_paths(self.input_base_path, self.customers_file)

    @property
    def products_path(self) -> str:
        """Get full path to products file."""
        return FileUtils.join_paths(self.input_base_path, self.products_file)

    @property
    def sales_path(self) -> str:
        """Get full path to sales file."""
        return FileUtils.join_paths(self.input_base_path, self.sales_file)


@dataclass
class ProcessingConfig:
    """Configuration for data processing settings."""

    enable_data_quality_checks: bool = True
    enable_caching: bool = True
    cache_storage_level: str = "MEMORY_AND_DISK"
    broadcast_threshold_mb: int = 200
    target_partition_size_mb: int = 128
    enable_adaptive_query_execution: bool = True
    shuffle_partitions: int = 200
    max_records_per_file: int = 1000000


@dataclass
class AppConfig:
    """Main application configuration."""

    environment: str = "development"
    debug_mode: bool = True
    log_level: str = "INFO"
    spark: SparkConfig = field(default_factory=SparkConfig)
    data: DataConfig = field(default_factory=DataConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "AppConfig":
        """Create AppConfig from dictionary."""
        spark_config = SparkConfig(**config_dict.get("spark", {}))
        data_config = DataConfig(**config_dict.get("data", {}))
        processing_config = ProcessingConfig(**config_dict.get("processing", {}))

        return cls(
            environment=config_dict.get("environment", "development"),
            debug_mode=config_dict.get("debug_mode", True),
            log_level=config_dict.get("log_level", "INFO"),
            spark=spark_config,
            data=data_config,
            processing=processing_config,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "environment": self.environment,
            "debug_mode": self.debug_mode,
            "log_level": self.log_level,
            "spark": self.spark.__dict__,
            "data": self.data.__dict__,
            "processing": self.processing.__dict__,
        }


class ConfigManager:
    """
    Configuration manager for loading and managing application settings.

    This class handles:
    - Loading configuration from YAML files
    - Environment-specific configuration
    - Environment variable overrides
    - Configuration validation
    """

    def __init__(self, config_dir: str = "config", environment: Optional[str] = None):
        """
        Initialize configuration manager.

        Args:
            config_dir: Directory containing configuration files
            environment: Environment name (dev, staging, prod)
        """
        self.config_dir = config_dir
        self.environment = environment or os.getenv("ENVIRONMENT", "development")
        self._config: Optional[AppConfig] = None

    def load_config(self) -> AppConfig:
        """
        Load configuration from files and environment variables.

        Returns:
            AppConfig instance
        """
        if self._config is not None:
            return self._config

        # Start with default configuration
        config_dict = self._get_default_config()

        # Load base configuration file
        base_config_path = FileUtils.join_paths(self.config_dir, "base.yaml")
        if FileUtils.file_exists(base_config_path):
            base_config = FileUtils.read_yaml_file(base_config_path)
            config_dict = self._merge_configs(config_dict, base_config)

        # Load environment-specific configuration
        env_config_path = FileUtils.join_paths(
            self.config_dir, f"{self.environment}.yaml"
        )
        if FileUtils.file_exists(env_config_path):
            env_config = FileUtils.read_yaml_file(env_config_path)
            config_dict = self._merge_configs(config_dict, env_config)

        # Apply environment variable overrides
        config_dict = self._apply_env_overrides(config_dict)

        # Create and cache AppConfig
        self._config = AppConfig.from_dict(config_dict)
        return self._config

    def get_config(self) -> AppConfig:
        """
        Get the current configuration.

        Returns:
            AppConfig instance
        """
        if self._config is None:
            return self.load_config()
        return self._config

    def reload_config(self) -> AppConfig:
        """
        Reload configuration from files.

        Returns:
            Updated AppConfig instance
        """
        self._config = None
        return self.load_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "environment": "development",
            "debug_mode": True,
            "log_level": "INFO",
            "spark": {
                "app_name": "MySparkProject",
                "master": "local[*]",
                "executor_memory": "2g",
                "driver_memory": "1g",
            },
            "data": {
                "input_base_path": "data",
                "output_base_path": "data/output",
                "file_format": "csv",
                "output_format": "parquet",
            },
            "processing": {
                "enable_data_quality_checks": True,
                "enable_caching": True,
                "target_partition_size_mb": 128,
            },
        }

    def _merge_configs(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Recursively merge configuration dictionaries.

        Args:
            base: Base configuration
            override: Override configuration

        Returns:
            Merged configuration
        """
        result = base.copy()

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value

        return result

    def _apply_env_overrides(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides to configuration.

        Args:
            config_dict: Configuration dictionary

        Returns:
            Configuration with environment overrides applied
        """
        # Environment variable mappings
        env_mappings = {
            "SPARK_MASTER": ["spark", "master"],
            "SPARK_EXECUTOR_MEMORY": ["spark", "executor_memory"],
            "SPARK_DRIVER_MEMORY": ["spark", "driver_memory"],
            "DATA_INPUT_PATH": ["data", "input_base_path"],
            "DATA_OUTPUT_PATH": ["data", "output_base_path"],
            "LOG_LEVEL": ["log_level"],
            "DEBUG_MODE": ["debug_mode"],
            "ENVIRONMENT": ["environment"],
        }

        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Navigate to the correct nested dictionary
                current_dict = config_dict
                for key in config_path[:-1]:
                    if key not in current_dict:
                        current_dict[key] = {}
                    current_dict = current_dict[key]

                # Set the value with proper type conversion
                final_key = config_path[-1]
                if final_key == "debug_mode":
                    current_dict[final_key] = env_value.lower() in (
                        "true",
                        "1",
                        "yes",
                        "on",
                    )
                else:
                    current_dict[final_key] = env_value

        return config_dict

    def save_config(self, file_path: str) -> None:
        """
        Save current configuration to a YAML file.

        Args:
            file_path: Path to save configuration file
        """
        if self._config is None:
            self.load_config()

        config_dict = self._config.to_dict()
        FileUtils.write_yaml_file(config_dict, file_path)

    def get_spark_config(self) -> Dict[str, str]:
        """
        Get Spark configuration dictionary.

        Returns:
            Spark configuration for SparkSession
        """
        config = self.get_config()
        return config.spark.to_dict()

    def validate_config(self) -> bool:
        """
        Validate the current configuration.

        Returns:
            True if configuration is valid
        """
        try:
            config = self.get_config()

            # Validate required paths exist
            data_config = config.data
            if not FileUtils.directory_exists(data_config.input_base_path):
                raise ValueError(
                    f"Input directory does not exist: {data_config.input_base_path}"
                )

            # Validate Spark configuration values
            spark_config = config.spark
            if not spark_config.master:
                raise ValueError("Spark master must be specified")

            return True

        except Exception as e:
            print(f"Configuration validation failed: {str(e)}")
            return False


# Global configuration manager instance
config_manager = ConfigManager()


def get_config() -> AppConfig:
    """
    Get the global application configuration.

    Returns:
        AppConfig instance
    """
    return config_manager.get_config()


def get_spark_config() -> Dict[str, str]:
    """
    Get Spark configuration dictionary.

    Returns:
        Spark configuration for SparkSession
    """
    return config_manager.get_spark_config()
