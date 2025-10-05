"""
Base Spark job class providing common functionality for all Spark applications.
This module demonstrates industry-standard patterns for Spark job architecture.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class BaseSparkJob(ABC):
    """
    Abstract base class for all Spark jobs.

    This class provides:
    - Spark session management
    - Configuration handling
    - Logging setup
    - Common job lifecycle methods
    """

    def __init__(self, app_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Spark job.

        Args:
            app_name: Name of the Spark application
            config: Optional configuration dictionary
        """
        self.app_name = app_name
        self.config = config or {}
        self.spark = None
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Set up logging for the job."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        return logging.getLogger(self.__class__.__name__)

    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.

        Returns:
            Configured SparkSession
        """
        spark_conf = SparkConf()

        # Set default configurations
        default_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.warehouse.dir": "./spark-warehouse",
        }

        # Apply default configurations
        for key, value in default_configs.items():
            spark_conf.set(key, value)

        # Apply custom configurations
        for key, value in self.config.get("spark_config", {}).items():
            spark_conf.set(key, value)

        self.logger.info(f"Creating Spark session for application: {self.app_name}")

        spark = (
            SparkSession.builder.appName(self.app_name)
            .config(conf=spark_conf)
            .getOrCreate()
        )

        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")

        return spark

    def initialize(self):
        """Initialize the Spark job."""
        self.spark = self._create_spark_session()
        self.logger.info(f"Spark session initialized. Version: {self.spark.version}")

    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.logger.info("Stopping Spark session")
            self.spark.stop()
            self.spark = None

    @abstractmethod
    def run(self) -> None:
        """
        Main job execution logic.
        Must be implemented by subclasses.
        """
        pass

    def execute(self) -> None:
        """
        Execute the complete job lifecycle.

        This method handles:
        1. Job initialization
        2. Job execution
        3. Cleanup
        """
        try:
            self.logger.info(f"Starting job: {self.app_name}")
            self.initialize()
            self.run()
            self.logger.info(f"Job completed successfully: {self.app_name}")
        except Exception as e:
            self.logger.error(f"Job failed: {self.app_name}, Error: {str(e)}")
            raise
        finally:
            self.cleanup()

    def get_spark_config_info(self) -> Dict[str, str]:
        """
        Get current Spark configuration information.

        Returns:
            Dictionary of Spark configuration
        """
        if not self.spark:
            return {}

        return {
            "app_name": self.spark.sparkContext.appName,
            "spark_version": self.spark.version,
            "master": self.spark.sparkContext.master,
            "executor_memory": self.spark.conf.get("spark.executor.memory", "1g"),
            "driver_memory": self.spark.conf.get("spark.driver.memory", "1g"),
        }
