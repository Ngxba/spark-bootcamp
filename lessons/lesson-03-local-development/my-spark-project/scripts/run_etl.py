#!/usr/bin/env python3
"""
ETL Pipeline Runner Script

This script demonstrates how to run the ETL pipeline with different configurations.
It shows best practices for production job execution including:
- Configuration management
- Error handling
- Logging
- Performance monitoring
"""

import sys
import os
import argparse
import logging
import time
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.jobs.etl_job import ETLJob
from src.config.settings import ConfigManager
from src.utils.file_utils import FileUtils


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Set up logging configuration.

    Args:
        log_level: Logging level

    Returns:
        Configured logger
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("etl_pipeline.log", mode="a"),
        ],
    )
    return logging.getLogger(__name__)


def create_sample_data(data_dir: str) -> None:
    """
    Create sample data files for demonstration.

    Args:
        data_dir: Directory to create data files in
    """
    FileUtils.create_directory(data_dir)

    # Sample customers data
    customers_data = """customer_id,customer_name,age,gender,city,customer_segment
CUST001,John Doe,35,Male,New York,PREMIUM
CUST002,Jane Smith,28,Female,Los Angeles,STANDARD
CUST003,Bob Johnson,42,Male,Chicago,PREMIUM
CUST004,Alice Brown,31,Female,Houston,STANDARD
CUST005,Charlie Davis,26,Male,Phoenix,BASIC"""

    # Sample products data
    products_data = """product_id,product_name,category,brand,price,supplier
PROD001,Laptop Pro,Electronics,TechBrand,999.99,Supplier A
PROD002,Wireless Mouse,Electronics,TechBrand,29.99,Supplier B
PROD003,Coffee Mug,Home,HomeGoods,12.50,Supplier C
PROD004,Notebook,Office,OfficeSupply,4.99,Supplier A
PROD005,Desk Chair,Furniture,FurnitureCo,199.99,Supplier D"""

    # Sample sales data
    sales_data = """transaction_id,customer_id,product_id,quantity,unit_price,total_amount,transaction_date
TXN001,CUST001,PROD001,1,999.99,999.99,2024-01-15
TXN002,CUST002,PROD002,2,29.99,59.98,2024-01-16
TXN003,CUST001,PROD003,3,12.50,37.50,2024-01-17
TXN004,CUST003,PROD001,1,999.99,999.99,2024-01-18
TXN005,CUST002,PROD004,5,4.99,24.95,2024-01-19
TXN006,CUST004,PROD005,1,199.99,199.99,2024-01-20
TXN007,CUST005,PROD002,1,29.99,29.99,2024-01-21
TXN008,CUST003,PROD003,2,12.50,25.00,2024-01-22
TXN009,CUST001,PROD004,10,4.99,49.90,2024-01-23
TXN010,CUST004,PROD002,1,29.99,29.99,2024-01-24"""

    # Write data files
    FileUtils.write_text_file(customers_data, os.path.join(data_dir, "customers.csv"))
    FileUtils.write_text_file(products_data, os.path.join(data_dir, "products.csv"))
    FileUtils.write_text_file(sales_data, os.path.join(data_dir, "sales.csv"))


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Run ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_etl.py                           # Run with default settings
  python scripts/run_etl.py --env production         # Run in production mode
  python scripts/run_etl.py --data-dir /path/to/data # Use custom data directory
  python scripts/run_etl.py --create-sample-data     # Create sample data first
  python scripts/run_etl.py --log-level DEBUG        # Enable debug logging
        """,
    )

    parser.add_argument(
        "--env",
        default="development",
        help="Environment to run in (development, staging, production)",
    )

    parser.add_argument(
        "--data-dir", default="data", help="Directory containing input data files"
    )

    parser.add_argument(
        "--output-dir", default="data/output", help="Directory for output files"
    )

    parser.add_argument(
        "--create-sample-data",
        action="store_true",
        help="Create sample data files before running ETL",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    parser.add_argument(
        "--config-dir",
        default="config",
        help="Directory containing configuration files",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and data without running ETL",
    )

    return parser.parse_args()


def validate_data_files(data_dir: str, logger: logging.Logger) -> bool:
    """
    Validate that required data files exist.

    Args:
        data_dir: Data directory path
        logger: Logger instance

    Returns:
        True if all required files exist
    """
    required_files = ["customers.csv", "products.csv", "sales.csv"]
    missing_files = []

    for file_name in required_files:
        file_path = os.path.join(data_dir, file_name)
        if not FileUtils.file_exists(file_path):
            missing_files.append(file_name)

    if missing_files:
        logger.error(f"Missing required data files: {missing_files}")
        logger.info(f"Expected files in {data_dir}: {required_files}")
        logger.info("Use --create-sample-data flag to create sample data files")
        return False

    logger.info(f"All required data files found in {data_dir}")
    return True


def create_etl_config(args: argparse.Namespace) -> dict:
    """
    Create ETL job configuration from arguments.

    Args:
        args: Parsed command line arguments

    Returns:
        ETL configuration dictionary
    """
    return {
        "input_paths": {
            "customers": os.path.join(args.data_dir, "customers.csv"),
            "products": os.path.join(args.data_dir, "products.csv"),
            "sales": os.path.join(args.data_dir, "sales.csv"),
        },
        "output_path": args.output_dir,
        "spark_config": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.ui.showConsoleProgress": "true",
        },
    }


def print_job_summary(
    start_time: float, output_dir: str, logger: logging.Logger
) -> None:
    """
    Print job execution summary.

    Args:
        start_time: Job start time
        output_dir: Output directory path
        logger: Logger instance
    """
    duration = time.time() - start_time
    logger.info("=" * 60)
    logger.info("ETL PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Execution Time: {duration:.2f} seconds")
    logger.info(f"Output Location: {output_dir}")

    # List output files
    if FileUtils.directory_exists(output_dir):
        output_subdirs = [
            d
            for d in os.listdir(output_dir)
            if os.path.isdir(os.path.join(output_dir, d))
        ]
        logger.info(f"Generated Datasets: {len(output_subdirs)}")
        for subdir in sorted(output_subdirs):
            logger.info(f"  - {subdir}")
    else:
        logger.warning("Output directory not found")

    logger.info("=" * 60)


def main():
    """Main execution function."""
    # Parse arguments
    args = parse_arguments()

    # Setup logging
    logger = setup_logging(args.log_level)
    logger.info("Starting ETL Pipeline Runner")
    logger.info(f"Environment: {args.env}")
    logger.info(f"Data Directory: {args.data_dir}")
    logger.info(f"Output Directory: {args.output_dir}")

    try:
        # Create sample data if requested
        if args.create_sample_data:
            logger.info("Creating sample data files...")
            create_sample_data(args.data_dir)
            logger.info("Sample data files created successfully")

        # Validate data files
        if not validate_data_files(args.data_dir, logger):
            logger.error("Data validation failed")
            sys.exit(1)

        # Create configuration
        config = create_etl_config(args)
        logger.info("Configuration created successfully")

        # Dry run mode
        if args.dry_run:
            logger.info("DRY RUN MODE - Configuration and data validation only")
            logger.info("Configuration:")
            for key, value in config.items():
                logger.info(f"  {key}: {value}")
            logger.info("Dry run completed successfully")
            return

        # Record start time
        start_time = time.time()
        logger.info("Starting ETL job execution...")

        # Create and execute ETL job
        etl_job = ETLJob(config)
        etl_job.execute()

        # Print summary
        print_job_summary(start_time, args.output_dir, logger)

        logger.info("ETL Pipeline completed successfully!")

    except KeyboardInterrupt:
        logger.warning("ETL Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ETL Pipeline failed with error: {str(e)}")
        logger.exception("Full error traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
