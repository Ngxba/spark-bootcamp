"""
Command-line interface for ETL pipeline operations.
"""

import click
import sys
from pathlib import Path
from typing import Optional

from etl_pipeline.config.settings import load_config, validate_config
from etl_pipeline.jobs.customer_etl import CustomerETLJob
from etl_pipeline.utils.logging import setup_logging
from etl_pipeline.utils.metrics import MetricsCollector

import structlog

logger = structlog.get_logger(__name__)

# Available job classes
JOBS = {
    "customer_etl": CustomerETLJob,
}


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option("--log-level", default="INFO", help="Set log level")
def main(verbose: bool, log_level: str) -> None:
    """Spark ETL Pipeline CLI"""
    if verbose:
        log_level = "DEBUG"

    setup_logging(log_level)
    logger.info("ETL Pipeline CLI started")


@main.command()
@click.option("--job", "-j", required=True, help="Job name to run")
@click.option("--env", "-e", default="dev", help="Environment (dev/staging/prod)")
@click.option("--config-file", "-c", help="Custom config file path")
@click.option("--dry-run", is_flag=True, help="Validate without executing")
def run(job: str, env: str, config_file: Optional[str], dry_run: bool) -> None:
    """Run an ETL job"""

    try:
        # Load configuration
        if config_file:
            config = load_config(env, Path(config_file))
        else:
            config = load_config(env)

        logger.info("Configuration loaded", environment=env, job=job)

        # Validate configuration
        if not validate_config(config):
            logger.error("Configuration validation failed")
            sys.exit(1)

        # Get job class
        if job not in JOBS:
            logger.error("Unknown job", job=job, available_jobs=list(JOBS.keys()))
            sys.exit(1)

        job_class = JOBS[job]

        if dry_run:
            logger.info("Dry run mode - validating job configuration")
            job_instance = job_class(config)
            logger.info("Job configuration is valid")
            return

        # Initialize metrics
        metrics = MetricsCollector()

        # Create and run job
        logger.info("Starting ETL job", job=job, environment=env)
        job_instance = job_class(config, metrics)

        result = job_instance.run()

        if result.get("success", False):
            logger.info("ETL job completed successfully",
                       job=job,
                       records_processed=result.get("records_processed", 0),
                       duration=result.get("duration", 0))
        else:
            logger.error("ETL job failed",
                        job=job,
                        error=result.get("error", "Unknown error"))
            sys.exit(1)

    except Exception as e:
        logger.error("ETL job execution failed", error=str(e), job=job)
        sys.exit(1)


@main.command()
@click.option("--env", "-e", default="dev", help="Environment to validate")
@click.option("--config-file", "-c", help="Custom config file path")
def validate(env: str, config_file: Optional[str]) -> None:
    """Validate configuration for an environment"""

    try:
        if config_file:
            config = load_config(env, Path(config_file))
        else:
            config = load_config(env)

        if validate_config(config):
            logger.info("Configuration is valid", environment=env)
            click.echo(f"✅ Configuration for '{env}' environment is valid")
        else:
            logger.error("Configuration validation failed", environment=env)
            click.echo(f"❌ Configuration for '{env}' environment is invalid")
            sys.exit(1)

    except Exception as e:
        logger.error("Configuration validation error", error=str(e), environment=env)
        click.echo(f"❌ Error validating configuration: {e}")
        sys.exit(1)


@main.command("list-jobs")
def list_jobs() -> None:
    """List available ETL jobs"""

    click.echo("Available ETL Jobs:")
    click.echo("=" * 20)

    for job_name, job_class in JOBS.items():
        description = getattr(job_class, "__doc__", "No description available")
        if description:
            description = description.strip().split('\n')[0]

        click.echo(f"  {job_name:<20} - {description}")


@main.command("quality-check")
@click.option("--input", "-i", required=True, help="Input data path")
@click.option("--schema", "-s", help="Schema validation file")
@click.option("--output", "-o", help="Output report path")
@click.option("--env", "-e", default="dev", help="Environment")
def quality_check(input: str, schema: Optional[str], output: Optional[str], env: str) -> None:
    """Run data quality checks on input data"""

    try:
        from etl_pipeline.transformers.validators import DataValidator
        from etl_pipeline.utils.spark import create_spark_session

        # Load config and create Spark session
        config = load_config(env)
        spark = create_spark_session(config)

        # Read input data
        logger.info("Reading input data", path=input)

        if input.endswith('.parquet'):
            df = spark.read.parquet(input)
        elif input.endswith('.csv'):
            df = spark.read.option("header", "true").csv(input)
        elif input.endswith('.json'):
            df = spark.read.json(input)
        else:
            logger.error("Unsupported file format", path=input)
            sys.exit(1)

        # Run quality checks
        validator = DataValidator()
        quality_report = validator.generate_quality_report(df)

        # Display results
        quality_score = quality_report.get("quality_score", 0)

        click.echo(f"\n📊 Data Quality Report")
        click.echo(f"=" * 30)
        click.echo(f"Input Path: {input}")
        click.echo(f"Records: {quality_report['dataset_info']['row_count']:,}")
        click.echo(f"Columns: {quality_report['dataset_info']['column_count']}")
        click.echo(f"Quality Score: {quality_score}%")

        # Show completeness
        if "completeness" in quality_report:
            click.echo(f"\nCompleteness by Column:")
            for col, completeness in quality_report["completeness"].items():
                status = "✅" if completeness >= 95 else "⚠️" if completeness >= 80 else "❌"
                click.echo(f"  {col:<20} {completeness:>6.1f}% {status}")

        # Save report if output specified
        if output:
            import json
            with open(output, 'w') as f:
                json.dump(quality_report, f, indent=2)
            logger.info("Quality report saved", path=output)

        # Exit with appropriate code
        if quality_score < 80:
            logger.warning("Quality score below threshold", score=quality_score)
            sys.exit(1)
        else:
            logger.info("Data quality check passed", score=quality_score)

    except Exception as e:
        logger.error("Quality check failed", error=str(e))
        sys.exit(1)


@main.command()
@click.option("--env", "-e", default="dev", help="Environment")
@click.option("--port", "-p", default=4040, help="Spark UI port")
def monitor(env: str, port: int) -> None:
    """Start monitoring dashboard"""

    try:
        config = load_config(env)

        click.echo(f"🚀 Starting monitoring for '{env}' environment")
        click.echo(f"Spark UI will be available at: http://localhost:{port}")
        click.echo("Press Ctrl+C to stop monitoring")

        # In a real implementation, this would start a monitoring service
        # For now, we'll just show the configuration
        click.echo(f"\nConfiguration Preview:")
        click.echo(f"  App Name: {config.get('spark', {}).get('app_name', 'Unknown')}")
        click.echo(f"  Master: {config.get('spark', {}).get('master', 'Unknown')}")
        click.echo(f"  Input Path: {config.get('data', {}).get('input_path', 'Unknown')}")
        click.echo(f"  Output Path: {config.get('data', {}).get('output_path', 'Unknown')}")

    except Exception as e:
        logger.error("Failed to start monitoring", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()