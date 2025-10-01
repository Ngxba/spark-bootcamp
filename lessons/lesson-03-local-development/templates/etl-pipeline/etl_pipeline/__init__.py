"""
Spark ETL Pipeline Template

A production-ready Apache Spark ETL pipeline with modern development practices.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from etl_pipeline.config.settings import load_config
from etl_pipeline.jobs.base import BaseETLJob

__all__ = [
    "load_config",
    "BaseETLJob",
]