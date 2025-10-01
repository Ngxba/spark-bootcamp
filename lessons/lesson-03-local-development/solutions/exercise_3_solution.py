"""
Solution 3: Multi-Environment Setup
==================================

Complete solution for Exercise 3 demonstrating how to implement configuration
management for different environments (dev/staging/production).

This solution shows:
- Hierarchical configuration management
- Environment variable resolution
- Secrets management with multiple sources
- Environment-aware Spark sessions
- Data path management for different storage systems
- Feature flag systems
- Environment validation
- Deployment configuration generation
"""

import os
import yaml
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Callable
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import tempfile
import logging
from datetime import datetime
import hashlib

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class Environment(Enum):
    """Supported environments"""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class ConfigSection:
    """Represents a configuration section with validation"""
    name: str
    data: Dict[str, Any]
    required_keys: List[str] = field(default_factory=list)

    def validate(self) -> List[str]:
        """Validate configuration section"""
        errors = []
        for key in self.required_keys:
            if key not in self.data:
                errors.append(f"Missing required key '{key}' in section '{self.name}'")
        return errors


def exercise_3a_create_configuration_hierarchy() -> Dict[str, Dict[str, Any]]:
    """
    SOLUTION: Create a hierarchical configuration system with base and environment-specific configs.
    """

    # Base configuration - common settings shared across all environments
    base_config = {
        "app": {
            "name": "spark-analytics-platform",
            "version": "2.0.0",
            "description": "Enterprise Spark Analytics Platform"
        },
        "spark": {
            "app_name": "AnalyticsPlatform",
            "serializer": "org.apache.spark.serializer.KryoSerializer",
            "sql": {
                "adaptive": {
                    "enabled": True,
                    "coalescePartitions": {
                        "enabled": True
                    }
                }
            },
            "default": {
                "parallelism": 100
            }
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "handlers": ["console", "file"]
        },
        "monitoring": {
            "metrics_enabled": True,
            "health_check_interval": 30
        },
        "data": {
            "formats": {
                "input": ["parquet", "csv", "json"],
                "output": ["parquet", "delta"]
            },
            "compression": "snappy"
        },
        "security": {
            "encryption_at_rest": False,
            "encryption_in_transit": False
        }
    }

    # Development configuration - optimized for local development
    dev_config = {
        "app": {
            "debug": True,
            "environment": "development"
        },
        "spark": {
            "master": "local[2]",
            "driver": {
                "memory": "2g",
                "cores": 1
            },
            "executor": {
                "memory": "1g",
                "cores": 1,
                "instances": 1
            },
            "sql": {
                "shuffle": {
                    "partitions": 4
                },
                "adaptive": {
                    "advisoryPartitionSizeInBytes": "64MB"
                }
            }
        },
        "data": {
            "input_path": "data/dev/input",
            "output_path": "data/dev/output",
            "temp_path": "data/dev/temp",
            "checkpoint_path": "data/dev/checkpoints"
        },
        "database": {
            "type": "sqlite",
            "host": "localhost",
            "port": None,
            "name": "dev_analytics.db",
            "schema": "public"
        },
        "storage": {
            "type": "local",
            "base_path": "data/dev"
        },
        "feature_flags": {
            "enable_advanced_analytics": True,
            "enable_streaming": False,
            "enable_ml_pipeline": True,
            "enable_data_quality_checks": True
        },
        "performance": {
            "cache_tables": True,
            "optimize_joins": False
        }
    }

    # Staging configuration - pre-production testing
    staging_config = {
        "app": {
            "debug": False,
            "environment": "staging"
        },
        "spark": {
            "master": "yarn",
            "deploy_mode": "cluster",
            "driver": {
                "memory": "4g",
                "cores": 2
            },
            "executor": {
                "memory": "4g",
                "cores": 2,
                "instances": 4
            },
            "sql": {
                "shuffle": {
                    "partitions": 200
                },
                "adaptive": {
                    "advisoryPartitionSizeInBytes": "128MB",
                    "skewJoin": {
                        "enabled": True
                    }
                }
            }
        },
        "data": {
            "input_path": "s3a://staging-input-bucket/data",
            "output_path": "s3a://staging-output-bucket/data",
            "temp_path": "s3a://staging-temp-bucket/temp",
            "checkpoint_path": "s3a://staging-checkpoint-bucket/checkpoints"
        },
        "database": {
            "type": "postgresql",
            "host": "${STAGING_DB_HOST}",
            "port": "${STAGING_DB_PORT:-5432}",
            "name": "${STAGING_DB_NAME}",
            "schema": "analytics"
        },
        "storage": {
            "type": "s3",
            "region": "us-west-2",
            "access_key": "${AWS_ACCESS_KEY_ID}",
            "secret_key": "${AWS_SECRET_ACCESS_KEY}"
        },
        "feature_flags": {
            "enable_advanced_analytics": True,
            "enable_streaming": True,
            "enable_ml_pipeline": True,
            "enable_data_quality_checks": True
        },
        "performance": {
            "cache_tables": True,
            "optimize_joins": True
        },
        "security": {
            "encryption_at_rest": True,
            "encryption_in_transit": True
        }
    }

    # Production configuration - full security and performance
    prod_config = {
        "app": {
            "debug": False,
            "environment": "production"
        },
        "spark": {
            "master": "yarn",
            "deploy_mode": "cluster",
            "driver": {
                "memory": "8g",
                "cores": 4
            },
            "executor": {
                "memory": "8g",
                "cores": 4,
                "instances": 20
            },
            "sql": {
                "shuffle": {
                    "partitions": 1000
                },
                "adaptive": {
                    "advisoryPartitionSizeInBytes": "256MB",
                    "skewJoin": {
                        "enabled": True,
                        "skewedPartitionFactor": 5
                    }
                }
            },
            "dynamicAllocation": {
                "enabled": True,
                "minExecutors": 5,
                "maxExecutors": 50,
                "initialExecutors": 10
            }
        },
        "data": {
            "input_path": "${PROD_INPUT_S3_BUCKET}/data",
            "output_path": "${PROD_OUTPUT_S3_BUCKET}/data",
            "temp_path": "${PROD_TEMP_S3_BUCKET}/temp",
            "checkpoint_path": "${PROD_CHECKPOINT_S3_BUCKET}/checkpoints"
        },
        "database": {
            "type": "postgresql",
            "host": "${PROD_DB_HOST}",
            "port": "${PROD_DB_PORT:-5432}",
            "name": "${PROD_DB_NAME}",
            "schema": "analytics",
            "ssl_mode": "require"
        },
        "storage": {
            "type": "s3",
            "region": "${AWS_REGION}",
            "access_key": "${AWS_ACCESS_KEY_ID}",
            "secret_key": "${AWS_SECRET_ACCESS_KEY}",
            "kms_key": "${AWS_KMS_KEY_ID}"
        },
        "feature_flags": {
            "enable_advanced_analytics": True,
            "enable_streaming": True,
            "enable_ml_pipeline": True,
            "enable_data_quality_checks": True
        },
        "performance": {
            "cache_tables": True,
            "optimize_joins": True,
            "enable_cost_based_optimizer": True
        },
        "security": {
            "encryption_at_rest": True,
            "encryption_in_transit": True,
            "kerberos_enabled": True,
            "ssl_enabled": True
        },
        "monitoring": {
            "metrics_enabled": True,
            "health_check_interval": 10,
            "alerting_enabled": True,
            "slack_webhook": "${SLACK_WEBHOOK_URL}",
            "email_alerts": ["${ALERT_EMAIL}"]
        }
    }

    return {
        "base": base_config,
        "dev": dev_config,
        "staging": staging_config,
        "prod": prod_config
    }


def exercise_3b_implement_config_loader() -> type:
    """
    SOLUTION: Create a configuration loader that can merge base and environment configs.
    """

    class ConfigLoader:
        """Configuration loader with environment merging and variable resolution"""

        def __init__(self, config_hierarchy: Optional[Dict[str, Dict[str, Any]]] = None):
            self.config_hierarchy = config_hierarchy or exercise_3a_create_configuration_hierarchy()
            self.env_var_pattern = re.compile(r'\$\{([^}]+)\}')

        def load_config(self, environment: str) -> Dict[str, Any]:
            """Load and merge configuration for specified environment"""
            if environment not in self.config_hierarchy:
                raise ValueError(f"Unknown environment: {environment}")

            # Start with base configuration
            base_config = self.config_hierarchy.get("base", {})
            env_config = self.config_hierarchy.get(environment, {})

            # Deep merge configurations
            merged_config = self._deep_merge(base_config, env_config)

            # Resolve environment variables
            resolved_config = self.resolve_environment_variables(merged_config)

            return resolved_config

        def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
            """Deep merge two dictionaries"""
            result = base.copy()

            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._deep_merge(result[key], value)
                else:
                    result[key] = value

            return result

        def resolve_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
            """Resolve environment variables in configuration values"""
            if isinstance(config, dict):
                return {key: self.resolve_environment_variables(value) for key, value in config.items()}
            elif isinstance(config, list):
                return [self.resolve_environment_variables(item) for item in config]
            elif isinstance(config, str):
                return self._resolve_env_vars_in_string(config)
            else:
                return config

        def _resolve_env_vars_in_string(self, value: str) -> str:
            """Replace ${VAR_NAME} or ${VAR_NAME:-default} patterns with environment variable values"""
            def replace_var(match):
                var_expr = match.group(1)
                if ':-' in var_expr:
                    var_name, default_value = var_expr.split(':-', 1)
                    return os.environ.get(var_name, default_value)
                else:
                    var_name = var_expr
                    return os.environ.get(var_name, match.group(0))  # Return original if not found

            return self.env_var_pattern.sub(replace_var, value)

        def validate_config(self, config: Dict[str, Any]) -> bool:
            """Validate configuration structure and required fields"""
            required_sections = ["app", "spark", "data"]
            required_app_fields = ["name", "version"]
            required_spark_fields = ["master"]
            required_data_fields = ["input_path", "output_path"]

            # Check required sections
            for section in required_sections:
                if section not in config:
                    print(f"❌ Missing required section: {section}")
                    return False

            # Validate app section
            for field in required_app_fields:
                if field not in config["app"]:
                    print(f"❌ Missing required app field: {field}")
                    return False

            # Validate spark section
            for field in required_spark_fields:
                if field not in config["spark"]:
                    print(f"❌ Missing required spark field: {field}")
                    return False

            # Validate data section
            for field in required_data_fields:
                if field not in config["data"]:
                    print(f"❌ Missing required data field: {field}")
                    return False

            return True

        def get_database_url(self, config: Dict[str, Any]) -> str:
            """Construct database URL from configuration"""
            db_config = config.get("database", {})
            db_type = db_config.get("type", "postgresql")
            host = db_config.get("host", "localhost")
            port = db_config.get("port", 5432)
            name = db_config.get("name", "analytics")

            if db_type == "sqlite":
                return f"sqlite:///{name}"
            elif db_type == "postgresql":
                return f"postgresql://{host}:{port}/{name}"
            elif db_type == "mysql":
                return f"mysql://{host}:{port}/{name}"
            else:
                return f"{db_type}://{host}:{port}/{name}"

        def get_spark_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
            """Extract Spark-specific configuration"""
            spark_config = config.get("spark", {})

            # Convert nested dict to flat keys for Spark
            flat_config = {}

            def flatten(d, prefix=""):
                for key, value in d.items():
                    if isinstance(value, dict):
                        flatten(value, f"{prefix}{key}.")
                    else:
                        flat_config[f"{prefix}{key}"] = str(value)

            flatten(spark_config, "spark.")

            # Remove the redundant spark prefix for some configs
            cleaned_config = {}
            for key, value in flat_config.items():
                if key.startswith("spark.spark."):
                    cleaned_config[key.replace("spark.spark.", "spark.")] = value
                else:
                    cleaned_config[key] = value

            return cleaned_config

    return ConfigLoader


def exercise_3c_implement_secrets_manager() -> type:
    """
    SOLUTION: Create a secrets management system for handling sensitive data.
    """

    class SecretsManager:
        """Secure secrets management with multiple sources and audit logging"""

        def __init__(self):
            self.secrets_cache = {}
            self.audit_log = []
            self.sources = ["env", "file", "vault"]  # Priority order

        def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
            """Get secret from various sources with audit logging"""
            self._log_access(key)

            # Check cache first
            if key in self.secrets_cache:
                return self.secrets_cache[key]

            # Try each source in priority order
            for source in self.sources:
                value = self._get_from_source(key, source)
                if value is not None:
                    self.secrets_cache[key] = value
                    return value

            return default

        def _get_from_source(self, key: str, source: str) -> Optional[str]:
            """Get secret from specific source"""
            if source == "env":
                return os.environ.get(key)
            elif source == "file":
                return self._get_from_file(key)
            elif source == "vault":
                return self._get_from_vault(key)
            return None

        def _get_from_file(self, key: str) -> Optional[str]:
            """Get secret from file (e.g., Docker secrets, Kubernetes secrets)"""
            # Check common secret file locations
            secret_paths = [
                f"/run/secrets/{key}",
                f"/var/secrets/{key}",
                f"~/.secrets/{key}",
                f".secrets/{key}"
            ]

            for path in secret_paths:
                expanded_path = Path(path).expanduser()
                if expanded_path.exists() and expanded_path.is_file():
                    try:
                        return expanded_path.read_text().strip()
                    except Exception:
                        continue
            return None

        def _get_from_vault(self, key: str) -> Optional[str]:
            """Get secret from HashiCorp Vault (mock implementation)"""
            # In a real implementation, this would connect to Vault
            # For demo purposes, we'll check a vault-like environment variable
            vault_key = f"VAULT_{key.upper()}"
            return os.environ.get(vault_key)

        def set_secret(self, key: str, value: str) -> None:
            """Set secret in cache (for testing/development)"""
            self.secrets_cache[key] = value
            self._log_access(key, action="set")

        def load_from_file(self, file_path: str) -> None:
            """Load secrets from a JSON or YAML file"""
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"Secrets file not found: {file_path}")

            try:
                if file_path.suffix.lower() == '.json':
                    with open(file_path, 'r') as f:
                        secrets = json.load(f)
                elif file_path.suffix.lower() in ['.yml', '.yaml']:
                    with open(file_path, 'r') as f:
                        secrets = yaml.safe_load(f)
                else:
                    raise ValueError(f"Unsupported file format: {file_path.suffix}")

                for key, value in secrets.items():
                    self.secrets_cache[key] = str(value)

            except Exception as e:
                raise ValueError(f"Failed to load secrets from {file_path}: {e}")

        def get_database_credentials(self) -> Dict[str, str]:
            """Get database credentials"""
            return {
                "username": self.get_secret("DB_USERNAME", "analytics_user"),
                "password": self.get_secret("DB_PASSWORD", ""),
                "host": self.get_secret("DB_HOST", "localhost"),
                "port": self.get_secret("DB_PORT", "5432"),
                "database": self.get_secret("DB_NAME", "analytics")
            }

        def get_aws_credentials(self) -> Dict[str, str]:
            """Get AWS credentials"""
            return {
                "access_key_id": self.get_secret("AWS_ACCESS_KEY_ID", ""),
                "secret_access_key": self.get_secret("AWS_SECRET_ACCESS_KEY", ""),
                "session_token": self.get_secret("AWS_SESSION_TOKEN", ""),
                "region": self.get_secret("AWS_REGION", "us-west-2")
            }

        def _log_access(self, key: str, action: str = "get") -> None:
            """Log secret access for audit purposes"""
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "action": action,
                "key": self._hash_key(key),  # Hash for security
                "source": "secrets_manager"
            }
            self.audit_log.append(log_entry)

            # Keep only last 1000 entries
            if len(self.audit_log) > 1000:
                self.audit_log = self.audit_log[-1000:]

        def _hash_key(self, key: str) -> str:
            """Hash key for audit logging without exposing actual key names"""
            return hashlib.sha256(key.encode()).hexdigest()[:16]

        def get_audit_log(self) -> List[Dict[str, str]]:
            """Get audit log for security monitoring"""
            return self.audit_log.copy()

    return SecretsManager


def exercise_3d_create_environment_aware_spark_session() -> callable:
    """
    SOLUTION: Create a function that creates Spark sessions based on environment configuration.
    """

    def create_spark_session(environment: str, app_name: str) -> SparkSession:
        """Create environment-specific Spark session with optimized configurations"""

        # Load configuration for environment
        config_loader = exercise_3b_implement_config_loader()()
        config = config_loader.load_config(environment)

        # Get Spark configuration
        spark_config = config_loader.get_spark_config(config)
        app_config = config.get("app", {})

        # Create Spark session builder
        builder = SparkSession.builder.appName(f"{app_name}-{environment}")

        # Set master URL based on environment
        spark_master = config["spark"]["master"]
        builder = builder.master(spark_master)

        # Set common configurations
        builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Environment-specific optimizations
        if environment == "dev":
            # Development optimizations
            builder = (builder
                      .config("spark.sql.shuffle.partitions", "4")
                      .config("spark.sql.adaptive.enabled", "true")
                      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                      .config("spark.driver.memory", "2g")
                      .config("spark.executor.memory", "1g"))

        elif environment == "staging":
            # Staging optimizations
            builder = (builder
                      .config("spark.sql.shuffle.partitions", "200")
                      .config("spark.sql.adaptive.enabled", "true")
                      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                      .config("spark.sql.adaptive.skewJoin.enabled", "true")
                      .config("spark.driver.memory", "4g")
                      .config("spark.executor.memory", "4g")
                      .config("spark.executor.instances", "4"))

        elif environment == "prod":
            # Production optimizations
            builder = (builder
                      .config("spark.sql.shuffle.partitions", "1000")
                      .config("spark.sql.adaptive.enabled", "true")
                      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                      .config("spark.sql.adaptive.skewJoin.enabled", "true")
                      .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
                      .config("spark.dynamicAllocation.enabled", "true")
                      .config("spark.dynamicAllocation.minExecutors", "5")
                      .config("spark.dynamicAllocation.maxExecutors", "50")
                      .config("spark.dynamicAllocation.initialExecutors", "10")
                      .config("spark.driver.memory", "8g")
                      .config("spark.executor.memory", "8g")
                      .config("spark.executor.cores", "4"))

            # Production security settings
            security_config = config.get("security", {})
            if security_config.get("kerberos_enabled"):
                builder = builder.config("spark.security.kerberos.enabled", "true")
            if security_config.get("ssl_enabled"):
                builder = builder.config("spark.ssl.enabled", "true")

        # Add storage-specific configurations
        storage_config = config.get("storage", {})
        if storage_config.get("type") == "s3":
            aws_credentials = exercise_3c_implement_secrets_manager()().get_aws_credentials()
            builder = (builder
                      .config("spark.hadoop.fs.s3a.access.key", aws_credentials["access_key_id"])
                      .config("spark.hadoop.fs.s3a.secret.key", aws_credentials["secret_access_key"])
                      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))

        # Add monitoring configurations
        monitoring_config = config.get("monitoring", {})
        if monitoring_config.get("metrics_enabled"):
            builder = (builder
                      .config("spark.sql.streaming.metricsEnabled", "true")
                      .config("spark.metrics.conf.*.sink.console.class",
                             "org.apache.spark.metrics.sink.ConsoleSink"))

        # Apply any additional Spark configurations from config
        for key, value in spark_config.items():
            if key.startswith("spark."):
                builder = builder.config(key, value)

        # Create and return session
        try:
            spark = builder.getOrCreate()

            # Set log level based on environment
            log_level = "DEBUG" if app_config.get("debug", False) else "WARN"
            spark.sparkContext.setLogLevel(log_level)

            print(f"✅ Spark session created for {environment} environment")
            print(f"   App: {spark.sparkContext.appName}")
            print(f"   Master: {spark.sparkContext.master}")
            print(f"   Executors: {spark.sparkContext.defaultParallelism}")

            return spark

        except Exception as e:
            print(f"❌ Failed to create Spark session for {environment}: {e}")
            raise

    return create_spark_session


def exercise_3e_implement_data_path_manager() -> type:
    """
    SOLUTION: Create a data path manager that handles different storage systems per environment.
    """

    class DataPathManager:
        """Manages data paths across different environments and storage systems"""

        def __init__(self, environment: str):
            self.environment = environment
            config_loader = exercise_3b_implement_config_loader()()
            self.config = config_loader.load_config(environment)
            self.data_config = self.config.get("data", {})
            self.storage_config = self.config.get("storage", {})

        def get_input_path(self, dataset: str) -> str:
            """Get input path for specified dataset"""
            base_path = self.data_config.get("input_path", "data/input")
            return self._construct_path(base_path, dataset)

        def get_output_path(self, dataset: str) -> str:
            """Get output path for specified dataset"""
            base_path = self.data_config.get("output_path", "data/output")
            return self._construct_path(base_path, dataset)

        def get_temp_path(self) -> str:
            """Get temporary path for intermediate data"""
            temp_path = self.data_config.get("temp_path", "data/temp")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return self._construct_path(temp_path, f"temp_{timestamp}")

        def get_checkpoint_path(self, job_name: str) -> str:
            """Get checkpoint path for streaming jobs"""
            checkpoint_path = self.data_config.get("checkpoint_path", "data/checkpoints")
            return self._construct_path(checkpoint_path, job_name)

        def _construct_path(self, base_path: str, *parts: str) -> str:
            """Construct path with proper separators for storage type"""
            storage_type = self.storage_config.get("type", "local")

            if storage_type == "local":
                # Use pathlib for local paths
                path = Path(base_path)
                for part in parts:
                    path = path / part
                return str(path)
            else:
                # Use forward slashes for cloud storage
                path_parts = [base_path.rstrip("/")]
                for part in parts:
                    path_parts.append(str(part).strip("/"))
                return "/".join(path_parts)

        def ensure_paths_exist(self) -> None:
            """Ensure required paths exist (for local storage)"""
            storage_type = self.storage_config.get("type", "local")

            if storage_type == "local":
                paths_to_create = [
                    self.data_config.get("input_path", "data/input"),
                    self.data_config.get("output_path", "data/output"),
                    self.data_config.get("temp_path", "data/temp"),
                    self.data_config.get("checkpoint_path", "data/checkpoints")
                ]

                for path in paths_to_create:
                    Path(path).mkdir(parents=True, exist_ok=True)
                    print(f"✅ Created directory: {path}")
            else:
                print(f"ℹ️  Path validation skipped for {storage_type} storage")

        def get_storage_config(self) -> Dict[str, Any]:
            """Get storage configuration for the current environment"""
            return {
                "type": self.storage_config.get("type", "local"),
                "region": self.storage_config.get("region"),
                "compression": self.data_config.get("compression", "snappy"),
                "formats": self.data_config.get("formats", {}),
                "partitioning": self._get_partitioning_strategy()
            }

        def _get_partitioning_strategy(self) -> Dict[str, Any]:
            """Get partitioning strategy based on environment"""
            if self.environment == "dev":
                return {"enabled": False}
            elif self.environment == "staging":
                return {
                    "enabled": True,
                    "columns": ["year", "month"],
                    "max_files_per_partition": 100
                }
            else:  # production
                return {
                    "enabled": True,
                    "columns": ["year", "month", "day"],
                    "max_files_per_partition": 1000,
                    "auto_optimize": True
                }

        def get_versioned_path(self, base_path: str, version: str) -> str:
            """Get versioned path for data lineage"""
            return self._construct_path(base_path, f"v{version}")

        def list_available_datasets(self, path_type: str = "input") -> List[str]:
            """List available datasets in specified path"""
            if path_type == "input":
                base_path = self.data_config.get("input_path", "data/input")
            elif path_type == "output":
                base_path = self.data_config.get("output_path", "data/output")
            else:
                raise ValueError(f"Unknown path type: {path_type}")

            storage_type = self.storage_config.get("type", "local")

            if storage_type == "local":
                return self._list_local_datasets(base_path)
            else:
                # In real implementation, would use cloud storage APIs
                return ["customers", "orders", "products"]  # Mock data

        def _list_local_datasets(self, base_path: str) -> List[str]:
            """List datasets in local storage"""
            path = Path(base_path)
            if not path.exists():
                return []

            datasets = []
            for item in path.iterdir():
                if item.is_dir():
                    datasets.append(item.name)
            return sorted(datasets)

    return DataPathManager


def exercise_3f_create_feature_flag_system() -> type:
    """
    SOLUTION: Create a feature flag system for environment-specific functionality.
    """

    class FeatureFlags:
        """Feature flag system for environment-specific functionality"""

        def __init__(self, environment: str):
            self.environment = environment
            self.flags = {}
            self.change_log = []
            self._load_default_flags()

        def _load_default_flags(self) -> None:
            """Load default feature flags for the environment"""
            config_loader = exercise_3b_implement_config_loader()()
            config = config_loader.load_config(self.environment)
            env_flags = config.get("feature_flags", {})

            for flag, value in env_flags.items():
                self.flags[flag] = value

        def is_enabled(self, feature: str) -> bool:
            """Check if a feature is enabled"""
            # Check environment variable override first
            env_var = f"FEATURE_{feature.upper()}"
            env_value = os.environ.get(env_var)
            if env_value is not None:
                return env_value.lower() in ["true", "1", "yes", "on"]

            # Return configured value or False as default
            return self.flags.get(feature, False)

        def enable_feature(self, feature: str) -> None:
            """Enable a feature flag"""
            old_value = self.flags.get(feature, False)
            self.flags[feature] = True
            self._log_change(feature, old_value, True)
            print(f"✅ Feature '{feature}' enabled")

        def disable_feature(self, feature: str) -> None:
            """Disable a feature flag"""
            old_value = self.flags.get(feature, False)
            self.flags[feature] = False
            self._log_change(feature, old_value, False)
            print(f"❌ Feature '{feature}' disabled")

        def toggle_feature(self, feature: str) -> bool:
            """Toggle a feature flag and return new state"""
            current_state = self.is_enabled(feature)
            if current_state:
                self.disable_feature(feature)
            else:
                self.enable_feature(feature)
            return not current_state

        def get_all_flags(self) -> Dict[str, bool]:
            """Get all feature flags with their current states"""
            result = {}
            for feature in self.flags:
                result[feature] = self.is_enabled(feature)
            return result

        def load_from_config(self, config: Dict[str, Any]) -> None:
            """Load feature flags from configuration"""
            feature_config = config.get("feature_flags", {})
            for feature, enabled in feature_config.items():
                old_value = self.flags.get(feature, False)
                self.flags[feature] = bool(enabled)
                if old_value != enabled:
                    self._log_change(feature, old_value, enabled)

        def get_feature_context(self, features: List[str]) -> Dict[str, bool]:
            """Get context for multiple features"""
            context = {}
            for feature in features:
                context[feature] = self.is_enabled(feature)
            return context

        def _log_change(self, feature: str, old_value: bool, new_value: bool) -> None:
            """Log feature flag changes"""
            change_entry = {
                "timestamp": datetime.now().isoformat(),
                "feature": feature,
                "old_value": old_value,
                "new_value": new_value,
                "environment": self.environment
            }
            self.change_log.append(change_entry)

            # Keep only last 100 changes
            if len(self.change_log) > 100:
                self.change_log = self.change_log[-100:]

        def get_change_log(self) -> List[Dict[str, Any]]:
            """Get feature flag change history"""
            return self.change_log.copy()

        def export_flags(self) -> str:
            """Export current flags as JSON"""
            export_data = {
                "environment": self.environment,
                "timestamp": datetime.now().isoformat(),
                "flags": self.get_all_flags()
            }
            return json.dumps(export_data, indent=2)

        def import_flags(self, json_data: str) -> None:
            """Import flags from JSON"""
            try:
                data = json.loads(json_data)
                flags = data.get("flags", {})
                for feature, enabled in flags.items():
                    old_value = self.flags.get(feature, False)
                    self.flags[feature] = bool(enabled)
                    if old_value != enabled:
                        self._log_change(feature, old_value, enabled)
                print(f"✅ Imported {len(flags)} feature flags")
            except Exception as e:
                print(f"❌ Failed to import flags: {e}")
                raise

    return FeatureFlags


def exercise_3g_implement_environment_validator() -> callable:
    """
    SOLUTION: Create a validator that ensures environment setup is correct.
    """

    def validate_environment(environment: str) -> Dict[str, bool]:
        """Comprehensive environment validation"""
        results = {}

        print(f"🔍 Validating {environment} environment...")

        # 1. Configuration validation
        results["config_valid"] = _validate_configuration(environment)

        # 2. Environment variables validation
        results["env_vars_valid"] = _validate_environment_variables(environment)

        # 3. Storage accessibility
        results["storage_accessible"] = _validate_storage_access(environment)

        # 4. Database connectivity
        results["database_accessible"] = _validate_database_connection(environment)

        # 5. Spark initialization
        results["spark_initializable"] = _validate_spark_initialization(environment)

        # 6. Secrets availability
        results["secrets_available"] = _validate_secrets_availability(environment)

        # 7. Feature flags validation
        results["feature_flags_valid"] = _validate_feature_flags(environment)

        # 8. Security requirements
        results["security_requirements_met"] = _validate_security_requirements(environment)

        # Summary
        all_valid = all(results.values())
        print(f"\n{'✅' if all_valid else '❌'} Environment validation {'PASSED' if all_valid else 'FAILED'}")

        return results

    def _validate_configuration(environment: str) -> bool:
        """Validate configuration loading and structure"""
        try:
            config_loader = exercise_3b_implement_config_loader()()
            config = config_loader.load_config(environment)
            is_valid = config_loader.validate_config(config)
            print(f"  {'✅' if is_valid else '❌'} Configuration: {'Valid' if is_valid else 'Invalid'}")
            return is_valid
        except Exception as e:
            print(f"  ❌ Configuration: Error loading - {e}")
            return False

    def _validate_environment_variables(environment: str) -> bool:
        """Validate required environment variables"""
        config_loader = exercise_3b_implement_config_loader()()
        config = config_loader.load_config(environment)

        required_vars = []
        if environment == "prod":
            required_vars = [
                "PROD_INPUT_S3_BUCKET",
                "PROD_OUTPUT_S3_BUCKET",
                "PROD_DB_HOST",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY"
            ]
        elif environment == "staging":
            required_vars = [
                "STAGING_DB_HOST",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY"
            ]

        missing_vars = [var for var in required_vars if not os.environ.get(var)]

        if missing_vars:
            print(f"  ❌ Environment Variables: Missing {missing_vars}")
            return False
        else:
            print(f"  ✅ Environment Variables: All required variables present")
            return True

    def _validate_storage_access(environment: str) -> bool:
        """Validate storage accessibility"""
        try:
            path_manager = exercise_3e_implement_data_path_manager()(environment)
            storage_config = path_manager.get_storage_config()

            if storage_config["type"] == "local":
                # For local storage, ensure paths can be created
                path_manager.ensure_paths_exist()
                print(f"  ✅ Storage: Local paths accessible")
                return True
            else:
                # For cloud storage, this would test actual connectivity
                # For demo purposes, we'll assume it's accessible if credentials exist
                aws_creds = exercise_3c_implement_secrets_manager()().get_aws_credentials()
                if aws_creds["access_key_id"] and aws_creds["secret_access_key"]:
                    print(f"  ✅ Storage: Cloud storage credentials available")
                    return True
                else:
                    print(f"  ❌ Storage: Missing cloud storage credentials")
                    return False
        except Exception as e:
            print(f"  ❌ Storage: Access validation failed - {e}")
            return False

    def _validate_database_connection(environment: str) -> bool:
        """Validate database connectivity"""
        try:
            config_loader = exercise_3b_implement_config_loader()()
            config = config_loader.load_config(environment)
            db_url = config_loader.get_database_url(config)

            # For demo purposes, we'll just validate the URL format
            if db_url and "://" in db_url:
                print(f"  ✅ Database: Connection URL valid")
                return True
            else:
                print(f"  ❌ Database: Invalid connection URL")
                return False
        except Exception as e:
            print(f"  ❌ Database: Connection validation failed - {e}")
            return False

    def _validate_spark_initialization(environment: str) -> bool:
        """Validate Spark can be initialized"""
        try:
            # For demo purposes, we'll just validate the configuration
            config_loader = exercise_3b_implement_config_loader()()
            config = config_loader.load_config(environment)
            spark_config = config_loader.get_spark_config(config)

            required_configs = ["spark.master"]
            missing_configs = [cfg for cfg in required_configs if cfg not in spark_config]

            if missing_configs:
                print(f"  ❌ Spark: Missing configurations {missing_configs}")
                return False
            else:
                print(f"  ✅ Spark: Configuration valid")
                return True
        except Exception as e:
            print(f"  ❌ Spark: Initialization validation failed - {e}")
            return False

    def _validate_secrets_availability(environment: str) -> bool:
        """Validate required secrets are available"""
        try:
            secrets_manager = exercise_3c_implement_secrets_manager()()

            # Test access to common secrets
            required_secrets = []
            if environment == "prod":
                required_secrets = ["DB_PASSWORD", "AWS_SECRET_ACCESS_KEY"]

            missing_secrets = []
            for secret in required_secrets:
                if not secrets_manager.get_secret(secret):
                    missing_secrets.append(secret)

            if missing_secrets:
                print(f"  ❌ Secrets: Missing secrets {missing_secrets}")
                return False
            else:
                print(f"  ✅ Secrets: All required secrets available")
                return True
        except Exception as e:
            print(f"  ❌ Secrets: Validation failed - {e}")
            return False

    def _validate_feature_flags(environment: str) -> bool:
        """Validate feature flags configuration"""
        try:
            feature_flags = exercise_3f_create_feature_flag_system()(environment)
            flags = feature_flags.get_all_flags()

            if flags:
                print(f"  ✅ Feature Flags: {len(flags)} flags configured")
                return True
            else:
                print(f"  ⚠️  Feature Flags: No flags configured")
                return True  # Not an error, just a warning
        except Exception as e:
            print(f"  ❌ Feature Flags: Validation failed - {e}")
            return False

    def _validate_security_requirements(environment: str) -> bool:
        """Validate security requirements for environment"""
        try:
            config_loader = exercise_3b_implement_config_loader()()
            config = config_loader.load_config(environment)
            security_config = config.get("security", {})

            if environment == "prod":
                # Production requires encryption
                encryption_at_rest = security_config.get("encryption_at_rest", False)
                encryption_in_transit = security_config.get("encryption_in_transit", False)

                if not (encryption_at_rest and encryption_in_transit):
                    print(f"  ❌ Security: Production requires encryption")
                    return False

            print(f"  ✅ Security: Requirements met for {environment}")
            return True
        except Exception as e:
            print(f"  ❌ Security: Validation failed - {e}")
            return False

    return validate_environment


def exercise_3h_create_deployment_config_generator() -> callable:
    """
    SOLUTION: Create a function that generates deployment-specific configuration files.
    """

    def generate_deployment_config(environment: str, output_dir: str) -> None:
        """Generate deployment configuration files for specified environment"""

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        print(f"🔧 Generating deployment configs for {environment} environment...")

        # Load environment configuration
        config_loader = exercise_3b_implement_config_loader()()
        config = config_loader.load_config(environment)

        # 1. Generate Docker environment file
        _generate_docker_env_file(config, output_path)

        # 2. Generate Kubernetes ConfigMap and Secret
        _generate_kubernetes_configs(config, environment, output_path)

        # 3. Generate CI/CD pipeline configuration
        _generate_cicd_config(config, environment, output_path)

        # 4. Generate monitoring configuration
        _generate_monitoring_config(config, environment, output_path)

        # 5. Generate Terraform infrastructure templates
        _generate_terraform_config(config, environment, output_path)

        print(f"✅ Deployment configs generated in {output_path}")

    def _generate_docker_env_file(config: Dict[str, Any], output_path: Path) -> None:
        """Generate Docker environment file"""
        env_content = []

        # App configuration
        app_config = config.get("app", {})
        env_content.append(f"APP_NAME={app_config.get('name', 'spark-app')}")
        env_content.append(f"APP_VERSION={app_config.get('version', '1.0.0')}")
        env_content.append(f"APP_DEBUG={str(app_config.get('debug', False)).lower()}")

        # Spark configuration
        spark_config = config.get("spark", {})
        env_content.append(f"SPARK_MASTER={spark_config.get('master', 'local[*]')}")
        env_content.append(f"SPARK_DRIVER_MEMORY={spark_config.get('driver', {}).get('memory', '2g')}")
        env_content.append(f"SPARK_EXECUTOR_MEMORY={spark_config.get('executor', {}).get('memory', '2g')}")

        # Data paths
        data_config = config.get("data", {})
        env_content.append(f"INPUT_PATH={data_config.get('input_path', 'data/input')}")
        env_content.append(f"OUTPUT_PATH={data_config.get('output_path', 'data/output')}")

        # Write Docker env file
        docker_env_file = output_path / ".env.docker"
        docker_env_file.write_text("\n".join(env_content))
        print(f"  📄 Generated: {docker_env_file}")

    def _generate_kubernetes_configs(config: Dict[str, Any], environment: str, output_path: Path) -> None:
        """Generate Kubernetes ConfigMap and Secret"""

        # ConfigMap for non-sensitive configuration
        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": f"spark-app-config-{environment}",
                "namespace": f"spark-{environment}"
            },
            "data": {
                "app_name": config.get("app", {}).get("name", "spark-app"),
                "app_version": config.get("app", {}).get("version", "1.0.0"),
                "spark_master": config.get("spark", {}).get("master", "local[*]"),
                "input_path": config.get("data", {}).get("input_path", "data/input"),
                "output_path": config.get("data", {}).get("output_path", "data/output"),
                "log_level": config.get("logging", {}).get("level", "INFO")
            }
        }

        # Secret for sensitive configuration
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": f"spark-app-secrets-{environment}",
                "namespace": f"spark-{environment}"
            },
            "type": "Opaque",
            "data": {
                # Base64 encoded placeholders - in real deployment, these would be actual secrets
                "db_password": "cGxhY2Vob2xkZXI=",  # placeholder
                "aws_access_key": "cGxhY2Vob2xkZXI=",  # placeholder
                "aws_secret_key": "cGxhY2Vob2xkZXI="   # placeholder
            }
        }

        # Write Kubernetes configs
        configmap_file = output_path / f"configmap-{environment}.yaml"
        with open(configmap_file, 'w') as f:
            yaml.dump(configmap, f, default_flow_style=False)
        print(f"  📄 Generated: {configmap_file}")

        secret_file = output_path / f"secret-{environment}.yaml"
        with open(secret_file, 'w') as f:
            yaml.dump(secret, f, default_flow_style=False)
        print(f"  📄 Generated: {secret_file}")

    def _generate_cicd_config(config: Dict[str, Any], environment: str, output_path: Path) -> None:
        """Generate CI/CD pipeline configuration"""

        github_workflow = {
            "name": f"Deploy to {environment.title()}",
            "on": {
                "push": {
                    "branches": [environment if environment != "prod" else "main"]
                }
            },
            "jobs": {
                "deploy": {
                    "runs-on": "ubuntu-latest",
                    "steps": [
                        {
                            "name": "Checkout code",
                            "uses": "actions/checkout@v3"
                        },
                        {
                            "name": "Setup Python",
                            "uses": "actions/setup-python@v4",
                            "with": {
                                "python-version": "3.9"
                            }
                        },
                        {
                            "name": "Install dependencies",
                            "run": "pip install -r requirements.txt"
                        },
                        {
                            "name": "Run tests",
                            "run": "pytest tests/"
                        },
                        {
                            "name": "Build Docker image",
                            "run": f"docker build -t spark-app:{environment} ."
                        },
                        {
                            "name": f"Deploy to {environment}",
                            "run": f"kubectl apply -f k8s/{environment}/"
                        }
                    ]
                }
            }
        }

        workflow_file = output_path / f"deploy-{environment}.yml"
        with open(workflow_file, 'w') as f:
            yaml.dump(github_workflow, f, default_flow_style=False)
        print(f"  📄 Generated: {workflow_file}")

    def _generate_monitoring_config(config: Dict[str, Any], environment: str, output_path: Path) -> None:
        """Generate monitoring configuration"""

        # Prometheus configuration
        prometheus_config = {
            "global": {
                "scrape_interval": "15s"
            },
            "scrape_configs": [
                {
                    "job_name": f"spark-app-{environment}",
                    "static_configs": [
                        {
                            "targets": [f"spark-app-{environment}:4040"]
                        }
                    ]
                }
            ]
        }

        # Grafana dashboard
        grafana_dashboard = {
            "dashboard": {
                "id": None,
                "title": f"Spark Application - {environment.title()}",
                "tags": ["spark", environment],
                "timezone": "browser",
                "panels": [
                    {
                        "id": 1,
                        "title": "Application Status",
                        "type": "stat",
                        "targets": [
                            {
                                "expr": f"up{{job=\"spark-app-{environment}\"}}"
                            }
                        ]
                    },
                    {
                        "id": 2,
                        "title": "Memory Usage",
                        "type": "graph",
                        "targets": [
                            {
                                "expr": f"spark_driver_memory_used{{job=\"spark-app-{environment}\"}}"
                            }
                        ]
                    }
                ],
                "time": {
                    "from": "now-1h",
                    "to": "now"
                },
                "refresh": "5s"
            }
        }

        # Write monitoring configs
        prometheus_file = output_path / f"prometheus-{environment}.yml"
        with open(prometheus_file, 'w') as f:
            yaml.dump(prometheus_config, f, default_flow_style=False)
        print(f"  📄 Generated: {prometheus_file}")

        grafana_file = output_path / f"grafana-dashboard-{environment}.json"
        with open(grafana_file, 'w') as f:
            json.dump(grafana_dashboard, f, indent=2)
        print(f"  📄 Generated: {grafana_file}")

    def _generate_terraform_config(config: Dict[str, Any], environment: str, output_path: Path) -> None:
        """Generate Terraform infrastructure configuration"""

        # Main Terraform configuration
        terraform_main = f'''
# Terraform configuration for {environment} environment
terraform {{
  required_version = ">= 1.0"
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region = var.aws_region
}}

# S3 buckets for data storage
resource "aws_s3_bucket" "input_bucket" {{
  bucket = "${{var.project_name}}-{environment}-input"

  tags = {{
    Environment = "{environment}"
    Project     = var.project_name
  }}
}}

resource "aws_s3_bucket" "output_bucket" {{
  bucket = "${{var.project_name}}-{environment}-output"

  tags = {{
    Environment = "{environment}"
    Project     = var.project_name
  }}
}}

# EKS cluster for Spark workloads
resource "aws_eks_cluster" "spark_cluster" {{
  name     = "${{var.project_name}}-{environment}"
  role_arn = aws_iam_role.cluster_role.arn
  version  = "1.28"

  vpc_config {{
    subnet_ids = var.subnet_ids
  }}

  depends_on = [
    aws_iam_role_policy_attachment.cluster_policy,
  ]
}}
'''

        # Variables file
        terraform_vars = f'''
variable "project_name" {{
  description = "Project name"
  type        = string
  default     = "spark-analytics"
}}

variable "aws_region" {{
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}}

variable "subnet_ids" {{
  description = "Subnet IDs for EKS cluster"
  type        = list(string)
}}

variable "environment" {{
  description = "Environment name"
  type        = string
  default     = "{environment}"
}}
'''

        # Write Terraform configs
        main_tf_file = output_path / f"main-{environment}.tf"
        main_tf_file.write_text(terraform_main)
        print(f"  📄 Generated: {main_tf_file}")

        vars_tf_file = output_path / f"variables-{environment}.tf"
        vars_tf_file.write_text(terraform_vars)
        print(f"  📄 Generated: {vars_tf_file}")

    return generate_deployment_config


def run_complete_solution():
    """Run the complete solution demonstration"""
    print("🚀 Running Complete Solution: Multi-Environment Setup")
    print("=" * 60)

    # Create temporary directory for demonstration
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Step 1: Configuration Hierarchy
        print("\n📊 Step 1: Configuration Hierarchy")
        configs = exercise_3a_create_configuration_hierarchy()

        print("✅ Configuration hierarchy created:")
        for env, config in configs.items():
            sections = list(config.keys())
            print(f"  {env}: {len(sections)} sections - {sections}")

        # Step 2: Config Loader
        print("\n🔧 Step 2: Configuration Loader")
        ConfigLoader = exercise_3b_implement_config_loader()
        loader = ConfigLoader()

        print("✅ Testing configuration loading:")
        for env in ["dev", "staging", "prod"]:
            try:
                config = loader.load_config(env)
                is_valid = loader.validate_config(config)
                print(f"  {env}: {'✅ Valid' if is_valid else '❌ Invalid'}")
            except Exception as e:
                print(f"  {env}: ❌ Error - {e}")

        # Step 3: Secrets Manager
        print("\n🔐 Step 3: Secrets Manager")
        SecretsManager = exercise_3c_implement_secrets_manager()
        secrets = SecretsManager()

        # Set some test secrets
        secrets.set_secret("DB_PASSWORD", "test_password")
        secrets.set_secret("AWS_ACCESS_KEY_ID", "test_access_key")

        print("✅ Secrets manager created:")
        print(f"  DB credentials: {len(secrets.get_database_credentials())} fields")
        print(f"  AWS credentials: {len(secrets.get_aws_credentials())} fields")

        # Step 4: Environment-Aware Spark Session
        print("\n⚡ Step 4: Environment-Aware Spark Session")
        create_spark_session = exercise_3d_create_environment_aware_spark_session()

        print("✅ Spark session creator function created")
        print(f"  Function: {create_spark_session.__name__}")

        # Step 5: Data Path Manager
        print("\n📁 Step 5: Data Path Manager")
        DataPathManager = exercise_3e_implement_data_path_manager()

        print("✅ Testing data path management:")
        for env in ["dev", "staging", "prod"]:
            try:
                path_manager = DataPathManager(env)
                storage_config = path_manager.get_storage_config()
                print(f"  {env}: {storage_config['type']} storage")
            except Exception as e:
                print(f"  {env}: ❌ Error - {e}")

        # Step 6: Feature Flags
        print("\n🚩 Step 6: Feature Flag System")
        FeatureFlags = exercise_3f_create_feature_flag_system()

        print("✅ Testing feature flags:")
        for env in ["dev", "staging", "prod"]:
            try:
                flags = FeatureFlags(env)
                all_flags = flags.get_all_flags()
                enabled_count = sum(all_flags.values())
                print(f"  {env}: {enabled_count}/{len(all_flags)} features enabled")
            except Exception as e:
                print(f"  {env}: ❌ Error - {e}")

        # Step 7: Environment Validator
        print("\n✅ Step 7: Environment Validator")
        validate_environment = exercise_3g_implement_environment_validator()

        print("✅ Running environment validation:")
        validation_results = validate_environment("dev")
        passed_checks = sum(validation_results.values())
        total_checks = len(validation_results)
        print(f"  Dev environment: {passed_checks}/{total_checks} checks passed")

        # Step 8: Deployment Config Generator
        print("\n🚀 Step 8: Deployment Config Generator")
        generate_deployment_config = exercise_3h_create_deployment_config_generator()

        deployment_dir = temp_dir / "deployment_configs"
        generate_deployment_config("staging", str(deployment_dir))

        generated_files = list(deployment_dir.glob("*"))
        print(f"✅ Generated {len(generated_files)} deployment configuration files")

        print("\n🎯 Architecture Benefits:")
        print("  ✅ Environment isolation - separate configs for each environment")
        print("  ✅ Secret management - secure handling of sensitive data")
        print("  ✅ Configuration validation - automated environment checks")
        print("  ✅ Feature flags - runtime feature control")
        print("  ✅ Path management - storage-agnostic data access")
        print("  ✅ Deployment automation - infrastructure as code")

        print("\n📈 Scalability Features:")
        print("  • Hierarchical configuration inheritance")
        print("  • Environment variable resolution")
        print("  • Multiple secret sources (env, files, vault)")
        print("  • Storage system abstraction")
        print("  • Automated deployment configuration generation")

    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("\n🎉 Solution demonstration completed!")
    print("🔧 This setup provides enterprise-grade multi-environment configuration management!")


if __name__ == "__main__":
    run_complete_solution()