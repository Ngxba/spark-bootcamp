"""
Exercise 3: Multi-Environment Setup
==================================

This exercise focuses on implementing configuration management for different
environments (dev/staging/production). You'll create flexible configuration
systems that can adapt to various deployment scenarios.

Learning Goals:
- Implement environment-specific configurations
- Handle secrets and credentials securely
- Create flexible configuration hierarchies
- Build environment-aware applications

Instructions:
1. Create configuration files for different environments
2. Implement configuration loading with environment variables
3. Add secrets management
4. Create environment-aware Spark jobs

Run this file: python exercise_3.py
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import tempfile
import json
from enum import Enum


class Environment(Enum):
    """Supported environments"""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


def exercise_3a_create_configuration_hierarchy() -> Dict[str, Dict[str, Any]]:
    """
    Create a hierarchical configuration system with base and environment-specific configs.

    Returns:
        Dictionary containing configuration for each environment:
        {
            "base": {...},
            "dev": {...},
            "staging": {...},
            "prod": {...}
        }

    Configuration should include:
    - Application settings (name, version, debug mode)
    - Spark configurations (master, executors, memory)
    - Data paths (input, output, temp)
    - Database settings (host, port, credentials)
    - Logging configurations
    - Feature flags
    """
    # TODO: Implement this function
    # 1. Create base configuration with common settings
    # 2. Create dev configuration with local development settings
    # 3. Create staging configuration with staging-specific settings
    # 4. Create prod configuration with production settings
    # 5. Use environment variables for sensitive data in prod
    pass


def exercise_3b_implement_config_loader() -> type:
    """
    Create a configuration loader that can merge base and environment configs.

    Returns:
        ConfigLoader class with methods:
        - load_config(environment: str) -> Dict
        - resolve_environment_variables(config: Dict) -> Dict
        - validate_config(config: Dict) -> bool
        - get_database_url(config: Dict) -> str
        - get_spark_config(config: Dict) -> Dict
    """
    # TODO: Implement this function
    # 1. Create ConfigLoader class
    # 2. Implement configuration merging (environment overrides base)
    # 3. Add environment variable resolution (${VAR_NAME} format)
    # 4. Add configuration validation
    # 5. Add helper methods for common configurations
    pass


def exercise_3c_implement_secrets_manager() -> type:
    """
    Create a secrets management system for handling sensitive data.

    Returns:
        SecretsManager class with methods:
        - get_secret(key: str, default: Optional[str]) -> Optional[str]
        - set_secret(key: str, value: str) -> None
        - load_from_file(file_path: str) -> None
        - get_database_credentials() -> Dict[str, str]
        - get_aws_credentials() -> Dict[str, str]
    """
    # TODO: Implement this function
    # 1. Create SecretsManager class
    # 2. Support multiple secret sources (env vars, files, vault)
    # 3. Add credential helpers for common services
    # 4. Include secret validation and encryption
    # 5. Add audit logging for secret access
    pass


def exercise_3d_create_environment_aware_spark_session() -> callable:
    """
    Create a function that creates Spark sessions based on environment configuration.

    Returns:
        Function: create_spark_session(environment: str, app_name: str) -> SparkSession

    The function should:
    - Load environment-specific Spark configurations
    - Set appropriate master URL for each environment
    - Configure memory and executor settings
    - Set up logging and monitoring
    - Handle authentication for production environments
    """
    # TODO: Implement this function
    # 1. Create function that takes environment and app_name
    # 2. Load configuration for the specified environment
    # 3. Configure Spark session based on environment
    # 4. Add environment-specific optimizations
    # 5. Include proper error handling and validation
    pass


def exercise_3e_implement_data_path_manager() -> type:
    """
    Create a data path manager that handles different storage systems per environment.

    Returns:
        DataPathManager class with methods:
        - get_input_path(dataset: str) -> str
        - get_output_path(dataset: str) -> str
        - get_temp_path() -> str
        - ensure_paths_exist() -> None
        - get_storage_config() -> Dict[str, Any]
    """
    # TODO: Implement this function
    # 1. Create DataPathManager class
    # 2. Handle local paths for dev, S3/HDFS for prod
    # 3. Add path validation and creation
    # 4. Support different storage systems
    # 5. Include path templating and parameterization
    pass


def exercise_3f_create_feature_flag_system() -> type:
    """
    Create a feature flag system for environment-specific functionality.

    Returns:
        FeatureFlags class with methods:
        - is_enabled(feature: str) -> bool
        - enable_feature(feature: str) -> None
        - disable_feature(feature: str) -> None
        - get_all_flags() -> Dict[str, bool]
        - load_from_config(config: Dict) -> None
    """
    # TODO: Implement this function
    # 1. Create FeatureFlags class
    # 2. Support environment-specific feature toggles
    # 3. Add runtime feature flag modification
    # 4. Include feature flag validation
    # 5. Add logging for feature flag changes
    pass


def exercise_3g_implement_environment_validator() -> callable:
    """
    Create a validator that ensures environment setup is correct.

    Returns:
        Function: validate_environment(environment: str) -> Dict[str, bool]

    The validator should check:
    - Required environment variables are set
    - Configuration files exist and are valid
    - Database connections can be established
    - Storage paths are accessible
    - Spark can be initialized
    - All required secrets are available
    """
    # TODO: Implement this function
    # 1. Create comprehensive environment validation
    # 2. Check all required components
    # 3. Test connectivity to external services
    # 4. Validate file permissions and access
    # 5. Return detailed validation results
    pass


def exercise_3h_create_deployment_config_generator() -> callable:
    """
    Create a function that generates deployment-specific configuration files.

    Returns:
        Function: generate_deployment_config(environment: str, output_dir: str) -> None

    The function should generate:
    - Docker environment files
    - Kubernetes ConfigMaps and Secrets
    - CI/CD pipeline configurations
    - Monitoring and alerting configs
    - Infrastructure as Code templates
    """
    # TODO: Implement this function
    # 1. Create deployment configuration templates
    # 2. Generate environment-specific configs
    # 3. Include security best practices
    # 4. Add validation for generated configs
    # 5. Support multiple deployment platforms
    pass


def create_sample_environment_files(temp_dir: Path) -> Dict[str, Path]:
    """
    Create sample configuration files for testing.
    """
    configs_dir = temp_dir / "configs"
    configs_dir.mkdir(exist_ok=True)

    # Base configuration
    base_config = {
        "app": {
            "name": "data-pipeline",
            "version": "1.0.0"
        },
        "spark": {
            "sql": {"shuffle_partitions": 200},
            "serializer": "org.apache.spark.serializer.KryoSerializer"
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }

    # Development configuration
    dev_config = {
        "app": {"debug": True},
        "spark": {
            "master": "local[2]",
            "sql": {"shuffle_partitions": 4}
        },
        "data": {
            "input_path": "data/dev/input",
            "output_path": "data/dev/output"
        },
        "database": {
            "host": "localhost",
            "port": 5432,
            "name": "dev_db"
        }
    }

    # Production configuration
    prod_config = {
        "app": {"debug": False},
        "spark": {
            "master": "yarn",
            "sql": {"shuffle_partitions": 1000}
        },
        "data": {
            "input_path": "${INPUT_S3_BUCKET}/input",
            "output_path": "${OUTPUT_S3_BUCKET}/output"
        },
        "database": {
            "host": "${DB_HOST}",
            "port": "${DB_PORT}",
            "name": "${DB_NAME}"
        }
    }

    # Write configuration files
    files = {}
    for name, config in [("base", base_config), ("dev", dev_config), ("prod", prod_config)]:
        file_path = configs_dir / f"{name}.yaml"
        with open(file_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        files[name] = file_path

    return files


def test_environment_setup():
    """
    Test environment setup with sample configurations.
    """
    print("\n🧪 Testing Environment Setup")
    print("=" * 40)

    # Create temporary directory with sample configs
    temp_dir = Path(tempfile.mkdtemp())
    config_files = create_sample_environment_files(temp_dir)

    print(f"📁 Created test configurations:")
    for name, path in config_files.items():
        print(f"  - {name}: {path}")

    # Set some environment variables for testing
    test_env_vars = {
        "INPUT_S3_BUCKET": "s3://prod-input-bucket",
        "OUTPUT_S3_BUCKET": "s3://prod-output-bucket",
        "DB_HOST": "prod-db.company.com",
        "DB_PORT": "5432",
        "DB_NAME": "prod_analytics"
    }

    print(f"\n🔧 Setting test environment variables:")
    for key, value in test_env_vars.items():
        os.environ[key] = value
        print(f"  {key}={value}")

    return temp_dir, config_files


def run_exercises():
    """Run all exercises and display results."""
    print("🚀 Running Exercise 3: Multi-Environment Setup")
    print("=" * 55)

    # Setup test environment
    temp_dir, config_files = test_environment_setup()

    try:
        # Test Exercise 3a
        print("\n📝 Exercise 3a: Create Configuration Hierarchy")
        try:
            configs = exercise_3a_create_configuration_hierarchy()

            if configs:
                print(f"✅ Configuration hierarchy created with {len(configs)} environments:")
                for env, config in configs.items():
                    print(f"  - {env}: {len(config)} sections")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3b
        print("\n📝 Exercise 3b: Implement Config Loader")
        try:
            ConfigLoader = exercise_3b_implement_config_loader()

            if ConfigLoader:
                print(f"✅ ConfigLoader class created: {ConfigLoader.__name__}")
                methods = [m for m in dir(ConfigLoader) if not m.startswith('_')]
                print(f"  Methods: {methods}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3c
        print("\n📝 Exercise 3c: Implement Secrets Manager")
        try:
            SecretsManager = exercise_3c_implement_secrets_manager()

            if SecretsManager:
                print(f"✅ SecretsManager class created: {SecretsManager.__name__}")
                methods = [m for m in dir(SecretsManager) if not m.startswith('_')]
                print(f"  Methods: {methods}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3d
        print("\n📝 Exercise 3d: Environment-Aware Spark Session")
        try:
            create_spark_session = exercise_3d_create_environment_aware_spark_session()

            if create_spark_session:
                print("✅ Environment-aware Spark session function created")
                print(f"  Function: {create_spark_session.__name__}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3e
        print("\n📝 Exercise 3e: Implement Data Path Manager")
        try:
            DataPathManager = exercise_3e_implement_data_path_manager()

            if DataPathManager:
                print(f"✅ DataPathManager class created: {DataPathManager.__name__}")
                methods = [m for m in dir(DataPathManager) if not m.startswith('_')]
                print(f"  Methods: {methods}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3f
        print("\n📝 Exercise 3f: Create Feature Flag System")
        try:
            FeatureFlags = exercise_3f_create_feature_flag_system()

            if FeatureFlags:
                print(f"✅ FeatureFlags class created: {FeatureFlags.__name__}")
                methods = [m for m in dir(FeatureFlags) if not m.startswith('_')]
                print(f"  Methods: {methods}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3g
        print("\n📝 Exercise 3g: Implement Environment Validator")
        try:
            validate_environment = exercise_3g_implement_environment_validator()

            if validate_environment:
                print("✅ Environment validator function created")
                print(f"  Function: {validate_environment.__name__}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Test Exercise 3h
        print("\n📝 Exercise 3h: Create Deployment Config Generator")
        try:
            generate_deployment_config = exercise_3h_create_deployment_config_generator()

            if generate_deployment_config:
                print("✅ Deployment config generator function created")
                print(f"  Function: {generate_deployment_config.__name__}")
            else:
                print("❌ Function returned None - needs implementation")
        except Exception as e:
            print(f"❌ Error: {e}")

        # Show environment examples
        print("\n📊 Environment Configuration Examples:")
        print("=" * 45)

        environments = ["dev", "staging", "prod"]
        for env in environments:
            print(f"\n🔧 {env.upper()} Environment:")
            print(f"  • Purpose: {'Development' if env == 'dev' else 'Staging' if env == 'staging' else 'Production'}")
            print(f"  • Spark: {'local[2]' if env == 'dev' else 'yarn-cluster'}")
            print(f"  • Storage: {'Local files' if env == 'dev' else 'S3/HDFS'}")
            print(f"  • Security: {'Minimal' if env == 'dev' else 'Full encryption'}")
            print(f"  • Monitoring: {'Basic' if env == 'dev' else 'Comprehensive'}")

    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("\n🎉 Exercise 3 completed!")
    print("🔐 Excellent work on multi-environment configuration!")
    print("⚙️  You've mastered environment-specific deployments!")
    print("🚀 Ready for Exercise 4!")


if __name__ == "__main__":
    run_exercises()