"""
Exercise 5: Development Pipeline
===============================

This exercise focuses on setting up an end-to-end development pipeline with
automated quality checks, CI/CD integration, and deployment automation.
You'll create the complete development workflow infrastructure.

Learning Goals:
- Set up automated development workflows
- Implement CI/CD pipelines for Spark applications
- Create deployment automation
- Build monitoring and alerting systems

Instructions:
1. Create pre-commit hooks and quality gates
2. Build CI/CD pipeline configurations
3. Implement deployment automation
4. Set up monitoring and alerting

Run this file: python exercise_5.py
"""

import os
import yaml
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import tempfile
import subprocess
import time
from datetime import datetime


def exercise_5a_create_pre_commit_configuration() -> Dict[str, Any]:
    """
    Create a comprehensive pre-commit configuration for code quality.

    Returns:
        Dictionary containing pre-commit configuration with:
        - Code formatting hooks (black, isort)
        - Linting hooks (flake8, pylint)
        - Security scanning (bandit, safety)
        - Type checking (mypy)
        - Testing hooks (pytest)
        - Documentation checks
        - Custom Spark-specific hooks
    """
    # TODO: Implement this function
    # 1. Create pre-commit configuration with multiple hooks
    # 2. Include Python code quality tools
    # 3. Add security scanning tools
    # 4. Include custom hooks for Spark validation
    # 5. Add documentation and schema validation
    pass


def exercise_5b_create_github_actions_workflow() -> Dict[str, str]:
    """
    Create GitHub Actions workflow files for CI/CD.

    Returns:
        Dictionary mapping workflow names to their YAML content:
        - "ci.yml": Continuous integration workflow
        - "cd.yml": Continuous deployment workflow
        - "quality.yml": Code quality checks
        - "security.yml": Security scanning
        - "performance.yml": Performance testing
    """
    # TODO: Implement this function
    # 1. Create CI workflow for testing and validation
    # 2. Create CD workflow for deployment
    # 3. Add code quality workflow
    # 4. Include security scanning workflow
    # 5. Add performance testing workflow
    pass


def exercise_5c_create_docker_deployment_setup() -> Dict[str, str]:
    """
    Create Docker deployment configuration for different environments.

    Returns:
        Dictionary containing Docker configurations:
        - "Dockerfile.dev": Development Docker image
        - "Dockerfile.prod": Production Docker image
        - "docker-compose.dev.yml": Development environment
        - "docker-compose.prod.yml": Production environment
        - ".dockerignore": Docker ignore patterns
    """
    # TODO: Implement this function
    # 1. Create optimized Dockerfile for development
    # 2. Create production-ready Dockerfile
    # 3. Add docker-compose for local development
    # 4. Create production docker-compose
    # 5. Include proper .dockerignore configuration
    pass


def exercise_5d_create_kubernetes_deployment() -> Dict[str, str]:
    """
    Create Kubernetes deployment manifests for production.

    Returns:
        Dictionary containing Kubernetes YAML configurations:
        - "namespace.yaml": Namespace configuration
        - "deployment.yaml": Application deployment
        - "service.yaml": Service configuration
        - "configmap.yaml": Configuration management
        - "secret.yaml": Secrets management
        - "ingress.yaml": Ingress configuration
    """
    # TODO: Implement this function
    # 1. Create namespace for application
    # 2. Create deployment with proper resource limits
    # 3. Add service configuration
    # 4. Include ConfigMap for application settings
    # 5. Add Secret management for credentials
    # 6. Create Ingress for external access
    pass


def exercise_5e_create_terraform_infrastructure() -> Dict[str, str]:
    """
    Create Terraform infrastructure as code.

    Returns:
        Dictionary containing Terraform configurations:
        - "main.tf": Main infrastructure definition
        - "variables.tf": Variable definitions
        - "outputs.tf": Output definitions
        - "versions.tf": Provider versions
        - "terraform.tfvars.example": Example variables
    """
    # TODO: Implement this function
    # 1. Create main infrastructure resources
    # 2. Define input variables
    # 3. Create output values
    # 4. Specify provider versions
    # 5. Include example variable values
    pass


def exercise_5f_create_monitoring_setup() -> Dict[str, str]:
    """
    Create monitoring and alerting configuration.

    Returns:
        Dictionary containing monitoring configurations:
        - "prometheus.yml": Prometheus configuration
        - "grafana-dashboard.json": Grafana dashboard
        - "alertmanager.yml": Alert manager configuration
        - "docker-compose.monitoring.yml": Monitoring stack
    """
    # TODO: Implement this function
    # 1. Create Prometheus configuration for metrics
    # 2. Build Grafana dashboard for visualization
    # 3. Configure alert manager for notifications
    # 4. Create monitoring stack with docker-compose
    # 5. Include custom metrics for Spark applications
    pass


def exercise_5g_create_deployment_scripts() -> Dict[str, str]:
    """
    Create deployment automation scripts.

    Returns:
        Dictionary containing deployment scripts:
        - "deploy.sh": Main deployment script
        - "rollback.sh": Rollback script
        - "health-check.sh": Health check script
        - "backup.sh": Backup script
        - "migrate.py": Database migration script
    """
    # TODO: Implement this function
    # 1. Create main deployment script with validation
    # 2. Add rollback capability
    # 3. Include health check validation
    # 4. Add backup and recovery scripts
    # 5. Create database migration utilities
    pass


def exercise_5h_create_quality_gates() -> type:
    """
    Create automated quality gates for the development pipeline.

    Returns:
        QualityGates class with methods:
        - check_code_quality() -> Dict[str, bool]
        - check_test_coverage() -> float
        - check_security_scan() -> Dict[str, Any]
        - check_performance_benchmarks() -> Dict[str, float]
        - validate_deployment_readiness() -> bool
    """
    # TODO: Implement this function
    # 1. Create QualityGates class
    # 2. Implement code quality checks
    # 3. Add test coverage validation
    # 4. Include security scanning
    # 5. Add performance benchmarking
    # 6. Create deployment readiness validation
    pass


def exercise_5i_create_pipeline_orchestrator() -> type:
    """
    Create a pipeline orchestrator for managing the entire development workflow.

    Returns:
        PipelineOrchestrator class with methods:
        - run_development_pipeline() -> Dict[str, Any]
        - run_staging_pipeline() -> Dict[str, Any]
        - run_production_pipeline() -> Dict[str, Any]
        - rollback_deployment(version: str) -> bool
        - get_pipeline_status() -> Dict[str, str]
    """
    # TODO: Implement this function
    # 1. Create PipelineOrchestrator class
    # 2. Implement environment-specific pipelines
    # 3. Add rollback capabilities
    # 4. Include pipeline status monitoring
    # 5. Add error handling and notifications
    pass


def create_sample_pipeline_config() -> Dict[str, Any]:
    """Create sample pipeline configuration"""
    return {
        "pipeline": {
            "name": "spark-etl-pipeline",
            "version": "1.0.0",
            "stages": [
                {
                    "name": "code-quality",
                    "steps": ["lint", "format-check", "type-check"],
                    "required": True
                },
                {
                    "name": "testing",
                    "steps": ["unit-tests", "integration-tests", "coverage-check"],
                    "required": True,
                    "coverage_threshold": 80
                },
                {
                    "name": "security",
                    "steps": ["security-scan", "dependency-check"],
                    "required": True
                },
                {
                    "name": "build",
                    "steps": ["docker-build", "image-scan"],
                    "required": True
                },
                {
                    "name": "deploy",
                    "steps": ["deploy-staging", "smoke-tests", "deploy-production"],
                    "required": False,
                    "manual_approval": True
                }
            ]
        },
        "notifications": {
            "slack_webhook": "https://hooks.slack.com/services/...",
            "email_recipients": ["team@company.com"],
            "on_failure": True,
            "on_success": False
        },
        "quality_gates": {
            "test_coverage": 80,
            "code_quality_score": 8.0,
            "security_issues": 0,
            "performance_threshold": 5.0
        }
    }


def demonstrate_pipeline_concepts():
    """Demonstrate CI/CD pipeline concepts"""
    print("\n🔄 CI/CD Pipeline Concepts")
    print("=" * 35)

    print("\n📊 Pipeline Stages:")
    print("  1. 🔍 Code Quality - Linting, formatting, type checking")
    print("  2. 🧪 Testing - Unit, integration, performance tests")
    print("  3. 🔒 Security - Vulnerability scanning, dependency checks")
    print("  4. 🏗️  Build - Docker images, artifacts, packaging")
    print("  5. 🚀 Deploy - Staging, testing, production deployment")

    print("\n✅ Quality Gates:")
    print("  • Code coverage > 80%")
    print("  • No security vulnerabilities")
    print("  • Performance benchmarks met")
    print("  • All tests passing")
    print("  • Code quality score > 8.0")

    print("\n🛠️  Tools Integration:")
    print("  • Git hooks for pre-commit validation")
    print("  • GitHub Actions for CI/CD automation")
    print("  • Docker for containerization")
    print("  • Kubernetes for orchestration")
    print("  • Terraform for infrastructure")
    print("  • Prometheus/Grafana for monitoring")

    print("\n🔄 Deployment Strategies:")
    print("  • Blue-Green deployments")
    print("  • Rolling updates")
    print("  • Canary releases")
    print("  • Feature flags")
    print("  • Automatic rollback")


def simulate_pipeline_execution():
    """Simulate pipeline execution"""
    print("\n🚀 Simulating Pipeline Execution")
    print("=" * 40)

    stages = [
        ("🔍 Code Quality Check", 2),
        ("🧪 Running Tests", 3),
        ("🔒 Security Scan", 2),
        ("🏗️  Building Artifacts", 4),
        ("🚀 Deploying to Staging", 3),
        ("✅ Production Deployment", 2)
    ]

    for stage_name, duration in stages:
        print(f"\n{stage_name}...")
        for i in range(duration):
            time.sleep(0.5)  # Simulate work
            print(".", end="", flush=True)
        print(" ✅ PASSED")

    print(f"\n🎉 Pipeline completed successfully!")
    print(f"⏱️  Total time: {sum(duration for _, duration in stages) * 0.5:.1f} seconds")


def run_exercises():
    """Run all exercises and display results."""
    print("🚀 Running Exercise 5: Development Pipeline")
    print("=" * 50)

    # Demonstrate pipeline concepts
    demonstrate_pipeline_concepts()

    # Test Exercise 5a
    print("\n📝 Exercise 5a: Create Pre-commit Configuration")
    try:
        pre_commit_config = exercise_5a_create_pre_commit_configuration()

        if pre_commit_config:
            print("✅ Pre-commit configuration created")
            if 'repos' in pre_commit_config:
                print(f"  Repositories: {len(pre_commit_config['repos'])}")
            print(f"  Configuration keys: {list(pre_commit_config.keys())}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5b
    print("\n📝 Exercise 5b: Create GitHub Actions Workflow")
    try:
        workflows = exercise_5b_create_github_actions_workflow()

        if workflows:
            print(f"✅ {len(workflows)} GitHub Actions workflows created:")
            for workflow_name in workflows.keys():
                print(f"  - {workflow_name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5c
    print("\n📝 Exercise 5c: Create Docker Deployment Setup")
    try:
        docker_configs = exercise_5c_create_docker_deployment_setup()

        if docker_configs:
            print(f"✅ {len(docker_configs)} Docker configurations created:")
            for config_name in docker_configs.keys():
                print(f"  - {config_name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5d
    print("\n📝 Exercise 5d: Create Kubernetes Deployment")
    try:
        k8s_configs = exercise_5d_create_kubernetes_deployment()

        if k8s_configs:
            print(f"✅ {len(k8s_configs)} Kubernetes configurations created:")
            for config_name in k8s_configs.keys():
                print(f"  - {config_name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5e
    print("\n📝 Exercise 5e: Create Terraform Infrastructure")
    try:
        terraform_configs = exercise_5e_create_terraform_infrastructure()

        if terraform_configs:
            print(f"✅ {len(terraform_configs)} Terraform configurations created:")
            for config_name in terraform_configs.keys():
                print(f"  - {config_name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5f
    print("\n📝 Exercise 5f: Create Monitoring Setup")
    try:
        monitoring_configs = exercise_5f_create_monitoring_setup()

        if monitoring_configs:
            print(f"✅ {len(monitoring_configs)} monitoring configurations created:")
            for config_name in monitoring_configs.keys():
                print(f"  - {config_name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5g
    print("\n📝 Exercise 5g: Create Deployment Scripts")
    try:
        deployment_scripts = exercise_5g_create_deployment_scripts()

        if deployment_scripts:
            print(f"✅ {len(deployment_scripts)} deployment scripts created:")
            for script_name in deployment_scripts.keys():
                print(f"  - {script_name}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5h
    print("\n📝 Exercise 5h: Create Quality Gates")
    try:
        QualityGates = exercise_5h_create_quality_gates()

        if QualityGates:
            print(f"✅ QualityGates class created: {QualityGates.__name__}")
            methods = [m for m in dir(QualityGates) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Test Exercise 5i
    print("\n📝 Exercise 5i: Create Pipeline Orchestrator")
    try:
        PipelineOrchestrator = exercise_5i_create_pipeline_orchestrator()

        if PipelineOrchestrator:
            print(f"✅ PipelineOrchestrator class created: {PipelineOrchestrator.__name__}")
            methods = [m for m in dir(PipelineOrchestrator) if not m.startswith('_')]
            print(f"  Methods: {methods}")
        else:
            print("❌ Function returned None - needs implementation")
    except Exception as e:
        print(f"❌ Error: {e}")

    # Show sample pipeline configuration
    print("\n📋 Sample Pipeline Configuration:")
    print("=" * 40)

    sample_config = create_sample_pipeline_config()
    print(f"Pipeline: {sample_config['pipeline']['name']}")
    print(f"Stages: {len(sample_config['pipeline']['stages'])}")

    for stage in sample_config['pipeline']['stages']:
        required = "✅ Required" if stage['required'] else "⚠️  Optional"
        print(f"  • {stage['name']}: {len(stage['steps'])} steps ({required})")

    print(f"\nQuality Gates:")
    for gate, threshold in sample_config['quality_gates'].items():
        print(f"  • {gate}: {threshold}")

    # Simulate pipeline execution
    simulate_pipeline_execution()

    print("\n🎉 Exercise 5 completed!")
    print("🔄 Outstanding work on development pipelines!")
    print("🏗️  You've mastered CI/CD automation for Spark applications!")
    print("🚀 Ready to move on to the next lesson!")


if __name__ == "__main__":
    run_exercises()