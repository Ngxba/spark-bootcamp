#!/usr/bin/env python3
"""
Demonstration Script for My Spark Project

This script provides a quick demonstration of the project's capabilities.
It showcases the ETL pipeline, data transformations, and analytics features.
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.jobs.etl_job import ETLJob
from src.utils.file_utils import FileUtils
from src.utils.spark_utils import SparkUtils


def create_demo_data():
    """Create demonstration data files."""
    print("🔧 Creating demonstration data...")

    # Ensure data directory exists
    data_dir = "data"
    FileUtils.create_directory(data_dir)

    # Enhanced sample data for better demonstration
    customers_data = """customer_id,customer_name,age,gender,city,customer_segment
CUST001,John Doe,35,Male,New York,PREMIUM
CUST002,Jane Smith,28,Female,Los Angeles,STANDARD
CUST003,Bob Johnson,42,Male,Chicago,PREMIUM
CUST004,Alice Brown,31,Female,Houston,STANDARD
CUST005,Charlie Davis,26,Male,Phoenix,BASIC
CUST006,Diana Wilson,45,Female,Seattle,PREMIUM
CUST007,Frank Miller,33,Male,Boston,STANDARD
CUST008,Grace Lee,29,Female,Miami,BASIC
CUST009,Henry Garcia,37,Male,Denver,PREMIUM
CUST010,Ivy Chen,24,Female,Portland,BASIC"""

    products_data = """product_id,product_name,category,brand,price,supplier
PROD001,MacBook Pro,Electronics,Apple,1999.99,TechDistributor
PROD002,Wireless Mouse,Electronics,Logitech,79.99,OfficeSupplies
PROD003,Coffee Mug,Home,HomeGoods,15.99,KitchenCorp
PROD004,Notebook Set,Office,Moleskine,24.99,PaperSupply
PROD005,Ergonomic Chair,Furniture,Herman Miller,799.99,OfficeFurniture
PROD006,USB-C Hub,Electronics,Anker,49.99,TechDistributor
PROD007,Desk Lamp,Home,IKEA,35.99,FurnitureWorld
PROD008,Water Bottle,Home,Hydro Flask,39.99,OutdoorGear
PROD009,Mechanical Keyboard,Electronics,Keychron,149.99,TechDistributor
PROD010,Standing Desk,Furniture,Uplift,599.99,OfficeFurniture"""

    sales_data = """transaction_id,customer_id,product_id,quantity,unit_price,total_amount,transaction_date
TXN001,CUST001,PROD001,1,1999.99,1999.99,2024-01-15
TXN002,CUST002,PROD002,2,79.99,159.98,2024-01-16
TXN003,CUST001,PROD003,3,15.99,47.97,2024-01-17
TXN004,CUST003,PROD005,1,799.99,799.99,2024-01-18
TXN005,CUST002,PROD004,5,24.99,124.95,2024-01-19
TXN006,CUST004,PROD010,1,599.99,599.99,2024-01-20
TXN007,CUST005,PROD006,2,49.99,99.98,2024-01-21
TXN008,CUST003,PROD007,1,35.99,35.99,2024-01-22
TXN009,CUST001,PROD009,1,149.99,149.99,2024-01-23
TXN010,CUST006,PROD001,1,1999.99,1999.99,2024-01-24
TXN011,CUST007,PROD008,3,39.99,119.97,2024-01-25
TXN012,CUST008,PROD003,2,15.99,31.98,2024-01-26
TXN013,CUST009,PROD002,1,79.99,79.99,2024-01-27
TXN014,CUST010,PROD004,10,24.99,249.90,2024-01-28
TXN015,CUST004,PROD006,1,49.99,49.99,2024-02-01
TXN016,CUST002,PROD007,2,35.99,71.98,2024-02-02
TXN017,CUST005,PROD008,1,39.99,39.99,2024-02-03
TXN018,CUST001,PROD010,1,599.99,599.99,2024-02-04
TXN019,CUST006,PROD009,1,149.99,149.99,2024-02-05
TXN020,CUST003,PROD003,5,15.99,79.95,2024-02-06"""

    # Write data files
    FileUtils.write_text_file(customers_data, os.path.join(data_dir, "customers.csv"))
    FileUtils.write_text_file(products_data, os.path.join(data_dir, "products.csv"))
    FileUtils.write_text_file(sales_data, os.path.join(data_dir, "sales.csv"))

    print("✅ Demonstration data created successfully!")


def run_etl_pipeline():
    """Run the ETL pipeline demonstration."""
    print("\n🚀 Running ETL Pipeline...")

    config = {
        "input_paths": {
            "customers": "data/customers.csv",
            "products": "data/products.csv",
            "sales": "data/sales.csv",
        },
        "output_path": "data/output",
        "spark_config": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.ui.showConsoleProgress": "false",  # Reduce console noise for demo
        },
    }

    # Create and run ETL job
    etl_job = ETLJob(config)
    etl_job.execute()

    print("✅ ETL Pipeline completed successfully!")


def display_results():
    """Display the results of the ETL pipeline."""
    print("\n📊 Analysis Results:")
    print("=" * 50)

    output_dir = "data/output"

    if not FileUtils.directory_exists(output_dir):
        print("❌ No output directory found. Please run the ETL pipeline first.")
        return

    # List generated datasets
    output_subdirs = [
        d for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))
    ]

    print(f"📈 Generated {len(output_subdirs)} analytical datasets:")
    for i, subdir in enumerate(sorted(output_subdirs), 1):
        print(f"  {i}. {subdir.replace('_', ' ').title()}")

    print("\n💡 Key Insights:")
    print("  • Customer analytics with purchase behavior")
    print("  • Product performance rankings")
    print("  • Category-wise sales analysis")
    print("  • Time-based sales trends")
    print("  • Customer segmentation (RFM analysis)")

    print(f"\n📁 Results saved to: {os.path.abspath(output_dir)}")


def display_project_features():
    """Display key features of the project."""
    print("\n🏗️  Project Architecture Features:")
    print("=" * 50)

    features = [
        "✅ Modular Spark job architecture",
        "✅ Reusable data transformation components",
        "✅ Comprehensive data cleaning utilities",
        "✅ Business analytics and aggregations",
        "✅ Schema validation and type safety",
        "✅ Configuration management system",
        "✅ Unit and integration testing",
        "✅ Performance optimization utilities",
        "✅ Production-ready error handling",
        "✅ Detailed logging and monitoring",
    ]

    for feature in features:
        print(f"  {feature}")

    print("\n🎯 Learning Objectives Demonstrated:")
    print("  • Production-ready Spark application structure")
    print("  • Industry best practices for data pipelines")
    print("  • Testing strategies for data applications")
    print("  • Configuration and environment management")
    print("  • Performance optimization techniques")


def main():
    """Main demonstration function."""
    print("🎉 Welcome to My Spark Project Demonstration!")
    print("=" * 60)

    try:
        # Step 1: Create demonstration data
        create_demo_data()

        # Step 2: Run ETL pipeline
        run_etl_pipeline()

        # Step 3: Display results
        display_results()

        # Step 4: Show project features
        display_project_features()

        print("\n🎊 Demonstration completed successfully!")
        print("\n📚 Next Steps:")
        print("  • Explore the generated analytics in data/output/")
        print("  • Run tests with: make test")
        print("  • Try different configurations")
        print("  • Examine the source code structure")
        print("  • Build your own transformations!")

    except Exception as e:
        print(f"\n❌ Demonstration failed: {str(e)}")
        print("Please check the logs for more details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
