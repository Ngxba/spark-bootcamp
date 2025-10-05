"""
Sample Datasets for Spark DataFrame Exercises
=============================================

This module provides rich, realistic sample datasets for all exercises.
These datasets are designed to be educational and demonstrate real-world scenarios.
"""

import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple


class SampleDataGenerator:
    """Generate realistic sample datasets for Spark DataFrame exercises."""

    def __init__(self, seed: int = 42):
        """Initialize with a seed for reproducible results."""
        random.seed(seed)

    # Exercise 1 Data: Basic DataFrame Operations
    def get_employee_data(self) -> List[Tuple]:
        """Generate employee data for basic DataFrame operations."""
        return [
            ("Alice Johnson", 28, "Engineering", 85000),
            ("Bob Chen", 34, "Marketing", 65000),
            ("Charlie Davis", 29, "Engineering", 78000),
            ("Diana Rodriguez", 31, "Sales", 72000),
            ("Eve Wilson", 26, "Engineering", 92000),
            ("Frank Thompson", 45, "Marketing", 75000),
            ("Grace Kim", 33, "Sales", 68000),
            ("Henry Brown", 38, "Engineering", 95000),
            ("Iris Martinez", 27, "Data Science", 88000),
            ("Jack Taylor", 42, "Sales", 70000),
        ]

    def get_product_catalog(self) -> List[Tuple]:
        """Generate product catalog for DataFrame operations."""
        return [
            ("LAPTOP001", "Gaming Laptop", "Electronics", 1299.99, 25),
            ("MOUSE002", "Wireless Mouse", "Electronics", 29.99, 150),
            ("CHAIR003", "Ergonomic Chair", "Furniture", 249.99, 40),
            ("DESK004", "Standing Desk", "Furniture", 399.99, 15),
            ("PHONE005", "Smartphone", "Electronics", 699.99, 80),
            ("HEADSET006", "Gaming Headset", "Electronics", 89.99, 60),
            ("MONITOR007", "4K Monitor", "Electronics", 349.99, 30),
            ("KEYBOARD008", "Mechanical Keyboard", "Electronics", 129.99, 45),
        ]

    # Exercise 2 Data: DataFrame Transformations
    def get_customer_data(self) -> List[Tuple]:
        """Generate customer data for transformations."""
        return [
            (1, "john doe", "john@email.com", "555-1234", 32),
            (2, "jane smith", "jane@company.org", None, 28),
            (3, "bob wilson", None, "555-5678", 45),
            (4, "alice cooper", "alice@test.net", "555-9999", 37),
            (5, "charlie brown", "charlie@example.com", "555-1111", 29),
        ]

    def get_sales_transactions(self) -> List[Tuple]:
        """Generate sales transaction data."""
        return [
            ("TXN001", "Widget A", 50, 10.0, 0.1),
            ("TXN002", "Widget B", 150, 25.0, 0.15),
            ("TXN003", "Widget C", 5, 100.0, 0.05),
            ("TXN004", "Gadget X", 75, 45.0, 0.12),
            ("TXN005", "Gadget Y", 200, 15.0, 0.08),
            ("TXN006", "Tool Alpha", 30, 80.0, 0.2),
            ("TXN007", "Tool Beta", 10, 150.0, 0.1),
        ]

    # Exercise 3 Data: Spark SQL
    def get_comprehensive_employee_data(self) -> List[Tuple]:
        """Generate comprehensive employee data for SQL operations."""
        return [
            ("Alice Johnson", "Engineering", 85000, 28),
            ("Bob Chen", "Marketing", 65000, 34),
            ("Charlie Davis", "Engineering", 78000, 29),
            ("Diana Rodriguez", "Sales", 72000, 31),
            ("Eve Wilson", "Engineering", 92000, 26),
            ("Frank Thompson", "Marketing", 75000, 45),
            ("Grace Kim", "Sales", 68000, 33),
            ("Henry Brown", "Engineering", 95000, 38),
            ("Iris Martinez", "Data Science", 88000, 27),
            ("Jack Taylor", "Sales", 70000, 42),
            ("Karen Lee", "HR", 62000, 35),
            ("Liam O'Connor", "Finance", 73000, 31),
            ("Maya Patel", "Data Science", 91000, 29),
            ("Noah Garcia", "Engineering", 87000, 26),
        ]

    def get_sales_by_region(self) -> List[Tuple]:
        """Generate regional sales data."""
        return [
            ("Laptop Pro", "Electronics", 1200, "North"),
            ("Mouse Deluxe", "Electronics", 45, "South"),
            ("Office Chair", "Furniture", 150, "North"),
            ("Standing Desk", "Furniture", 300, "East"),
            ("Smartphone X", "Electronics", 800, "West"),
            ("Coffee Table", "Furniture", 200, "South"),
            ("Tablet Pro", "Electronics", 400, "North"),
            ("Bookshelf", "Furniture", 120, "East"),
            ("Wireless Speaker", "Electronics", 150, "West"),
            ("Dining Chair", "Furniture", 80, "South"),
        ]

    # Exercise 4 Data: Joins
    def get_customer_order_data(self) -> Tuple[List[Tuple], List[Tuple]]:
        """Generate customer and order data for join operations."""
        customers = [
            (1, "Alice Johnson", "New York", 28),
            (2, "Bob Smith", "Los Angeles", 34),
            (3, "Charlie Brown", "Chicago", 29),
            (4, "Diana Wilson", "Houston", 31),
            (5, "Eve Davis", "Phoenix", 26),
            (6, "Frank Miller", "Philadelphia", 45),
        ]

        orders = [
            (101, 1, 250.0, "Completed"),
            (102, 1, 150.0, "Completed"),
            (103, 2, 300.0, "Pending"),
            (104, 3, 75.0, "Completed"),
            (105, 3, 200.0, "Completed"),
            (106, 4, 400.0, "Shipped"),
            (107, 2, 125.0, "Completed"),
            (108, 5, 350.0, "Processing"),
            # Note: Customer 6 (Frank) has no orders
        ]

        return customers, orders

    def get_product_inventory_data(self) -> Tuple[List[Tuple], List[Tuple]]:
        """Generate product and inventory data for joins."""
        products = [
            ("PROD001", "Gaming Laptop", "Electronics"),
            ("PROD002", "Wireless Mouse", "Electronics"),
            ("PROD003", "Office Chair", "Furniture"),
            ("PROD005", "Smartphone", "Electronics"),
            # Note: PROD004 in inventory but not in products
        ]

        inventory = [
            ("PROD001", "Warehouse A", 50, "2023-01-15"),
            ("PROD001", "Warehouse B", 30, "2023-01-15"),
            ("PROD002", "Warehouse A", 100, "2023-01-16"),
            ("PROD003", "Warehouse C", 25, "2023-01-17"),
            ("PROD004", "Warehouse B", 15, "2023-01-18"),  # Unknown product
            ("PROD005", "Warehouse A", 75, "2023-01-19"),
        ]

        return products, inventory

    # Exercise 5 Data: Column Functions
    def get_people_contact_data(self) -> List[Tuple]:
        """Generate people contact data for string functions."""
        return [
            ("John Smith Jr.", "john.smith@email.com", "(555) 123-4567"),
            ("Jane Mary Doe", "jane.doe@company.org", "555.987.6543"),
            ("Bob Johnson", "bob@test.net", "555-111-2222"),
            ("Alice Cooper Williams", "alice.williams@corp.com", "(555) 444-7890"),
            ("Charlie Brown", "charlie@example.net", "555 333 1234"),
            ("Diana Prince", "diana.prince@hero.com", "+1-555-555-5555"),
        ]

    def get_event_schedule_data(self) -> List[Tuple]:
        """Generate event schedule data for date functions."""
        return [
            ("Annual Conference", "2024-06-15", 3),
            ("Team Workshop", "2023-12-01", 1),
            ("Quarterly Meeting", "2024-01-30", 0),
            ("Training Session", "2024-03-20", 2),
            ("Company Retreat", "2024-07-10", 4),
            ("Product Launch", "2024-05-05", 1),
            ("Board Meeting", "2024-02-14", 0),
        ]

    def get_ecommerce_orders(self) -> List[Tuple]:
        """Generate e-commerce order data for complex processing."""
        return [
            ("ORD001", "alice@company.com", "250.50", "2023-11-15", " completed "),
            ("ORD002", "bob@email.org", "750.00", "2023-12-01", "PENDING"),
            ("ORD003", None, "0.00", "2023-10-20", "cancelled"),
            ("ORD004", "charlie@test.net", "125.75", "2023-11-30", "COMPLETED"),
            ("ORD005", "diana@example.com", "899.99", "2023-12-15", "SHIPPED"),
            ("ORD006", "eve@corp.net", "-50.00", "2023-12-20", "refunded"),
            ("ORD007", "frank@business.org", "1200.00", "2023-12-25", "  COMPLETED  "),
        ]

    # Exercise 6 Data: Analytics and Aggregations
    def get_regional_sales_data(self) -> List[Tuple]:
        """Generate regional sales data for analytics."""
        return [
            ("Electronics", "North", 1500.00, 3),
            ("Electronics", "South", 2200.00, 4),
            ("Electronics", "East", 1800.00, 2),
            ("Electronics", "West", 1900.00, 5),
            ("Clothing", "North", 800.00, 8),
            ("Clothing", "South", 1200.00, 12),
            ("Clothing", "East", 950.00, 7),
            ("Books", "North", 300.00, 15),
            ("Books", "South", 450.00, 20),
            ("Books", "West", 275.00, 18),
            ("Furniture", "East", 1100.00, 6),
            ("Furniture", "West", 1350.00, 4),
        ]

    def get_employee_performance_data(self) -> List[Tuple]:
        """Generate employee performance data for ranking."""
        return [
            ("Alice Thompson", "Engineering", 95000, 8),
            ("Bob Rodriguez", "Engineering", 87000, 5),
            ("Charlie Kim", "Marketing", 75000, 6),
            ("Diana Patel", "Marketing", 82000, 7),
            ("Eve Johnson", "Engineering", 92000, 4),
            ("Frank Wilson", "Sales", 68000, 3),
            ("Grace Chen", "Engineering", 88000, 6),
            ("Henry Davis", "Marketing", 79000, 5),
            ("Iris Garcia", "Sales", 71000, 4),
            ("Jack Martinez", "Engineering", 91000, 7),
        ]

    def get_customer_cohort_data(self) -> List[Tuple]:
        """Generate customer cohort data for analysis."""
        return [
            (1, "2023-01-15", 100.00),
            (1, "2023-02-20", 150.00),
            (1, "2023-03-10", 200.00),
            (1, "2023-04-05", 75.00),
            (2, "2023-01-20", 75.00),
            (2, "2023-03-15", 125.00),
            (2, "2023-05-10", 200.00),
            (3, "2023-02-01", 300.00),
            (3, "2023-04-15", 150.00),
            (4, "2023-01-10", 250.00),
            (4, "2023-02-25", 180.00),
            (4, "2023-04-20", 320.00),
            (4, "2023-06-05", 95.00),
        ]

    def get_product_reviews_data(self) -> List[Tuple]:
        """Generate product review data for analysis."""
        return [
            ("PROD001", 5, "Excellent product! Highly recommend.", 15),
            ("PROD001", 4, "Good quality, fast delivery.", 8),
            ("PROD001", 5, "Perfect for my needs.", 12),
            ("PROD001", 2, "Had some issues with setup.", 3),
            ("PROD001", 4, "Solid build quality.", 6),
            ("PROD002", 5, "Amazing! Best purchase ever.", 20),
            ("PROD002", 5, "Love everything about it.", 18),
            ("PROD002", 4, "Very good, minor improvements needed.", 7),
            ("PROD003", 3, "Average product, nothing special.", 4),
            ("PROD003", 2, "Below expectations.", 2),
            ("PROD003", 1, "Terrible quality, don't buy.", 1),
            ("PROD003", 3, "Okay for the price.", 3),
            ("PROD004", 4, "Good value for money.", 9),
            ("PROD004", 5, "Exceeded expectations!", 11),
            ("PROD004", 4, "Reliable and durable.", 8),
        ]

    def get_time_series_data(self) -> List[Tuple]:
        """Generate time series data for analysis."""
        base_date = datetime(2023, 1, 1)
        data = []
        base_value = 100

        for i in range(30):  # 30 days of data
            date_str = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            # Add some trend and randomness
            value = base_value + (i * 2) + random.randint(-10, 20)
            data.append((date_str, value))

        return data

    # Exercise 7 Data: Performance Comparison
    def get_large_dataset(self, size: int = 10000) -> List[Tuple]:
        """Generate large dataset for performance testing."""
        departments = [
            "Engineering",
            "Marketing",
            "Sales",
            "Data Science",
            "HR",
            "Finance",
        ]
        names = ["Person_" + str(i) for i in range(size)]

        data = []
        for i in range(size):
            name = names[i]
            age = 20 + (i % 40)
            department = departments[i % len(departments)]
            salary = 40000 + (i * 100) % 50000
            data.append((name, age, department, salary))

        return data

    def get_transaction_dataset(self) -> List[Tuple]:
        """Generate transaction dataset for RDD vs DataFrame comparison."""
        return [
            ("TXN001", 1, 250.0, "2023-01-15", "Restaurant"),
            ("TXN002", 1, 75.0, "2023-01-16", "Gas Station"),
            ("TXN003", 1, 150.0, "2023-01-17", "Grocery"),
            ("TXN004", 1, 45.0, "2023-01-18", "Coffee Shop"),
            ("TXN005", 2, 500.0, "2023-01-15", "Electronics"),
            ("TXN006", 2, 300.0, "2023-01-16", "Clothing"),
            ("TXN007", 2, 125.0, "2023-01-17", "Grocery"),
            ("TXN008", 3, 50.0, "2023-01-15", "Coffee Shop"),
            ("TXN009", 3, 800.0, "2023-01-16", "Electronics"),
            ("TXN010", 3, 200.0, "2023-01-17", "Clothing"),
            ("TXN011", 4, 1200.0, "2023-01-15", "Electronics"),
            ("TXN012", 4, 90.0, "2023-01-16", "Grocery"),
            ("TXN013", 5, 350.0, "2023-01-15", "Clothing"),
            ("TXN014", 5, 75.0, "2023-01-16", "Gas Station"),
        ]


# Convenience functions for easy access
def get_sample_data_generator(seed: int = 42) -> SampleDataGenerator:
    """Get a sample data generator instance."""
    return SampleDataGenerator(seed)


def get_all_datasets() -> Dict[str, Any]:
    """Get all sample datasets in a dictionary."""
    generator = SampleDataGenerator()

    datasets = {
        # Exercise 1
        "employees": generator.get_employee_data(),
        "products": generator.get_product_catalog(),
        # Exercise 2
        "customers": generator.get_customer_data(),
        "sales_transactions": generator.get_sales_transactions(),
        # Exercise 3
        "comprehensive_employees": generator.get_comprehensive_employee_data(),
        "regional_sales": generator.get_sales_by_region(),
        # Exercise 4
        "customer_orders": generator.get_customer_order_data(),
        "product_inventory": generator.get_product_inventory_data(),
        # Exercise 5
        "people_contacts": generator.get_people_contact_data(),
        "events": generator.get_event_schedule_data(),
        "ecommerce_orders": generator.get_ecommerce_orders(),
        # Exercise 6
        "analytics_sales": generator.get_regional_sales_data(),
        "employee_performance": generator.get_employee_performance_data(),
        "customer_cohorts": generator.get_customer_cohort_data(),
        "product_reviews": generator.get_product_reviews_data(),
        "time_series": generator.get_time_series_data(),
        # Exercise 7
        "large_dataset": generator.get_large_dataset(),
        "transactions": generator.get_transaction_dataset(),
    }

    return datasets


if __name__ == "__main__":
    # Demo the sample data generator
    generator = SampleDataGenerator()

    print("Sample Employee Data:")
    for emp in generator.get_employee_data()[:3]:
        print(f"  {emp}")

    print("\nSample Product Data:")
    for prod in generator.get_product_catalog()[:3]:
        print(f"  {prod}")

    print("\nDataset sizes:")
    datasets = get_all_datasets()
    for name, data in datasets.items():
        if isinstance(data, tuple):
            print(f"  {name}: {len(data[0])}, {len(data[1])} records")
        else:
            print(f"  {name}: {len(data)} records")
