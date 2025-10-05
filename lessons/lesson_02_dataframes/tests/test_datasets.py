"""
Test cases for sample datasets
==============================

These tests validate the quality and integrity of sample datasets.
"""

import pytest
from datasets.sample_data import SampleDataGenerator, get_all_datasets


@pytest.fixture(scope="module")
def data_generator():
    """Create a sample data generator for testing."""
    return SampleDataGenerator(seed=42)


class TestSampleDataGenerator:
    """Test the sample data generator."""

    def test_generator_reproducibility(self):
        """Test that the generator produces reproducible results."""
        gen1 = SampleDataGenerator(seed=42)
        gen2 = SampleDataGenerator(seed=42)

        data1 = gen1.get_employee_data()
        data2 = gen2.get_employee_data()

        assert data1 == data2, "Same seed should produce identical data"

    def test_employee_data_structure(self, data_generator):
        """Test employee data has correct structure."""
        data = data_generator.get_employee_data()

        assert len(data) > 0, "Should have employee records"

        # Check structure of first record
        first_record = data[0]
        assert len(first_record) == 4, "Employee record should have 4 fields"

        name, age, department, salary = first_record
        assert isinstance(name, str), "Name should be string"
        assert isinstance(age, int), "Age should be integer"
        assert isinstance(department, str), "Department should be string"
        assert isinstance(salary, int), "Salary should be integer"

        # Check reasonable values
        assert age > 0, "Age should be positive"
        assert salary > 0, "Salary should be positive"
        assert len(name) > 0, "Name should not be empty"
        assert len(department) > 0, "Department should not be empty"

    def test_product_catalog_structure(self, data_generator):
        """Test product catalog has correct structure."""
        data = data_generator.get_product_catalog()

        assert len(data) > 0, "Should have product records"

        # Check structure
        first_record = data[0]
        assert len(first_record) == 5, "Product record should have 5 fields"

        product_id, name, category, price, stock = first_record
        assert isinstance(product_id, str), "Product ID should be string"
        assert isinstance(name, str), "Name should be string"
        assert isinstance(category, str), "Category should be string"
        assert isinstance(price, float), "Price should be float"
        assert isinstance(stock, int), "Stock should be integer"

        # Check reasonable values
        assert price > 0, "Price should be positive"
        assert stock >= 0, "Stock should be non-negative"

    def test_customer_order_data_relationships(self, data_generator):
        """Test customer and order data have valid relationships."""
        customers, orders = data_generator.get_customer_order_data()

        # Get customer IDs
        customer_ids = {customer[0] for customer in customers}

        # Check all order customer IDs exist in customers
        order_customer_ids = {order[1] for order in orders}

        # All order customer IDs should reference valid customers
        invalid_refs = order_customer_ids - customer_ids
        assert (
            len(invalid_refs) == 0
        ), f"Orders reference non-existent customers: {invalid_refs}"

    def test_time_series_data_consistency(self, data_generator):
        """Test time series data has consistent date progression."""
        data = data_generator.get_time_series_data()

        assert len(data) > 1, "Should have multiple time points"

        # Check dates are in order
        dates = [record[0] for record in data]
        sorted_dates = sorted(dates)

        assert dates == sorted_dates, "Dates should be in chronological order"

        # Check all values are reasonable
        for date_str, value in data:
            assert isinstance(date_str, str), "Date should be string"
            assert isinstance(value, int), "Value should be integer"
            assert len(date_str) == 10, "Date should be in YYYY-MM-DD format"
            assert value > 0, "Values should be positive"


class TestDatasetIntegration:
    """Test dataset integration with Spark."""

    def test_all_datasets_load_in_spark(self, spark):
        """Test that all datasets can be loaded into Spark DataFrames."""
        generator = SampleDataGenerator()

        # Test each dataset type
        datasets_to_test = [
            (
                "employees",
                generator.get_employee_data(),
                ["name", "age", "department", "salary"],
            ),
            (
                "products",
                generator.get_product_catalog(),
                ["id", "name", "category", "price", "stock"],
            ),
            (
                "customers",
                generator.get_customer_data(),
                ["id", "name", "email", "phone", "age"],
            ),
        ]

        for name, data, columns in datasets_to_test:
            try:
                df = spark.createDataFrame(data, columns)
                assert df.count() > 0, f"Dataset {name} should have records"
                assert len(df.columns) == len(
                    columns
                ), f"Dataset {name} should have correct columns"
            except Exception as e:
                pytest.fail(f"Failed to create DataFrame for {name}: {e}")

    def test_join_compatibility(self, spark):
        """Test that related datasets can be joined properly."""
        generator = SampleDataGenerator()

        # Test customer-order join
        customers, orders = generator.get_customer_order_data()

        customers_df = spark.createDataFrame(
            customers, ["customer_id", "name", "city", "age"]
        )
        orders_df = spark.createDataFrame(
            orders, ["order_id", "customer_id", "amount", "status"]
        )

        # Perform join
        joined = customers_df.join(orders_df, "customer_id", "inner")

        assert joined.count() > 0, "Join should produce results"
        assert "name" in joined.columns, "Should have customer name"
        assert "amount" in joined.columns, "Should have order amount"

    def test_aggregation_compatibility(self, spark):
        """Test that datasets work well with aggregation operations."""
        generator = SampleDataGenerator()

        # Test sales data aggregation
        sales_data = generator.get_regional_sales_data()
        df = spark.createDataFrame(
            sales_data, ["category", "region", "amount", "quantity"]
        )

        # Test groupBy aggregation
        result = df.groupBy("category").agg({"amount": "sum", "quantity": "avg"})

        assert result.count() > 0, "Aggregation should produce results"

        # Check that we have reasonable categories
        categories = [row["category"] for row in result.collect()]
        assert len(categories) > 1, "Should have multiple categories"


class TestDataQuality:
    """Test data quality aspects of sample datasets."""

    def test_no_duplicate_ids(self, data_generator):
        """Test that ID fields are unique where expected."""
        # Test employee names (should be unique in small dataset)
        employees = data_generator.get_employee_data()
        names = [emp[0] for emp in employees]
        assert len(names) == len(set(names)), "Employee names should be unique"

        # Test product IDs
        products = data_generator.get_product_catalog()
        product_ids = [prod[0] for prod in products]
        assert len(product_ids) == len(set(product_ids)), "Product IDs should be unique"

    def test_realistic_value_ranges(self, data_generator):
        """Test that generated values are in realistic ranges."""
        # Test employee data
        employees = data_generator.get_employee_data()

        for name, age, department, salary in employees:
            assert 18 <= age <= 70, f"Age {age} should be realistic"
            assert 30000 <= salary <= 200000, f"Salary {salary} should be realistic"
            assert (
                len(name.split()) >= 2
            ), "Name should have at least first and last name"

        # Test product data
        products = data_generator.get_product_catalog()

        for prod_id, name, category, price, stock in products:
            assert 0.01 <= price <= 10000, f"Price {price} should be realistic"
            assert 0 <= stock <= 1000, f"Stock {stock} should be realistic"

    def test_data_variety(self, data_generator):
        """Test that datasets have sufficient variety."""
        # Test department variety
        employees = data_generator.get_employee_data()
        departments = set(emp[2] for emp in employees)
        assert len(departments) >= 3, "Should have variety in departments"

        # Test category variety
        products = data_generator.get_product_catalog()
        categories = set(prod[2] for prod in products)
        assert len(categories) >= 2, "Should have variety in product categories"


class TestGetAllDatasets:
    """Test the convenience function for getting all datasets."""

    def test_get_all_datasets_structure(self):
        """Test that get_all_datasets returns expected structure."""
        datasets = get_all_datasets()

        assert isinstance(datasets, dict), "Should return a dictionary"
        assert len(datasets) > 0, "Should have datasets"

        # Check some expected keys
        expected_keys = ["employees", "products", "customers", "analytics_sales"]
        for key in expected_keys:
            assert key in datasets, f"Should have {key} dataset"

    def test_all_datasets_non_empty(self):
        """Test that all datasets have data."""
        datasets = get_all_datasets()

        for name, data in datasets.items():
            if isinstance(data, tuple):
                # Handle datasets that return multiple related datasets
                for i, sub_data in enumerate(data):
                    assert len(sub_data) > 0, f"Dataset {name}[{i}] should not be empty"
            else:
                assert len(data) > 0, f"Dataset {name} should not be empty"

    def test_dataset_types_consistency(self):
        """Test that dataset types are consistent."""
        datasets = get_all_datasets()

        for name, data in datasets.items():
            if isinstance(data, tuple):
                # Multi-dataset case (like customer_orders)
                for sub_data in data:
                    assert isinstance(
                        sub_data, list
                    ), f"Sub-dataset in {name} should be list"
                    if sub_data:  # If not empty
                        assert isinstance(
                            sub_data[0], tuple
                        ), f"Records in {name} should be tuples"
            else:
                assert isinstance(data, list), f"Dataset {name} should be list"
                if data:  # If not empty
                    assert isinstance(
                        data[0], tuple
                    ), f"Records in {name} should be tuples"
