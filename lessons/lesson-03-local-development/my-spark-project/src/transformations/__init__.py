"""
Data transformation module containing reusable transformation functions.

This module provides:
- Base cleaning and aggregation utilities
- Domain-specific business transformations
- Sales, customer, and product analytics
"""

# Base transformation utilities
from .base_cleaning import BaseDataCleaner
from .base_aggregations import BaseDataAggregator

# Domain-specific transformations
from .customer_transformations import CustomerTransformations
from .product_transformations import ProductTransformations
from .sales_transformations import SalesTransformations

# Legacy imports for backward compatibility
from .cleaning import DataCleaner
from .aggregations import DataAggregator

__all__ = [
    # Base utilities
    "BaseDataCleaner",
    "BaseDataAggregator",
    # Domain-specific transformations
    "CustomerTransformations",
    "ProductTransformations",
    "SalesTransformations",
    # Legacy compatibility
    "DataCleaner",
    "DataAggregator",
]
