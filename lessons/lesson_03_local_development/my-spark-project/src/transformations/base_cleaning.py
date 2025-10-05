"""
Base data cleaning transformations for Spark DataFrames.
This module provides generic, reusable data cleaning functions that can be applied across domains.
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    isnan,
    regexp_replace,
    trim,
    upper,
    to_date,
    lit,
)


class BaseDataCleaner:
    """
    Base data cleaning utility class with generic cleaning functions.

    This class provides methods for:
    - Handling null values
    - Data type conversions
    - Text cleaning and standardization
    - Outlier detection and handling
    - Data validation
    """

    @staticmethod
    def remove_nulls(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Remove rows with null values in specified columns.

        Args:
            df: Input DataFrame
            columns: List of columns to check for nulls. If None, check all columns.

        Returns:
            DataFrame with null rows removed
        """
        if columns is None:
            columns = df.columns

        for column in columns:
            df = df.filter(col(column).isNotNull() & (~isnan(col(column))))

        return df

    @staticmethod
    def fill_nulls(df: DataFrame, fill_values: Dict[str, Any]) -> DataFrame:
        """
        Fill null values with specified values.

        Args:
            df: Input DataFrame
            fill_values: Dictionary mapping column names to fill values

        Returns:
            DataFrame with nulls filled
        """
        return df.fillna(fill_values)

    @staticmethod
    def clean_text_column(
        df: DataFrame,
        column: str,
        remove_special_chars: bool = True,
        trim_whitespace: bool = True,
        to_uppercase: bool = False,
    ) -> DataFrame:
        """
        Clean text column by removing special characters and normalizing text.

        Args:
            df: Input DataFrame
            column: Column name to clean
            remove_special_chars: Whether to remove special characters
            trim_whitespace: Whether to trim whitespace
            to_uppercase: Whether to convert to uppercase

        Returns:
            DataFrame with cleaned text column
        """
        cleaned_col = col(column)

        if trim_whitespace:
            cleaned_col = trim(cleaned_col)

        if remove_special_chars:
            # Remove non-alphanumeric characters except spaces
            cleaned_col = regexp_replace(cleaned_col, r"[^a-zA-Z0-9\s]", "")

        if to_uppercase:
            cleaned_col = upper(cleaned_col)

        return df.withColumn(column, cleaned_col)

    @staticmethod
    def standardize_phone_numbers(df: DataFrame, column: str) -> DataFrame:
        """
        Standardize phone number format.

        Args:
            df: Input DataFrame
            column: Phone number column name

        Returns:
            DataFrame with standardized phone numbers
        """
        # Remove all non-numeric characters
        cleaned_phone = regexp_replace(col(column), r"[^\d]", "")

        # Format as (XXX) XXX-XXXX for 10-digit numbers
        formatted_phone = when(
            cleaned_phone.rlike(r"^\d{10}$"),
            regexp_replace(cleaned_phone, r"(\d{3})(\d{3})(\d{4})", "($1) $2-$3"),
        ).otherwise(col(column))

        return df.withColumn(column, formatted_phone)

    @staticmethod
    def validate_email(df: DataFrame, column: str) -> DataFrame:
        """
        Validate email format and mark invalid emails as null.

        Args:
            df: Input DataFrame
            column: Email column name

        Returns:
            DataFrame with validated emails
        """
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

        validated_email = when(col(column).rlike(email_pattern), col(column)).otherwise(
            lit(None)
        )

        return df.withColumn(column, validated_email)

    @staticmethod
    def handle_outliers(
        df: DataFrame,
        column: str,
        method: str = "clip",
        lower_percentile: float = 0.01,
        upper_percentile: float = 0.99,
    ) -> DataFrame:
        """
        Handle outliers in numeric columns.

        Args:
            df: Input DataFrame
            column: Numeric column name
            method: Method to handle outliers ('clip', 'remove', 'null')
            lower_percentile: Lower percentile threshold
            upper_percentile: Upper percentile threshold

        Returns:
            DataFrame with outliers handled
        """
        # Calculate percentile bounds
        bounds = df.select(
            col(column).quantile(lower_percentile).alias("lower"),
            col(column).quantile(upper_percentile).alias("upper"),
        ).collect()[0]

        lower_bound = bounds["lower"]
        upper_bound = bounds["upper"]

        if method == "clip":
            # Clip values to bounds
            clipped_col = (
                when(col(column) < lower_bound, lower_bound)
                .when(col(column) > upper_bound, upper_bound)
                .otherwise(col(column))
            )
            return df.withColumn(column, clipped_col)

        elif method == "remove":
            # Remove outlier rows
            return df.filter(
                (col(column) >= lower_bound) & (col(column) <= upper_bound)
            )

        elif method == "null":
            # Set outliers to null
            nulled_col = when(
                (col(column) >= lower_bound) & (col(column) <= upper_bound), col(column)
            ).otherwise(lit(None))
            return df.withColumn(column, nulled_col)

        else:
            raise ValueError(f"Unknown method: {method}")

    @staticmethod
    def convert_date_column(
        df: DataFrame, column: str, date_format: str = "yyyy-MM-dd"
    ) -> DataFrame:
        """
        Convert string column to DateType.

        Args:
            df: Input DataFrame
            column: Column name to convert
            date_format: Date format pattern

        Returns:
            DataFrame with converted date column
        """
        return df.withColumn(column, to_date(col(column), date_format))

    @staticmethod
    def validate_numeric_range(
        df: DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> DataFrame:
        """
        Validate numeric values are within specified range.

        Args:
            df: Input DataFrame
            column: Numeric column name
            min_value: Minimum allowed value
            max_value: Maximum allowed value

        Returns:
            DataFrame with invalid values set to null
        """
        validated_col = col(column)

        if min_value is not None:
            validated_col = when(col(column) >= min_value, validated_col).otherwise(
                lit(None)
            )

        if max_value is not None:
            validated_col = when(col(column) <= max_value, validated_col).otherwise(
                lit(None)
            )

        return df.withColumn(column, validated_col)

    @staticmethod
    def standardize_categorical_values(
        df: DataFrame, column: str, value_mappings: Dict[str, str]
    ) -> DataFrame:
        """
        Standardize categorical values using a mapping dictionary.

        Args:
            df: Input DataFrame
            column: Column name to standardize
            value_mappings: Dictionary mapping old values to new values

        Returns:
            DataFrame with standardized categorical values
        """
        standardized_col = col(column)

        for old_value, new_value in value_mappings.items():
            standardized_col = when(
                upper(col(column)) == old_value.upper(), new_value
            ).otherwise(standardized_col)

        return df.withColumn(column, standardized_col)

    @staticmethod
    def get_data_quality_report(df: DataFrame, dataset_name: str) -> Dict[str, Any]:
        """
        Generate a data quality report for a DataFrame.

        Args:
            df: DataFrame to analyze
            dataset_name: Name of the dataset

        Returns:
            Dictionary containing quality metrics
        """
        total_rows = df.count()
        total_columns = len(df.columns)

        # Calculate null counts per column
        null_counts = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts[column] = {
                "null_count": null_count,
                "null_percentage": (
                    round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
                ),
            }

        return {
            "dataset_name": dataset_name,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "null_analysis": null_counts,
            "overall_completeness": (
                round(
                    (
                        1
                        - sum(col["null_count"] for col in null_counts.values())
                        / (total_rows * total_columns)
                    )
                    * 100,
                    2,
                )
                if total_rows > 0 and total_columns > 0
                else 0
            ),
        }
