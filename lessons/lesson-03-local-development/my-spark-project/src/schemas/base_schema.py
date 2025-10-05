"""
Base schema utilities and common schema functions.
This module provides utility functions for working with schemas across the project.
"""

from pyspark.sql.types import StructType
from typing import Tuple, List, Dict, Any


class SchemaUtils:
    """
    Utility functions for working with schemas.

    This class provides helper methods for:
    - Schema validation
    - Schema evolution
    - Schema comparison
    - Schema documentation
    """

    @staticmethod
    def validate_dataframe_schema(
        df, expected_schema: StructType
    ) -> Tuple[bool, List[str]]:
        """
        Validate if a DataFrame matches the expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema

        Returns:
            Tuple of (is_valid, list_of_differences)
        """
        differences = []
        actual_schema = df.schema

        # Check field count
        if len(actual_schema.fields) != len(expected_schema.fields):
            differences.append(
                f"Field count mismatch: expected {len(expected_schema.fields)}, "
                f"got {len(actual_schema.fields)}"
            )

        # Check individual fields
        expected_fields = {field.name: field for field in expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}

        for field_name, expected_field in expected_fields.items():
            if field_name not in actual_fields:
                differences.append(f"Missing field: {field_name}")
            else:
                actual_field = actual_fields[field_name]
                if actual_field.dataType != expected_field.dataType:
                    differences.append(
                        f"Data type mismatch for field '{field_name}': "
                        f"expected {expected_field.dataType}, got {actual_field.dataType}"
                    )
                if actual_field.nullable != expected_field.nullable:
                    differences.append(
                        f"Nullable mismatch for field '{field_name}': "
                        f"expected {expected_field.nullable}, got {actual_field.nullable}"
                    )

        # Check for extra fields
        for field_name in actual_fields:
            if field_name not in expected_fields:
                differences.append(f"Unexpected field: {field_name}")

        return len(differences) == 0, differences

    @staticmethod
    def get_schema_summary(schema: StructType) -> Dict[str, Any]:
        """
        Get a summary of the schema.

        Args:
            schema: Schema to summarize

        Returns:
            Dictionary with schema summary
        """
        field_types = {}
        nullable_fields = []
        required_fields = []

        for field in schema.fields:
            field_types[field.name] = str(field.dataType)
            if field.nullable:
                nullable_fields.append(field.name)
            else:
                required_fields.append(field.name)

        return {
            "total_fields": len(schema.fields),
            "field_types": field_types,
            "nullable_fields": nullable_fields,
            "required_fields": required_fields,
            "field_names": [field.name for field in schema.fields],
        }

    @staticmethod
    def compare_schemas(schema1: StructType, schema2: StructType) -> Dict[str, Any]:
        """
        Compare two schemas and return differences.

        Args:
            schema1: First schema
            schema2: Second schema

        Returns:
            Dictionary with comparison results
        """
        fields1 = {field.name: field for field in schema1.fields}
        fields2 = {field.name: field for field in schema2.fields}

        only_in_schema1 = set(fields1.keys()) - set(fields2.keys())
        only_in_schema2 = set(fields2.keys()) - set(fields1.keys())
        common_fields = set(fields1.keys()) & set(fields2.keys())

        type_differences = []
        nullable_differences = []

        for field_name in common_fields:
            field1 = fields1[field_name]
            field2 = fields2[field_name]

            if field1.dataType != field2.dataType:
                type_differences.append(
                    {
                        "field": field_name,
                        "schema1_type": str(field1.dataType),
                        "schema2_type": str(field2.dataType),
                    }
                )

            if field1.nullable != field2.nullable:
                nullable_differences.append(
                    {
                        "field": field_name,
                        "schema1_nullable": field1.nullable,
                        "schema2_nullable": field2.nullable,
                    }
                )

        return {
            "identical": len(only_in_schema1) == 0
            and len(only_in_schema2) == 0
            and len(type_differences) == 0
            and len(nullable_differences) == 0,
            "only_in_schema1": list(only_in_schema1),
            "only_in_schema2": list(only_in_schema2),
            "type_differences": type_differences,
            "nullable_differences": nullable_differences,
            "common_fields": list(common_fields),
        }

    @staticmethod
    def get_schema_documentation(
        schema: StructType, schema_name: str = "Schema"
    ) -> str:
        """
        Generate documentation for a schema.

        Args:
            schema: Schema to document
            schema_name: Name of the schema

        Returns:
            Formatted schema documentation
        """
        doc = f"# {schema_name} Documentation\n\n"
        doc += f"**Total Fields:** {len(schema.fields)}\n\n"
        doc += "## Fields\n\n"
        doc += "| Field Name | Data Type | Nullable | Description |\n"
        doc += "|------------|-----------|----------|-------------|\n"

        for field in schema.fields:
            nullable_str = "Yes" if field.nullable else "No"
            doc += f"| {field.name} | {field.dataType} | {nullable_str} | |\n"

        return doc

    @staticmethod
    def extract_field_names(schema: StructType) -> List[str]:
        """
        Extract field names from a schema.

        Args:
            schema: Schema to extract field names from

        Returns:
            List of field names
        """
        return [field.name for field in schema.fields]

    @staticmethod
    def extract_required_fields(schema: StructType) -> List[str]:
        """
        Extract required (non-nullable) field names from a schema.

        Args:
            schema: Schema to extract required fields from

        Returns:
            List of required field names
        """
        return [field.name for field in schema.fields if not field.nullable]

    @staticmethod
    def extract_optional_fields(schema: StructType) -> List[str]:
        """
        Extract optional (nullable) field names from a schema.

        Args:
            schema: Schema to extract optional fields from

        Returns:
            List of optional field names
        """
        return [field.name for field in schema.fields if field.nullable]
