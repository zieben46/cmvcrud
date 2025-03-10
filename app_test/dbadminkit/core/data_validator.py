from pydantic import BaseModel, ValidationError
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, schema: Dict[str, str]):
        """Initialize with a table schema (e.g., {'id': 'INTEGER', 'name': 'VARCHAR'})."""
        self.schema = schema
        self.model = self._create_dynamic_model()

    def _create_dynamic_model(self):
        """Dynamically create a Pydantic model from the schema."""
        fields = {}
        for col, col_type in self.schema.items():
            if "INTEGER" in col_type.upper():
                fields[col] = (int, ...)
            elif "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                fields[col] = (str, ...)
            elif "BOOLEAN" in col_type.upper():
                fields[col] = (bool, ...)
            # Add more type mappings as needed
            else:
                fields[col] = (Any, ...)  # Fallback for unknown types
        return type("DynamicTableModel", (BaseModel,), {"__annotations__": fields})

    def validate(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate a list of data dictionaries against the schema."""
        validated_data = []
        for row in data:
            try:
                validated_row = self.model(**row).dict()
                validated_data.append(validated_row)
            except ValidationError as e:
                logger.error(f"Validation failed for row {row}: {e}")
                raise ValueError(f"Invalid data: {e}")
        return validated_data

# Example usage in DBTable