# datastorekit/models/table_info.py
from typing import Dict, Any, Optional
from sqlalchemy import Integer, String, Float, DateTime, Boolean, BigInteger
from datastorekit.permissions.manager import PermissionsManager

class TableInfo:
    def __init__(
        self,
        table_name: str,
        columns: Dict[str, Any],
        key: str = "unique_id",
        scd_type: str = "type1",
        permissions_manager: Optional[PermissionsManager] = None
    ):
        """Initialize TableInfo with table metadata and permissions.

        Args:
            table_name: Name of the table (e.g., 'spend_plan').
            columns: Dictionary of column names to SQLAlchemy types (e.g., {'unique_id': Integer, 'category': String}).
            key: Primary key column name (default: 'unique_id').
            scd_type: SCD type (default: 'type1').
            permissions_manager: PermissionsManager instance for access control (optional).
        """
        self.table_name = table_name
        self.columns = columns
        self.key = key
        self.scd_type = scd_type
        self.permissions_manager = permissions_manager

    def to_dict(self) -> Dict[str, Any]:
        """Return table metadata as a dictionary."""
        return {
            "table_name": self.table_name,
            "columns": {name: str(type_) for name, type_ in self.columns.items()},
            "key": self.key,
            "scd_type": self.scd_type
        }

    def has_access(self, username: str, operation: str) -> bool:
        """Check if a user has permission for an operation on this table."""
        if not self.permissions_manager:
            return True  # No permissions manager, allow access
        datastore_key = f"{self.permissions_manager.datastore_key}"
        return self.permissions_manager.check_access(username, datastore_key, self.table_name, operation)