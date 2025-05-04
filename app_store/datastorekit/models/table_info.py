# datastorekit/models/table_info.py
from typing import Dict, Any, Optional
from sqlalchemy import Column, Integer, String, JSON, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datastorekit.permissions.manager import PermissionsManager
import json

from dataclasses import dataclass
from typing import Optional

# Base = declarative_base()

@dataclass
class TableInfo:
    table_name: str
    datastore_key: str
    keys: Optional[str] = None
    scd_type: str = "type1"

    def __init__(
        self,
        table_name: str,
        keys: str = "unique_id",
        scd_type: str = "type1",
        datastore_key: str = "spend_plan_db:safe_user",
        permissions_manager: Optional[PermissionsManager] = None
    ):
        """Initialize TableInfo with table metadata and permissions.

        Args:
            table_name: Name of the table (e.g., 'spend_plan').
            keys: Comma-separated key columns (e.g., 'unique_id,secondary_key').
            scd_type: SCD type (default: 'type1').
            datastore_key: Datastore identifier (e.g., 'spend_plan_db:safe_user').
            schedule_frequency: Scheduling frequency (e.g., 'hourly').
            enabled: Whether the table is enabled for operations (default: True).
            columns: Optional dictionary of column names to types (e.g., {'unique_id': 'Integer'}).
            permissions_manager: Optional PermissionsManager instance.
        """
        self.table_name = table_name
        self.keys = keys
        self.scd_type = scd_type
        self.datastore_key = datastore_key
        self.permissions_manager = permissions_manager



    def has_access(self, username: str, operation: str) -> bool:
        """Check if a user has permission for an operation on this table."""
        if not self.permissions_manager:
            return True
        return self.permissions_manager.check_access(username, self, operation)