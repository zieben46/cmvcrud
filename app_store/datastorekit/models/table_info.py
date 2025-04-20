# datastorekit/models/table_info.py
from typing import Dict, Any, Optional
from sqlalchemy import Column, Integer, String, JSON, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datastorekit.permissions.manager import PermissionsManager
import json

Base = declarative_base()

class TableInfo(Base):
    __tablename__ = "table_info"
    id = Column(Integer, primary_key=True)
    table_name = Column(String, nullable=False, unique=True)
    columns = Column(JSON, nullable=True)  # Optional: {"unique_id": "Integer", "category": "String"}
    keys = Column(String, nullable=False, default="unique_id")  # Comma-separated, e.g., "unique_id,secondary_key"
    scd_type = Column(String, nullable=False, default="type1")  # e.g., "type0", "type1", "type2"
    datastore_key = Column(String, nullable=False)  # e.g., "spend_plan_db:safe_user"
    schedule_frequency = Column(String, nullable=False, default="hourly")  # e.g., "hourly", "daily"
    enabled = Column(Boolean, nullable=False, default=True)
    last_replicated = Column(DateTime, nullable=True)
    last_status = Column(String, nullable=True)  # e.g., "success", "failed"
    last_error = Column(String, nullable=True)

    def __init__(
        self,
        table_name: str,
        keys: str = "unique_id",
        scd_type: str = "type1",
        datastore_key: str = "spend_plan_db:safe_user",
        schedule_frequency: str = "hourly",
        enabled: bool = True,
        columns: Optional[Dict[str, str] = None,
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
        self.schedule_frequency = schedule_frequency
        self.enabled = enabled
        self.columns = columns
        self.permissions_manager = permissions_manager
        self.validate()

    def validate(self):
        """Validate TableInfo attributes."""
        if not self.keys or not self.keys.strip():
            raise ValueError("Keys cannot be empty")
        if self.scd_type == "type2" and self.columns:
            required = ["start_date", "end_date", "is_active"]
            if not all(col in self.columns for col in required):
                raise ValueError(f"SCD Type 2 requires columns: {required}")

    def to_dict(self) -> Dict[str, Any]:
        """Return table metadata as a dictionary."""
        return {
            "table_name": self.table_name,
            "columns": self.columns,
            "keys": self.keys,
            "scd_type": self.scd_type,
            "datastore_key": self.datastore_key,
            "schedule_frequency": self.schedule_frequency,
            "enabled": self.enabled,
            "last_replicated": self.last_replicated,
            "last_status": self.last_status,
            "last_error": self.last_error
        }

    def has_access(self, username: str, operation: str) -> bool:
        """Check if a user has permission for an operation on this table."""
        if not self.permissions_manager:
            return True
        return self.permissions_manager.check_access(username, self, operation)