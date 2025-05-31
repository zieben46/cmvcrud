# datastorekit/scd/type1.py
from datastorekit.scd.base import SCDHandler
from typing import List, Dict, Any, Optional
from datastorekit.exceptions import DatastoreOperationError
import logging

logger = logging.getLogger(__name__)

class Type1Handler(SCDHandler):
    def create(self, records: List[Dict]) -> int:
        try:
            if not records:
                return 0
            return self.adapter.create(self.table_name, records)
        except Exception as e:
            raise DatastoreOperationError(f"Failed to create records: {e}")

    def read(self, filters: Optional[Dict] = None) -> List[Dict]:
        try:
            return self.adapter.read(self.table_name, filters)
        except Exception as e:
            raise DatastoreOperationError(f"Failed to read records: {e}")

    def update(self, updates: List[Dict]) -> int:
        try:
            if not updates:
                return 0
            key_columns = self.adapter.get_reflected_keys(self.table_name) or (self.adapter.profile.keys.split(",") if self.adapter.profile.keys else [])
            if not key_columns:
                raise ValueError(f"No primary keys defined for table {self.table_name}")

            for update_data in updates:
                if not all(pk in update_data for pk in key_columns):
                    raise ValueError(f"Update dictionary missing primary key(s): {key_columns}")
            return self.adapter.update(self.table_name, updates)
        except Exception as e:
            raise DatastoreOperationError(f"Failed to update records: {e}")

    def delete(self, conditions: List[Dict]) -> int:
        try:
            if not conditions:
                return 0
            return self.adapter.delete(self.table_name, conditions)
        except Exception as e:
            raise DatastoreOperationError(f"Failed to delete records: {e}")