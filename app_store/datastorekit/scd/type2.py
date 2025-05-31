# datastorekit/scd/type2.py
from datastorekit.scd.base import SCDHandler
from datetime import datetime
from typing import List, Dict, Any, Optional
from datastorekit.exceptions import DatastoreOperationError
import logging

logger = logging.getLogger(__name__)

class Type2Handler(SCDHandler):
    def create(self, records: List[Dict]) -> int:
        try:
            if not records:
                return 0
            for record in records:
                record["start_date"] = datetime.now()
                record["end_date"] = None
                record["is_active"] = True
            logger.debug(f"Creating {len(records)} records in {self.table_name} with SCD Type 2 attributes")
            return self.adapter.create(self.table_name, records)
        except Exception as e:
            logger.error(f"Failed to create records in {self.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create records: {e}")

    def read(self, filters: Optional[Dict] = None) -> List[Dict]:
        try:
            read_filters = filters.copy() if filters else {}
            if "is_active" not in read_filters:
                read_filters["is_active"] = True
            logger.debug(f"Reading records from {self.table_name} with filters: {read_filters}")
            return self.adapter.read(self.table_name, read_filters)
        except Exception as e:
            logger.error(f"Failed to read records from {self.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to read records: {e}")

    def update(self, updates: List[Dict]) -> int:
        try:
            if not updates:
                return 0
            key_columns = self.adapter.get_reflected_keys(self.table_name) or (self.adapter.profile.keys.split(",") if self.adapter.profile.keys else [])
            if not key_columns:
                raise ValueError(f"No primary keys defined for table {self.table_name}")

            changes = []
            for update_data in updates:
                # Validate primary keys
                if not all(pk in update_data for pk in key_columns):
                    raise ValueError(f"Update dictionary missing primary key(s): {key_columns}")

                # End existing record
                end_filters = {pk: update_data[pk] for pk in key_columns}
                end_filters["is_active"] = True
                changes.append({
                    "operation": "update",
                    "end_date": datetime.now(),
                    "is_active": False,
                    **end_filters
                })

                # Create new version
                new_record = update_data.copy()
                new_record["start_date"] = datetime.now()
                new_record["end_date"] = None
                new_record["is_active"] = True
                changes.append({
                    "operation": "create",
                    **new_record
                })

            logger.debug(f"Applying {len(changes)} changes (updates and creates) for {self.table_name}")
            if changes:
                inserted_ids, inserted_count, updated_count, deleted_count = self.adapter.apply_changes(self.table_name, changes)
                return updated_count + inserted_count  # Total rows affected (ended records + new versions)
            return 0
        except Exception as e:
            logger.error(f"Failed to update records in {self.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to update records: {e}")

    def delete(self, conditions: List[Dict]) -> int:
        try:
            if not conditions:
                return 0

            changes = []
            for cond in conditions:
                if not cond:
                    raise ValueError("Condition dictionary cannot be empty")
                
                # Validate condition columns
                table_columns = self.adapter.get_table_columns(self.table_name)
                if not all(key in table_columns for key in cond.keys()):
                    raise ValueError(f"Invalid column names in conditions: {cond.keys()}")

                # Create update operation to mark record as inactive
                update_data = {
                    "operation": "update",
                    "end_date": datetime.now(),
                    "is_active": False,
                    **cond
                }
                changes.append(update_data)

            logger.debug(f"Applying {len(changes)} logical delete operations for {self.table_name}")
            if changes:
                inserted_ids, inserted_count, updated_count, deleted_count = self.adapter.apply_changes(self.table_name, changes)
                return updated_count  # Return the number of rows updated (logically deleted)
            return 0
        except Exception as e:
            logger.error(f"Failed to logically delete records in {self.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to logically delete records: {e}")