# datastorekit/adapters/inmemory_adapter.py
from dataclasses import dataclass
from typing import List, Dict, Any, Iterator, Optional, Tuple
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DatastoreOperationError
import copy
import logging

logger = logging.getLogger(__name__)

@dataclass
class DBTable:
    table_name: str

class InMemoryAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.data = {}  # {table_name: List[Dict]}

    def get_reflected_keys(self, table_name: str) -> List[str]:
        return self.profile.keys.split(",") if self.profile.keys else []

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        try:
            self.data[table_name] = []
            logger.info(f"Created in-memory table {table_name} with schema {schema}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create table {table_name}: {e}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        try:
            if table_name not in self.data or not self.data[table_name]:
                return {}
            sample_record = self.data[table_name][0]
            type_map = {
                "int": "INTEGER",
                "str": "STRING",
                "float": "FLOAT",
                "datetime": "TIMESTAMP",
                "bool": "BOOLEAN"
            }
            return {key: type_map.get(type(value).__name__, "STRING") for key, value in sample_record.items()}
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return {}

    def create(self, table_name: str, records: List[Dict]) -> int:
        dbtable = DBTable(table_name)
        try:
            return self._create_records(dbtable, records)
        except Exception as e:
            logger.error(f"Create failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during create on {table_name}: {e}")

    def _create_records(self, dbtable: DBTable, records: List[Dict]) -> int:
        try:
            if dbtable.table_name not in self.data:
                self.data[dbtable.table_name] = []
            self.data[dbtable.table_name].extend(copy.deepcopy(records))
            return len(records)
        except Exception as e:
            logger.error(f"Error during bulk insert into {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error during bulk insert into {dbtable.table_name}: {e}")

    def read(self, table_name: str, filters: Optional[Dict] = None) -> List[Dict]:
        try:
            if table_name not in self.data:
                return []
            result = []
            for record in self.data[table_name]:
                if not filters or all(record.get(key) == value for key, value in filters.items()):
                    result.append(copy.deepcopy(record))
            return result
        except Exception as e:
            logger.error(f"Read failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during read on {table_name}: {e}")

    def select_chunks(self, table_name: str, filters: Optional[Dict] = None, chunk_size: int = 100000) -> Iterator[List[Dict]]:
        try:
            if table_name not in self.data:
                return
            chunk = []
            for record in self.data[table_name]:
                if not filters or all(record.get(key) == value for key, value in filters.items()):
                    chunk.append(copy.deepcopy(record))
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
            if chunk:
                yield chunk
        except Exception as e:
            logger.error(f"Select chunks failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select_chunks on {table_name}: {e}")

    def update(self, table_name: str, updates: List[Dict]) -> int:
        dbtable = DBTable(table_name)
        try:
            return self._update_records(dbtable, updates)
        except Exception as e:
            logger.error(f"Update failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def _update_records(self, dbtable: DBTable, updates: List[Dict]) -> int:
        try:
            if dbtable.table_name not in self.data:
                return 0
            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for update operations.")

            updated_count = 0
            for update_dict in updates:
                if not all(pk in update_dict for pk in primary_keys):
                    raise ValueError(f"Update dictionary missing primary key(s): {primary_keys}")

                for record in self.data[dbtable.table_name]:
                    if all(record.get(pk) == update_dict.get(pk) for pk in primary_keys):
                        for k, v in update_dict.items():
                            if k not in primary_keys:
                                record[k] = copy.deepcopy(v)
                        updated_count += 1

            return updated_count
        except Exception as e:
            logger.error(f"Error updating {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error updating {dbtable.table_name}: {e}")

    def delete(self, table_name: str, conditions: List[Dict]) -> int:
        dbtable = DBTable(table_name)
        try:
            return self._delete_records(dbtable, conditions)
        except Exception as e:
            logger.error(f"Delete failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

    def _delete_records(self, dbtable: DBTable, conditions: List[Dict]) -> int:
        try:
            if dbtable.table_name not in self.data:
                return 0
            if not conditions:
                raise ValueError("Conditions list cannot be empty.")

            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for delete operations.")

            initial_count = len(self.data[dbtable.table_name])
            new_data = []
            for record in self.data[dbtable.table_name]:
                keep = True
                for cond in conditions:
                    if not cond:
                        raise ValueError("Condition dictionary cannot be empty.")
                    if all(record.get(key) == value for key, value in cond.items()):
                        keep = False
                        break
                if keep:
                    new_data.append(record)

            self.data[dbtable.table_name] = new_data
            return initial_count - len(new_data)
        except Exception as e:
            logger.error(f"Error deleting from {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error deleting from {dbtable.table_name}: {e}")

    def apply_changes(self, table_name: str, changes: List[Dict]) -> Tuple[List[Any], int, int, int]:
        dbtable = DBTable(table_name)
        try:
            inserts = []
            updates = []
            deletes = []

            for change in changes:
                if "operation" not in change:
                    raise ValueError("Each change dictionary must include an 'operation' key")
                operation = change["operation"]
                data = {k: v for k, v in change.items() if k != "operation"}

                if operation == "create":
                    inserts.append(data)
                elif operation == "update":
                    updates.append(data)
                elif operation == "delete":
                    deletes.append(data)
                else:
                    raise ValueError(f"Invalid operation: {operation}")

            inserted_ids = []
            inserted_count = 0
            updated_count = 0
            deleted_count = 0

            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if inserts or updates or deletes:
                if not primary_keys:
                    raise ValueError("Table must have a primary key for operations.")

            if inserts:
                inserted_ids, inserted_count = self._create_records_with_ids(dbtable, inserts)

            if updates:
                updated_count = self._update_records(dbtable, updates)

            if deletes:
                deleted_count = self._delete_records(dbtable, deletes)

            return inserted_ids, inserted_count, updated_count, deleted_count
        except Exception as e:
            logger.error(f"Error applying changes to {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error applying changes to {dbtable.table_name}: {e}")

    def _create_records_with_ids(self, dbtable: DBTable, records: List[Dict]) -> Tuple[List[Any], int]:
        try:
            if dbtable.table_name not in self.data:
                self.data[dbtable.table_name] = []

            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for insert operations.")

            max_id = 0
            if self.data[dbtable.table_name] and primary_keys[0] in self.data[dbtable.table_name][0]:
                max_id = max(record.get(primary_keys[0], 0) for record in self.data[dbtable.table_name])

            inserted_ids = []
            for i, record in enumerate(records):
                record = copy.deepcopy(record)
                if primary_keys[0] not in record:
                    record[primary_keys[0]] = max_id + i + 1
                self.data[dbtable.table_name].append(record)
                inserted_ids.append(record[primary_keys[0]] if len(primary_keys) == 1 else {pk: record.get(pk) for pk in primary_keys})

            return inserted_ids, len(records)
        except Exception as e:
            logger.error(f"Error generating IDs for insert into {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error generating IDs for insert into {dbtable.table_name}: {e}")

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError("SQL execution is not supported for InMemoryAdapter")

    def list_tables(self, schema: str) -> List[str]:
        return list(self.data.keys())

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        metadata = {}
        for table_name in self.data:
            columns = self.get_table_columns(table_name)
            pk_columns = self.get_reflected_keys(table_name)
            metadata[table_name] = {
                "columns": columns,
                "primary_keys": pk_columns
            }
        return metadata