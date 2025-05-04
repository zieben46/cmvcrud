# datastorekit/adapters/inmemory_adapter.py
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DatastoreOperationError
from typing import List, Dict, Any, Iterator, Optional
import copy
import logging

logger = logging.getLogger(__name__)

class InMemoryAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.data = {}  # {table_name: List[Dict]}

    def get_reflected_keys(self, table_name: str) -> List[str]:
        """Return an empty list as in-memory tables have no inherent primary keys."""
        return []

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        try:
            if table_name not in self.data:
                self.data[table_name] = []
            self.data[table_name].extend(copy.deepcopy(data))
        except Exception as e:
            logger.error(f"Insert failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during insert on {table_name}: {e}")

    def select(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            if table_name not in self.data:
                return []
            result = []
            for record in self.data[table_name]:
                if not filters or all(record.get(key) == value for key, value in filters.items()):
                    result.append(copy.deepcopy(record))
            return result
        except Exception as e:
            logger.error(f"Select failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select on {table_name}: {e}")

    def select_chunks(self, table_name: str, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
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
            logger.error(f"Select chunks failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select_chunks on {table_name}: {e}")

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None):
        try:
            if table_name not in self.data:
                return
            for record in self.data[table_name]:
                if not filters or all(record.get(key) == value for key, value in filters.items()):
                    for update_data in data:
                        record.update(copy.deepcopy(update_data))
        except Exception as e:
            logger.error(f"Update failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def delete(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        try:
            if table_name not in self.data:
                return
            self.data[table_name] = [
                record for record in self.data[table_name]
                if filters and not all(record.get(key) == value for key, value in filters.items())
            ]
        except Exception as e:
            logger.error(f"Delete failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        try:
            if table_name not in self.data:
                self.data[table_name] = []
            # Apply inserts
            self.data[table_name].extend(copy.deepcopy(inserts))
            logger.debug(f"Applied {len(inserts)} inserts to {table_name}")

            # Apply updates
            key_columns = self.get_reflected_keys(table_name) or (self.profile.keys.split(",") if self.profile.keys else [])
            for update_data in updates:
                for record in self.data[table_name]:
                    if all(record.get(key) == update_data.get(key) for key in key_columns):
                        record.update(copy.deepcopy(update_data))
            logger.debug(f"Applied {len(updates)} updates to {table_name}")

            # Apply deletes
            for delete_data in deletes:
                self.data[table_name] = [
                    record for record in self.data[table_name]
                    if not all(record.get(key) == delete_data.get(key) for key in key_columns)
                ]
            logger.debug(f"Applied {len(deletes)} deletes to {table_name}")
        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError("SQL execution is not supported for InMemoryAdapter")

    def list_tables(self, schema: str) -> List[str]:
        return list(self.data.keys())

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        metadata = {}
        for table_name in self.data:
            if self.data[table_name]:
                columns = {key: str(type(value).__name__) for key, value in self.data[table_name][0].items()}
                pk_columns = self.profile.keys.split(",") if self.profile.keys else []
                metadata[table_name] = {
                    "columns": columns,
                    "primary_keys": pk_columns
                }
        return metadata