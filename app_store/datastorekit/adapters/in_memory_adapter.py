# datastorekit/adapters/in_memory_adapter.py
from datastorekit.adapters.base import DatastoreAdapter
from typing import List, Dict, Any, Iterator, Optional
import copy
import logging

logger = logging.getLogger(__name__)

class InMemoryAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.data = {}  # {table_name: List[Dict]}

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys are present in the data."""
        if table_name in self.data and self.data[table_name]:
            record = self.data[table_name][0]
            data_keys = set(record.keys())
            if not all(key in data_keys for key in table_info_keys):
                raise ValueError(
                    f"Table {table_name} data keys {data_keys} do not include all TableInfo keys {table_info_keys}"
                )

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        if table_name not in self.data:
            self.data[table_name] = []
        self.data[table_name].extend(copy.deepcopy(data))

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        if table_name not in self.data:
            return []
        result = []
        for record in self.data[table_name]:
            if all(record.get(key) == value for key, value in filters.items()):
                result.append(copy.deepcopy(record))
        return result

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        if table_name not in self.data:
            return
        chunk = []
        for record in self.data[table_name]:
            if all(record.get(key) == value for key, value in filters.items()):
                chunk.append(copy.deepcopy(record))
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
        if chunk:
            yield chunk

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        if table_name not in self.data:
            return
        for record in self.data[table_name]:
            if all(record.get(key) == value for key, value in filters.items()):
                for update_data in data:
                    record.update(copy.deepcopy(update_data))

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        if table_name not in self.data:
            return
        self.data[table_name] = [
            record for record in self.data[table_name]
            if not all(record.get(key) == value for key, value in filters.items())
        ]

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query (not supported for InMemoryAdapter)."""
        raise NotImplementedError("SQL execution is not supported for InMemoryAdapter")