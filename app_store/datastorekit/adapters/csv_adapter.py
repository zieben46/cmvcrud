# datastorekit/adapters/csv_adapter.py
import os
import pandas as pd
from datastorekit.adapters.base import DatastoreAdapter
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class CSVAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.base_dir = profile.connection_string or "./data"
        self.db_type = profile.db_type
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, table_name: str) -> str:
        """Get the file path for a table."""
        return os.path.join(self.base_dir, f"{table_name}.csv")

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys are present in the CSV."""
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return
        df = pd.read_csv(file_path, nrows=1)
        columns = set(df.columns)
        if not all(key in columns for key in table_info_keys):
            raise ValueError(
                f"Table {table_name} CSV columns {columns} do not include all TableInfo keys {table_info_keys}"
            )

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        df = pd.DataFrame(data)
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            df = pd.concat([existing_df, df], ignore_index=True)
        df.to_csv(file_path, index=False)

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return []
        df = pd.read_csv(file_path)
        for key, value in filters.items():
            df = df[df[key] == value]
        return df.to_dict('records')

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            for key, value in filters.items():
                chunk = chunk[chunk[key] == value]
            yield chunk.to_dict('records')

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return
        df = pd.read_csv(file_path)
        mask = True
        for key, value in filters.items():
            mask &= df[key] == value
        for update_data in data:
            for key, value in update_data.items():
                df.loc[mask, key] = value
        df.to_csv(file_path, index=False)

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return
        df = pd.read_csv(file_path)
        mask = True
        for key, value in filters.items():
            mask &= df[key] == value
        df = df[~mask]
        df.to_csv(file_path, index=False)

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query (not supported for CSV)."""
        raise NotImplementedError("SQL execution is not supported for CSVAdapter")