import os
import pandas as pd
from datastorekit.adapters.base import DatastoreAdapter
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class CSVAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.base_dir = profile.connection_string or "./data"
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
        self.validate_keys(table_name, self.table_info.keys.split(","))
        file_path = self._get_file_path(table_name)
        df = pd.DataFrame(data)
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            df = pd.concat([existing_df, df], ignore_index=True)
        df.to_csv(file_path, index=False)

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return []
        df = pd.read_csv(file_path)
        for key, value in filters.items():
            df = df[df[key] == value]
        return df.to_dict('records')

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        file_path = self._get_file_path(table_name)
        if not os.path.exists(file_path):
            return
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            for key, value in filters.items():
                chunk = chunk[chunk[key] == value]
            yield chunk.to_dict('records')

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
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
        self.validate_keys(table_name, self.table_info.keys.split(","))
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

    def list_tables(self, schema: str) -> List[str]:
        """List tables (CSV files) in the base directory."""
        try:
            return [os.path.splitext(f)[0] for f in os.listdir(self.base_dir) if f.endswith(".csv")]
        except Exception as e:
            logger.error(f"Failed to list tables in {self.base_dir}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all CSV tables."""
        metadata = {}
        for table_name in self.list_tables(schema):
            file_path = self._get_file_path(table_name)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, nrows=1)
                columns = {col: str(df[col].dtype) for col in df.columns}
                pk_columns = self.table_info.keys.split(",") if self.table_info else []
                metadata[table_name] = {
                    "columns": columns,
                    "primary_keys": pk_columns
                }
        return metadata
    
    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        key_columns = self.profile.keys.split(",")  # e.g., ["key1", "key2"]
        try:
            # Load current data
            current_df = pd.read_csv(self.file_path)
            new_df = current_df.copy()

            # Batch inserts
            if inserts:
                new_df = pd.concat([new_df, pd.DataFrame(inserts)], ignore_index=True)
                logger.debug(f"Applied {len(inserts)} inserts to {table_name}")

            # Batch updates
            if updates:
                for update_data in updates:
                    key_values = tuple(update_data[key] for key in key_columns)
                    mask = new_df[key_columns].eq(key_values).all(axis=1)
                    for k, v in update_data.items():
                        if k not in key_columns:
                            new_df.loc[mask, k] = v
                logger.debug(f"Applied {len(updates)} updates to {table_name}")

            # Batch deletes
            if deletes:
                key_tuples = [tuple(d[key] for key in key_columns) for d in deletes]
                mask = new_df[key_columns].apply(tuple, axis=1).isin(key_tuples)
                new_df = new_df[~mask]
                logger.debug(f"Applied {len(deletes)} deletes to {table_name}")

            # Write to temporary file
            temp_path = self.file_path + ".tmp"
            new_df.to_csv(temp_path, index=False)
            # Replace original file only on success
            os.replace(temp_path, self.file_path)

        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")