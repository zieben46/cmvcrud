# datastorekit/adapters/csv_adapter.py
import os
import pandas as pd
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DatastoreOperationError
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class CSVAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.base_dir = profile.connection_string or "./data"
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, table_name: str) -> str:
        return os.path.join(self.base_dir, f"{table_name}.csv")
    
    def get_reflected_keys(self, table_name: str) -> List[str]:
        return []

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        file_path = self._get_file_path(table_name)
        try:
            current_df = pd.read_csv(file_path) if os.path.exists(file_path) else pd.DataFrame()
            new_df = pd.concat([current_df, pd.DataFrame(data)], ignore_index=True)
            new_df.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Insert failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during insert on {table_name}: {e}")

    def select(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        file_path = self._get_file_path(table_name)
        try:
            if not os.path.exists(file_path):
                return []
            df = pd.read_csv(file_path)
            if filters:
                for key, value in filters.items():
                    df = df[df[key] == value]
            return df.to_dict("records")
        except Exception as e:
            logger.error(f"Select failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select on {table_name}: {e}")

    def select_chunks(self, table_name: str, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        file_path = self._get_file_path(table_name)
        try:
            if not os.path.exists(file_path):
                return
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                if filters:
                    for key, value in filters.items():
                        chunk = chunk[chunk[key] == value]
                yield chunk.to_dict('records')
        except Exception as e:
            logger.error(f"Select chunks failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select_chunks on {table_name}: {e}")

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None):
        file_path = self._get_file_path(table_name)
        try:
            if not os.path.exists(file_path):
                return
            df = pd.read_csv(file_path)
            for update_data in data:
                if filters:
                    mask = df[list(filters.keys())].eq(list(filters.values())).all(axis=1)
                else:
                    key_columns = self.get_reflected_keys(table_name) or list(update_data.keys())
                    mask = df[key_columns].eq([update_data[k] for k in key_columns]).all(axis=1)
                for k, v in update_data.items():
                    df.loc[mask, k] = v
            df.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Update failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def delete(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        file_path = self._get_file_path(table_name)
        try:
            if not os.path.exists(file_path):
                return
            df = pd.read_csv(file_path)
            if filters:
                mask = df[list(filters.keys())].eq(list(filters.values())).all(axis=1)
                df = df[~mask]
            else:
                df = pd.DataFrame(columns=df.columns)
            df.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Delete failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        file_path = self._get_file_path(table_name)
        try:
            current_df = pd.read_csv(file_path) if os.path.exists(file_path) else pd.DataFrame()
            new_df = current_df.copy()
            key_columns = self.get_reflected_keys(table_name) or (self.profile.keys.split(",") if self.profile.keys else [])

            if inserts:
                new_df = pd.concat([new_df, pd.DataFrame(inserts)], ignore_index=True)
                logger.debug(f"Applied {len(inserts)} inserts to {table_name}")

            if updates:
                for update_data in updates:
                    mask = new_df[key_columns].eq([update_data.get(k) for k in key_columns]).all(axis=1)
                    for k, v in update_data.items():
                        if k not in key_columns:
                            new_df.loc[mask, k] = v
                logger.debug(f"Applied {len(updates)} updates to {table_name}")

            if deletes:
                key_tuples = [tuple(d.get(key) for key in key_columns) for d in deletes]
                mask = new_df[key_columns].apply(tuple, axis=1).isin(key_tuples)
                new_df = new_df[~mask]
                logger.debug(f"Applied {len(deletes)} deletes to {table_name}")

            temp_path = file_path + ".tmp"
            new_df.to_csv(temp_path, index=False)
            os.replace(temp_path, file_path)
        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError("SQL execution is not supported for CSVAdapter")

    def list_tables(self, schema: str) -> List[str]:
        try:
            return [os.path.splitext(f)[0] for f in os.listdir(self.base_dir) if f.endswith(".csv")]
        except Exception as e:
            logger.error(f"Failed to list tables in {self.base_dir}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        metadata = {}
        for table_name in self.list_tables(schema):
            file_path = self._get_file_path(table_name)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, nrows=1)
                columns = {col: str(df[col].dtype) for col in df.columns}
                pk_columns = self.profile.keys.split(",") if self.profile.keys else []
                metadata[table_name] = {
                    "columns": columns,
                    "primary_keys": pk_columns
                }
        return metadata