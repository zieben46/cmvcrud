# datastorekit/adapters/csv_adapter.py
import os
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Any, Iterator, Optional, Tuple
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DatastoreOperationError
import logging

logger = logging.getLogger(__name__)

@dataclass
class DBTable:
    table_name: str
    file_path: str

class CSVAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.base_dir = profile.connection_string or "./data"
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, table_name: str) -> str:
        return os.path.join(self.base_dir, f"{table_name}.csv")

    def get_reflected_keys(self, table_name: str) -> List[str]:
        return self.profile.keys.split(",") if self.profile.keys else []

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        try:
            file_path = self._get_file_path(table_name)
            df = pd.DataFrame(columns=schema.keys())
            df.to_csv(file_path, index=False)
            logger.info(f"Created CSV file {file_path}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create table {table_name}: {e}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        try:
            file_path = self._get_file_path(table_name)
            if not os.path.exists(file_path):
                return {}
            df = pd.read_csv(file_path, nrows=1)
            type_map = {
                "int64": "INTEGER",
                "object": "STRING",
                "float64": "FLOAT",
                "datetime64[ns]": "TIMESTAMP",
                "bool": "BOOLEAN"
            }
            return {col: type_map.get(str(df[col].dtype), "STRING") for col in df.columns}
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return {}

    def create(self, table_name: str, records: List[Dict]) -> int:
            dbtable = DBTable(table_name, self._get_file_path(table_name))
            try:
                return self._create_records(dbtable, records)
            except Exception as e:
                logger.error(f"Create failed for {table_name}: {e}")
                raise DatastoreOperationError(f"Error during create on {table_name}: {e}")

    def _create_records(self, dbtable: DBTable, records: List[Dict]) -> int:
        try:
            if not records:
                return 0
            current_df = pd.read_csv(dbtable.file_path) if os.path.exists(dbtable.file_path) else pd.DataFrame()
            new_df = pd.concat([current_df, pd.DataFrame(records)], ignore_index=True)
            temp_path = dbtable.file_path + ".tmp"
            new_df.to_csv(temp_path, index=False)
            os.replace(temp_path, dbtable.file_path)
            return len(records)
        except Exception as e:
            logger.error(f"Error during bulk insert into {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error during bulk insert into {dbtable.table_name}: {e}")

    def read(self, table_name: str, filters: Optional[Dict] = None) -> List[Dict]:
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
            logger.error(f"Read failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during read on {table_name}: {e}")

    def select_chunks(self, table_name: str, filters: Optional[Dict] = None, chunk_size: int = 100000) -> Iterator[List[Dict]]:
        file_path = self._get_file_path(table_name)
        try:
            if not os.path.exists(file_path):
                return
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                if filters:
                    for key, value in filters.items():
                        chunk = chunk[chunk[key] == value]
                yield chunk.to_dict("records")
        except Exception as e:
            logger.error(f"Select chunks failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select_chunks on {table_name}: {e}")

    def update(self, table_name: str, updates: List[Dict]) -> int:
        dbtable = DBTable(table_name, self._get_file_path(table_name))
        try:
            return self._update_records(dbtable, updates)
        except Exception as e:
            logger.error(f"Update failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def _update_records(self, dbtable: DBTable, updates: List[Dict]) -> int:
        try:
            if not os.path.exists(dbtable.file_path):
                return 0
            df = pd.read_csv(dbtable.file_path)
            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for update operations.")

            updated_count = 0
            for update_dict in updates:
                if not all(pk in update_dict for pk in primary_keys):
                    raise ValueError(f"Update dictionary missing primary key(s): {primary_keys}")

                mask = df[primary_keys].eq([update_dict[pk] for pk in primary_keys]).all(axis=1)
                if mask.any():
                    for k, v in update_dict.items():
                        if k not in primary_keys:
                            df.loc[mask, k] = v
                    updated_count += mask.sum()

            temp_path = dbtable.file_path + ".tmp"
            df.to_csv(temp_path, index=False)
            os.replace(temp_path, dbtable.file_path)
            return updated_count
        except Exception as e:
            logger.error(f"Error updating {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error updating {dbtable.table_name}: {e}")

    def delete(self, table_name: str, conditions: List[Dict]) -> int:
        dbtable = DBTable(table_name, self._get_file_path(table_name))
        try:
            return self._delete_records(dbtable, conditions)
        except Exception as e:
            logger.error(f"Delete failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

    def _delete_records(self, dbtable: DBTable, conditions: List[Dict]) -> int:
        try:
            if not os.path.exists(dbtable.file_path):
                return 0
            if not conditions:
                raise ValueError("Conditions list cannot be empty.")

            df = pd.read_csv(dbtable.file_path)
            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for delete operations.")

            initial_count = len(df)
            for cond in conditions:
                if not cond:
                    raise ValueError("Condition dictionary cannot be empty.")
                if not all(key in df.columns for key in cond.keys()):
                    raise ValueError(f"Invalid column name in conditions for table {dbtable.table_name}")

                mask = df[list(cond.keys())].eq(list(cond.values())).all(axis=1)
                df = df[~mask]

            deleted_count = initial_count - len(df)
            temp_path = dbtable.file_path + ".tmp"
            df.to_csv(temp_path, index=False)
            os.replace(temp_path, dbtable.file_path)
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting from {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error deleting from {dbtable.table_name}: {e}")

    def apply_changes(self, table_name: str, changes: List[Dict]) -> Tuple[List[Any], int, int, int]:
        dbtable = DBTable(table_name, self._get_file_path(table_name))
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

            inserted_count = 0
            updated_count = 0
            deleted_count = 0

            current_df = pd.read_csv(dbtable.file_path) if os.path.exists(dbtable.file_path) else pd.DataFrame()
            new_df = current_df.copy()
            primary_keys = self.get_reflected_keys(dbtable.table_name)
            if inserts or updates or deletes:
                if not primary_keys:
                    raise ValueError("Table must have a primary key for operations.")

            if inserts:
                inserted_count = self._create_records(dbtable, inserts)
                new_df = pd.concat([new_df, pd.DataFrame(inserts)], ignore_index=True)

            if updates:
                updated_count = self._update_records(dbtable, updates)
                for update_dict in updates:
                    mask = new_df[primary_keys].eq([update_dict[pk] for pk in primary_keys]).all(axis=1)
                    for k, v in update_dict.items():
                        if k not in primary_keys:
                            new_df.loc[mask, k] = v

            if deletes:
                deleted_count = self._delete_records(dbtable, deletes)
                for cond in deletes:
                    mask = new_df[list(cond.keys())].eq(list(cond.values())).all(axis=1)
                    new_df = new_df[~mask]

            temp_path = dbtable.file_path + ".tmp"
            new_df.to_csv(temp_path, index=False)
            os.replace(temp_path, dbtable.file_path)
            return [], inserted_count, updated_count, deleted_count
        except Exception as e:
            logger.error(f"Error applying changes to {dbtable.table_name}: {e}")
            raise DatastoreOperationError(f"Error applying changes to {dbtable.table_name}: {e}")

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
            columns = self.get_table_columns(table_name)
            pk_columns = self.get_reflected_keys(table_name)
            metadata[table_name] = {
                "columns": columns,
                "primary_keys": pk_columns
            }
        return metadata