# datastorekit/orchestrator.py
from typing import Dict, List, Optional, Union
from sqlalchemy.sql import text
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.profile import DatabaseProfile
from datastorekit.utils import generate_sync_operations
from datastorekit.exceptions import DatastoreOperationError
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

class DataStoreOrchestrator:
    def __init__(self, env_paths: List[str] = None, profiles: List[DatabaseProfile] = None):
        """Initialize with .env file paths or DatabaseProfile instances."""
        self.adapters: Dict[str, DatastoreAdapter] = {}
        if env_paths:
            for path in env_paths:
                profile = self._load_profile(path)
                self._add_adapter(profile)
        if profiles:
            for profile in profiles:
                self._add_adapter(profile)
        if not self.adapters:
            raise ValueError("No valid datastore configurations provided")

    def _load_profile(self, env_path: str) -> DatabaseProfile:
        """Load a DatabaseProfile from a .env file based on its type."""
        env_path_lower = env_path.lower()
        profile_map = {
            "postgres": DatabaseProfile.postgres,
            "databricks": DatabaseProfile.databricks,
            "spark": DatabaseProfile.spark,
            "mongodb": DatabaseProfile.mongodb,
            "csv": DatabaseProfile.csv,
            "inmemory": DatabaseProfile.inmemory,
        }
        for key, profile_func in profile_map.items():
            if key in env_path_lower:
                return profile_func(env_path)
        raise ValueError(f"Unknown datastore type for {env_path}")

    def _add_adapter(self, profile: DatabaseProfile):
        """Add a DatastoreAdapter for a given profile."""
        adapter = self._create_adapter(profile)
        key = f"{profile.dbname}:{profile.schema or 'default'}"
        self.adapters[key] = adapter
        print(f"Initialized datastore: {key}")

    def _create_adapter(self, profile: DatabaseProfile) -> DatastoreAdapter:
        """Create a DatastoreAdapter based on the profile's db_type."""
        adapter_class = AdapterRegistry.get_adapter_class(profile.db_type)
        if not adapter_class:
            raise ValueError(f"Unsupported db_type: {profile.db_type}")
        return adapter_class(profile)

    def process_changes(self, adapter: DatastoreAdapter, table_name: str, source_df: pd.DataFrame, target_df: pd.DataFrame, chunk_size: Optional[int] = None):
        """Process changes (create, update, delete) between source and target DataFrames, chunking both to avoid memory issues.
        
        Args:
            adapter: DatastoreAdapter for the target datastore.
            table_name: Name of the target table.
            source_df: Pandas DataFrame with source data.
            target_df: Pandas DataFrame with target data.
            chunk_size: Optional size of chunks for processing large datasets (default: None, no chunking).
        
        Raises:
            DatastoreOperationError: If processing changes fails.
        """
        try:
            key_columns = adapter.profile.keys.split(",") if adapter.profile.keys else adapter.get_reflected_keys(table_name)
            if not key_columns:
                raise ValueError(f"No primary keys defined for table {table_name}")

            if chunk_size:
                inserted_count = 0
                updated_count = 0
                deleted_count = 0
                source_chunk_count = (len(source_df) + chunk_size - 1) // chunk_size
                target_chunk_count = (len(target_df) + chunk_size - 1) // chunk_size

                for i in range(0, len(source_df), chunk_size):
                    source_chunk = source_df.iloc[i:i + chunk_size]
                    source_chunk_index = i // chunk_size + 1
                    logger.debug(f"Processing source chunk {source_chunk_index}/{source_chunk_count} with {len(source_chunk)} records")

                    # Initialize aggregated changes
                    all_changes = []

                    # Process target_df in chunks
                    for j in range(0, len(target_df), chunk_size):
                        target_chunk = target_df.iloc[j:j + chunk_size]
                        target_chunk_index = j // chunk_size + 1
                        logger.debug(f"Comparing against target chunk {target_chunk_index}/{target_chunk_count} with {len(target_chunk)} records")

                        # Generate changes
                        changes = generate_sync_operations(source_chunk, target_chunk, key_columns)
                        all_changes.extend(changes)

                        # Free memory
                        del target_chunk

                    # Deduplicate changes by operation and primary key
                    unique_changes = []
                    seen_keys = set()
                    for change in all_changes:
                        key_tuple = tuple(change.get(k, "") for k in key_columns)
                        operation_key = (change["operation"], key_tuple)
                        if operation_key not in seen_keys:
                            unique_changes.append(change)
                            seen_keys.add(operation_key)

                    # Apply changes
                    if unique_changes:
                        _, ic, uc, dc = adapter.apply_changes(table_name, unique_changes)
                        inserted_count += ic
                        updated_count += uc
                        deleted_count += dc

                    logger.debug(f"Source chunk {source_chunk_index}: {sum(1 for c in unique_changes if c['operation'] == 'create')} creates, "
                                 f"{sum(1 for c in unique_changes if c['operation'] == 'update')} updates, "
                                 f"{sum(1 for c in unique_changes if c['operation'] == 'delete')} deletes for {table_name}")

                    # Free memory
                    del source_chunk, all_changes, unique_changes, seen_keys

                logger.debug(f"Total: {inserted_count} creates, {updated_count} updates, {deleted_count} deletes for {table_name}")
            else:
                # No chunking: process entire DataFrames (for small datasets)
                changes = generate_sync_operations(source_df, target_df, key_columns)
                logger.debug(f"Computed {sum(1 for c in changes if c['operation'] == 'create')} creates, "
                             f"{sum(1 for c in changes if c['operation'] == 'update')} updates, "
                             f"{sum(1 for c in changes if c['operation'] == 'delete')} deletes for {table_name}")
                if changes:
                    adapter.apply_changes(table_name, changes)

        except Exception as e:
            logger.error(f"Failed to process changes for table {table_name} with adapter {adapter.profile.db_type}: {e}")
            raise DatastoreOperationError(f"Failed to process changes: {e}")

        except Exception as e:
            logger.error(f"Failed to process changes for table {table_name} with adapter {adapter.profile.db_type}: {e}")
            raise DatastoreOperationError(f"Failed to process changes: {e}")

    def sync_tables(self, source_db: str, source_schema: str, source_table_name: str,
                    target_db: str, target_schema: str, target_table_name: str, 
                    filters: Optional[Dict] = None, chunk_size: Optional[int] = None):
        """Synchronize data from a source table to a target table.
        
        Args:
            source_db: Source database name.
            source_schema: Source schema name.
            source_table_name: Source table name.
            target_db: Target database name.
            target_schema: Target schema name.
            target_table_name: Target table name.
            filters: Optional dictionary of filters for source data.
            chunk_size: Optional size of chunks for processing large datasets.
        
        Raises:
            DatastoreOperationError: If synchronization fails.
        """
        try:
            source_key = f"{source_db}:{source_schema or 'default'}"
            target_key = f"{target_db}:{target_schema or 'default'}"
            source_adapter = self.adapters.get(source_key)
            target_adapter = self.adapters.get(target_key)
            if not source_adapter or not target_adapter:
                raise KeyError(f"Source ({source_key}) or target ({target_key}) datastore not found")

            # Initialize table metadata
            source_table_info = TableInfo(
                table_name=source_table_name, 
                keys=source_adapter.profile.keys, 
                scd_type="type1", 
                datastore_key=source_key
            )
            target_table_info = TableInfo(
                table_name=target_table_name, 
                keys=target_adapter.profile.keys,
                scd_type="type1", 
                datastore_key=target_key
            )
            source_table = DBTable(source_table_name, source_table_info, source_adapter)
            target_table = DBTable(target_table_name, target_table_info, target_adapter)

            # Check table existence and schema
            table_exists = target_table_name in self.list_tables(target_db, target_schema)
            source_columns = source_adapter.get_table_columns(source_table_name, source_schema or "default")
            target_columns = target_adapter.get_table_columns(target_table_name, target_schema or "default") if table_exists else {}
            schema_changed = table_exists and source_columns != target_columns

            if not table_exists or schema_changed:
                if table_exists:
                    logger.info(f"Schema changed for {target_schema}.{target_table_name}. Dropping and recreating table.")
                    drop_sql = f"DROP TABLE {target_schema}.{target_table_name}"
                    try:
                        target_adapter.execute_sql(drop_sql)
                    except Exception as e:
                        logger.warning(f"Failed to drop table {target_schema}.{target_table_name}: {e}. Proceeding with recreation.")

                logger.info(f"Creating target table {target_schema}.{target_table_name}...")
                self._create_target_table(target_adapter, target_db, target_schema, target_table, source_table)

                # Load source data
                source_data = source_table.read(filters)
                if not source_data:
                    logger.info(f"No data to insert into {target_schema}.{target_table_name}")
                    return

                if chunk_size:
                    source_df = pd.DataFrame(source_data)
                    for i in range(0, len(source_df), chunk_size):
                        chunk = source_df.iloc[i:i + chunk_size].to_dict("records")
                        changes = [{"operation": "create", **record} for record in chunk]
                        target_adapter.apply_changes(target_table_name, changes)
                        logger.debug(f"Inserted chunk {i // chunk_size + 1} with {len(chunk)} records")
                else:
                    changes = [{"operation": "create", **record} for record in source_data]
                    target_adapter.apply_changes(target_table_name, changes)
                    logger.debug(f"Inserted {len(source_data)} records")

            else:
                logger.info(f"Schema unchanged for {target_schema}.{target_table_name}. Applying changes.")
                source_data = source_table.read(filters)
                target_data = target_table.read()
                source_df = pd.DataFrame(source_data)
                target_df = pd.DataFrame(target_data)
                self.process_changes(target_adapter, target_table_name, source_df, target_df, chunk_size=chunk_size)

            logger.info(f"Synchronized {source_db}:{source_schema}.{source_table_name} "
                        f"to {target_db}:{target_schema}.{target_table_name}")

        except Exception as e:
            logger.error(f"Table synchronization failed from {source_db}:{source_schema}.{source_table_name} "
                         f"to {target_db}:{target_schema}.{target_table_name}: {e}")
            raise DatastoreOperationError(f"Table synchronization failed: {e}")

    def _create_target_table(self, target_adapter: DatastoreAdapter, target_db: str, target_schema: str, 
                             target_table: DBTable, source_table: DBTable):
        """Create the target table based on the source table's schema."""
        try:
            source_columns = source_table.adapter.get_table_columns(source_table.table_name, source_table.info.datastore_key.split(":")[1])
            if not source_columns:
                raise ValueError(f"No columns found for source table {source_table.table_name}")
            target_adapter.create_table(target_table.table_name, source_columns, target_schema)
        except Exception as e:
            logger.error(f"Failed to create target table {target_schema}.{target_table.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create target table: {e}")

    # Other methods (e.g., get_table, list_tables) remain unchanged from the original
    def list_adapters(self) -> List[str]:
        """List all managed datastore keys (e.g., 'dbname:schema')."""
        return list(self.adapters.keys())

    def list_tables(self, db_name: str, schema: str, include_metadata: bool = False) -> Union[List[str], Dict[str, Dict]]:
        """List tables in a specific datastore, optionally with metadata."""
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        if include_metadata:
            return self.get_table_metadata(db_name, schema)
        return self.adapters[key].list_tables(schema or "default")

    def get_table_metadata(self, db_name: str, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in a datastore."""
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        adapter = self.adapters[key]
        metadata = {}
        for table_name in adapter.list_tables(schema or "default"):
            columns = adapter.get_table_columns(table_name, schema)
            pk_columns = adapter.get_reflected_keys(table_name)
            metadata[table_name] = {
                "columns": columns,
                "primary_keys": pk_columns
            }
        return metadata

    def query(self, db_name: str, schema: str, query_str: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw query on a specific datastore."""
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        return self.adapters[key].execute_sql(query_str, parameters)

    def get_table(self, db_key: str, table_info: TableInfo) -> DBTable:
        """Get a DBTable instance for a specific table in a datastore."""
        if db_key not in self.adapters:
            raise KeyError(f"No datastore found for key: {db_key}")
        adapter = self.adapters[db_key]
        adapter.table_info = table_info
        return DBTable(adapter, table_info)

    def _create_target_table(self, target_adapter: DatastoreAdapter, target_db: str, target_schema: str, target_table: DBTable, source_table: DBTable):
        """Create target table using source table's schema."""
        try:
            source_columns = source_table.columns
            if not source_columns:
                # Fallback schema if no columns exist
                source_columns = {
                    "unique_id": "Integer",
                    "secondary_key": "String",
                    "category": "String",
                    "amount": "Float",
                    "start_date": "DateTime",
                    "end_date": "DateTime",
                    "is_active": "Boolean"
                }
            target_adapter.create_table(target_table.table_name, source_columns, target_schema)
            print(f"Created table {target_schema}.{target_table.table_name}")
        except Exception as e:
            raise ValueError(f"Failed to create target table {target_schema}.{target_table.table_name}: {e}")