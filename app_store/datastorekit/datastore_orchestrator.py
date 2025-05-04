# datastorekit/orchestrator.py
from typing import Dict, List, Optional, Union
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.sql import text
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.profile import DatabaseProfile
from datastorekit.utils import detect_changes
from datastorekit.exceptions import DatastoreOperationError
import pandas as pd
import logging
import os
from datetime import datetime, date

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
        try:
            key_columns = adapter.profile.keys.split(",") if adapter.profile.keys else adapter.get_reflected_keys(table_name)
            if chunk_size:
                for i in range(0, len(source_df), chunk_size):
                    source_chunk = source_df.iloc[i:i + chunk_size]
                    inserts, updates, deletes = detect_changes(source_chunk, target_df, key_columns)
                    logger.debug(f"Chunk {i // chunk_size + 1}: {len(inserts)} inserts, {len(updates)} updates, {len(deletes)} deletes for {table_name}")
                    adapter.apply_changes(table_name, inserts, updates, deletes)
            else:
                inserts, updates, deletes = detect_changes(source_df, target_df, key_columns)
                logger.debug(f"Computed {len(inserts)} inserts, {len(updates)} updates, {len(deletes)} deletes for {table_name}")
                adapter.apply_changes(table_name, inserts, updates, deletes)
        except Exception as e:
            logger.error(f"Failed to process changes for table {table_name} with adapter {adapter.profile.db_type}: {e}")
            raise DatastoreOperationError(f"Failed to process changes: {e}")

    def sync_tables(self, source_db: str, source_schema: str, source_table_name: str,
                    target_db: str, target_schema: str, target_table_name: str, 
                    filters: Optional[Dict] = None, chunk_size: Optional[int] = None):
        try:
            source_key = f"{source_db}:{source_schema or 'default'}"
            target_key = f"{target_db}:{target_schema or 'default'}"
            source_adapter = self.adapters.get(source_key)
            target_adapter = self.adapters.get(target_key)
            if not source_adapter or not target_adapter:
                raise KeyError(f"Source ({source_key}) or target ({target_key}) datastore not found")

            source_table_info = TableInfo(
                table_name=source_table_name, 
                keys=source_adapter.profile.keys, 
                scd_type="type1", 
                datastore_key=source_key,
                columns={}
            )
            target_table_info = TableInfo(
                table_name=target_table_name, 
                keys=target_adapter.profile.keys,
                scd_type="type1", 
                datastore_key=target_key,
                columns={}
            )
            source_table = DBTable(source_adapter, source_table_info)
            target_table = DBTable(target_adapter, target_table_info)

            table_exists = target_table.table_name in self.list_tables(target_db, target_schema)
            source_schema = source_adapter.get_table_metadata(source_schema or "public").get(source_table.table_name, {})
            target_schema = target_adapter.get_table_metadata(target_schema or "public").get(target_table.table_name, {}) if table_exists else {}
            schema_changed = table_exists and source_schema.get("columns", {}) != target_schema.get("columns", {})

            if not table_exists or schema_changed:
                if table_exists:
                    logger.info(f"Schema changed for {target_schema}.{target_table.table_name}. Dropping and recreating table.")
                    if isinstance(target_adapter, (SQLAlchemyCoreAdapter, SQLAlchemyORMAdapter)):
                        with target_adapter.engine.connect() as conn:
                            conn.execute(text(f"DROP TABLE {target_schema}.{target_table.table_name}"))
                            conn.commit()
                    elif isinstance(target_adapter, MongoDBAdapter):
                        target_adapter.db[target_table.table_name].drop()
                    elif isinstance(target_adapter, SparkAdapter):
                        target_adapter.spark.sql(f"DROP TABLE {target_table.table_name}")
                    elif isinstance(target_adapter, CSVAdapter):
                        if os.path.exists(target_adapter.file_path):
                            os.remove(target_adapter.file_path)
                    else:
                        raise ValueError(f"Unsupported adapter for dropping table: {type(target_adapter)}")

                logger.info(f"Creating target table {target_schema}.{target_table.table_name}...")
                self._create_target_table(target_adapter, target_db, target_schema, target_table, source_table)

                source_data = source_table.read(filters)
                if not source_data:
                    logger.info(f"No data to insert into {target_schema}.{target_table.table_name}")
                    return

                if chunk_size:
                    source_df = pd.DataFrame(source_data)
                    for i in range(0, len(source_df), chunk_size):
                        chunk = source_df.iloc[i:i + chunk_size].to_dict("records")
                        target_adapter.apply_changes(target_table.table_name, inserts=chunk, updates=[], deletes=[])
                        logger.debug(f"Inserted chunk {i // chunk_size + 1} with {len(chunk)} records")
                else:
                    target_adapter.apply_changes(target_table.table_name, inserts=source_data, updates=[], deletes=[])
                    logger.debug(f"Inserted {len(source_data)} records")

            else:
                logger.info(f"Schema unchanged for {target_schema}.{target_table.table_name}. Applying changes.")
                source_data = source_table.read(filters)
                target_data = target_table.read({})
                source_df = pd.DataFrame(source_data)
                target_df = pd.DataFrame(target_data)
                self.process_changes(target_adapter, target_table.table_name, source_df, target_df, chunk_size=chunk_size)

            print(f"Synchronized {source_db}:{source_schema}.{source_table} "
                  f"to {target_db}:{target_schema}.{target_table}")

        except Exception as e:
            logger.error(f"Table synchronization failed from {source_db}:{source_schema}.{source_table} "
                         f"to {target_db}:{target_schema}.{target_table}: {e}")
            raise DatastoreOperationError(f"Table synchronization failed: {e}")

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
        return self.adapters[key].list_tables(schema or "public")

    def get_table_metadata(self, db_name: str, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in a datastore."""
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        return self.adapters[key].get_table_metadata(schema or "public")

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
        try:
            sample_data = source_table.read()
            schema = target_table.table_info.columns if target_table.table_info.columns else {}
            if not sample_data and not schema:
                schema = {
                    "category": String,
                    "amount": Float,
                    "start_date": DateTime,
                    "end_date": DateTime,
                    "is_active": Boolean
                }
            elif sample_data:
                sample_record = sample_data[0]
                for key, value in sample_record.items():
                    if key not in schema:
                        if isinstance(value, str):
                            schema[key] = String
                        elif isinstance(value, float):
                            schema[key] = Float
                        elif isinstance(value, bool):
                            schema[key] = Boolean
                        elif isinstance(value, (datetime, date)):
                            schema[key] = DateTime
                        else:
                            schema[key] = String

            if isinstance(target_adapter, (SQLAlchemyORMAdapter, SQLAlchemyCoreAdapter)):
                metadata = MetaData()
                columns = [
                    Column(col_name, col_type, primary_key=col_name in (target_table.keys or target_adapter.get_reflected_keys(target_table.table_name)))
                    for col_name, col_type in schema.items()
                ]
                Table(target_table.table_name, metadata, *columns, schema=target_schema or None if target_adapter.profile.db_type == "sqlite" else "public")
                metadata.create_all(target_adapter.engine)
                print(f"Created SQL table {target_schema}.{target_table.table_name}")
            elif isinstance(target_adapter, MongoDBAdapter):
                print(f"MongoDB collection {target_table.table_name} will be created on first insert")
            elif isinstance(target_adapter, CSVAdapter):
                print(f"CSV file for {target_table.table_name} will be created on first insert")
            elif isinstance(target_adapter, SparkAdapter):
                print(f"Delta table {target_table.table_name} will be created on first insert")
            elif isinstance(target_adapter, InMemoryAdapter):
                if target_table.table_name not in target_adapter.data:
                    target_adapter.data[target_table.table_name] = []
                print(f"In-memory table {target_table.table_name} initialized")
            else:
                raise ValueError(f"Unsupported adapter type: {type(target_adapter)}")
        except Exception as e:
            raise ValueError(f"Failed to create target table {target_schema}.{target_table.table_name}: {e}")