from typing import Dict, List, Optional, Union
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.adapters.sqlalchemy_orm_adapter import SQLAlchemyORMAdapter
from datastorekit.adapters.sqlalchemy_core_adapter import SQLAlchemyCoreAdapter
from datastorekit.adapters.csv_adapter import CSVAdapter
from datastorekit.adapters.in_memory_adapter import InMemoryAdapter
from datastorekit.adapters.mongodb_adapter import MongoDBAdapter
from datastorekit.adapters.spark_adapter import SparkAdapter
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.profile import DatabaseProfile
import os
from datetime import datetime, date

class AdapterRegistry:
    """Registry for mapping db_types to adapter classes."""
    _adapters: Dict[str, type[DatastoreAdapter]] = {
        "csv": CSVAdapter,
        "inmemory": InMemoryAdapter,
        "mongodb": MongoDBAdapter,
        "spark": SparkAdapter,
        "postgres": SQLAlchemyORMAdapter,
        "sqlite": SQLAlchemyORMAdapter,
        "databricks": SQLAlchemyCoreAdapter,
    }

    @classmethod
    def register_adapter(cls, db_type: str, adapter_class: type[DatastoreAdapter]):
        """Register a custom adapter for a given db_type."""
        cls._adapters[db_type] = adapter_class

    @classmethod
    def get_adapter_class(cls, db_type: str) -> Optional[type[DatastoreAdapter]]:
        """Retrieve the adapter class for a given db_type."""
        return cls._adapters.get(db_type)

class DataStoreOrchestrator:
    def __init__(self, env_paths: List[str] = None, profiles: List[DatabaseProfile] = None):
        """Initialize with .env file paths or DatabaseProfile instances.

        Args:
            env_paths: List of paths to .env files for database configurations.
            profiles: List of DatabaseProfile instances for direct configuration.
        """
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
        """Private: Load a DatabaseProfile from a .env file based on its type."""
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
        """Private: Add a DatastoreAdapter for a given profile."""
        adapter = self._create_adapter(profile)
        key = f"{profile.dbname}:{profile.schema or 'default'}"
        self.adapters[key] = adapter
        print(f"Initialized datastore: {key}")

    def _create_adapter(self, profile: DatabaseProfile) -> DatastoreAdapter:
        """Private: Create a DatastoreAdapter based on the profile's db_type."""
        adapter_class = AdapterRegistry.get_adapter_class(profile.db_type)
        if not adapter_class:
            raise ValueError(f"Unsupported db_type: {profile.db_type}")
        return adapter_class(profile)

    def list_adapters(self) -> List[str]:
        """List all managed datastore keys (e.g., 'dbname:schema')."""
        return list(self.adapters.keys())

    def list_tables(self, db_name: str, schema: str, include_metadata: bool = False) -> Union[List[str], Dict[str, Dict]]:
        """List tables in a specific datastore, optionally with metadata.

        Args:
            db_name: Name of the database.
            schema: Schema name (or None for default).
            include_metadata: If True, return table metadata instead of just names.

        Returns:
            List of table names or dict with table metadata.
        """
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        if include_metadata:
            return self.get_table_metadata(db_name, schema)
        return self.adapters[key].list_tables(schema or "public")

    def get_table_metadata(self, db_name: str, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in a datastore.

        Args:
            db_name: Name of the database.
            schema: Schema name (or None for default).

        Returns:
            Dict with table names as keys and metadata (e.g., columns, keys) as values.
        """
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        return self.adapters[key].get_table_metadata(schema or "public")

    def query(self, db_name: str, schema: str, query_str: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw query on a specific datastore.

        Args:
            db_name: Name of the database.
            schema: Schema name (or None for default).
            query_str: Query string (e.g., SQL for SQL-based adapters).
            parameters: Optional query parameters.

        Returns:
            List of result records.
        """
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

    def replicate(self, source_db: str, source_schema: str, source_table: str,
                  target_db: str, target_schema: str, target_table: str, filters: Dict = None):
        """Replicate data from a source table to a target table across datastores."""
        try:
            source_key = f"{source_db}:{source_schema or 'default'}"
            target_key = f"{target_db}:{target_schema or 'default'}"
            source_adapter = self.adapters.get(source_key)
            target_adapter = self.adapters.get(target_key)
            if not source_adapter or not target_adapter:
                raise KeyError(f"Source ({source_key}) or target ({target_key}) datastore not found")

            source_table_info = TableInfo(
                table_name=source_table,
                keys="unique_id",
                scd_type="type1",
                datastore_key=source_key,
                columns={"unique_id": "Integer", "category": "String", "amount": "Float"}
            )
            target_table_info = TableInfo(
                table_name=target_table,
                keys="_id" if target_db == "mydb" else "unique_id",
                scd_type="type1",
                datastore_key=target_key,
                columns={"unique_id": "Integer", "category": "String", "amount": "Float"}
            )
            source_table = DBTable(source_adapter, source_table_info)
            target_table = DBTable(target_adapter, target_table_info)

            target_tables = self.list_tables(target_db, target_schema)
            if target_table not in target_tables:
                print(f"Creating target table {target_schema}.{target_table}...")
                self._create_target_table(target_adapter, target_db, target_schema, target_table, source_table)

            data = source_table.read(filters)
            if not data:
                print(f"No data to replicate from {source_schema}.{source_table}")
                return

            for record in data:
                record_copy = record.copy()
                if target_db == "mydb":
                    record_copy["_id"] = record_copy.pop("unique_id", None)
                target_table.create([record_copy])

            print(f"Replicated {len(data)} records from {source_db}:{source_schema}.{source_table} "
                  f"to {target_db}:{target_schema}.{target_table}")
        except Exception as e:
            print(f"Replication failed: {e}")
            raise

    def _create_target_table(self, target_adapter: DatastoreAdapter, target_db: str, target_schema: str, target_table: DBTable, source_table: DBTable):
        """Private: Create the target table if it doesn't exist, inferring schema from the source table."""
        try:
            sample_data = source_table.read(filters=None)
            schema = target_table.table_info.columns if target_table.table_info.columns else {}
            if not sample_data and not schema:
                schema = {
                    "unique_id": Integer,
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
                        if key == "unique_id" and target_db == "mydb":
                            schema["_id"] = String
                        elif key == "unique_id":
                            schema[key] = Integer
                        elif isinstance(value, str):
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
                    Column(col_name, col_type, primary_key=col_name in target_table.table_info.keys.split(","))
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
                print(f"In-memory table {target_table.table_name} initialized")
            else:
                raise ValueError(f"Unsupported adapter type: {type(target_adapter)}")
        except Exception as e:
            raise ValueError(f"Failed to create target table {target_schema}.{target_table.table_name}: {e}")