# datastorekit/datastore_orchestrator.py
from typing import Dict, List
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.sql import text
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
        """Load a DatabaseProfile from a .env file."""
        if "postgres" in env_path.lower():
            return DatabaseProfile.postgres(env_path)
        elif "databricks" in env_path.lower():
            return DatabaseProfile.databricks(env_path)
        elif "spark" in env_path.lower():
            return DatabaseProfile.spark(env_path)
        elif "mongodb" in env_path.lower():
            return DatabaseProfile.mongodb(env_path)
        elif "csv" in env_path.lower():
            return DatabaseProfile.csv(env_path)
        elif "inmemory" in env_path.lower():
            return DatabaseProfile.inmemory(env_path)
        else:
            raise ValueError(f"Unknown datastore type for {env_path}")

    def _add_adapter(self, profile: DatabaseProfile):
        """Add a DatastoreAdapter for a given profile."""
        adapter = self._create_adapter(profile)
        key = f"{profile.dbname}:{profile.schema or 'default'}"
        self.adapters[key] = adapter
        print(f"Initialized datastore: {key}")

    def _create_adapter(self, profile: DatabaseProfile) -> DatastoreAdapter:
        """Create a DatastoreAdapter based on the profile's db_type."""
        if profile.db_type == "csv":
            return CSVAdapter(profile)
        elif profile.db_type == "inmemory":
            return InMemoryAdapter(profile)
        elif profile.db_type == "mongodb":
            return MongoDBAdapter(profile)
        elif profile.db_type == "spark":
            return SparkAdapter(profile)
        elif profile.db_type in ["postgres", "sqlite"]:
            return SQLAlchemyORMAdapter(profile)
        elif profile.db_type == "databricks":
            return SQLAlchemyCoreAdapter(profile)
        else:
            raise ValueError(f"Unsupported db_type: {profile.db_type}")

    def list_adapters(self) -> List[str]:
        """List all managed datastore keys (e.g., 'dbname:schema')."""
        return list(self.adapters.keys())

    def list_tables(self, db_name: str, schema: str) -> List[str]:
        """List tables in a specific datastore."""
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        adapter = self.adapters[key]
        if isinstance(adapter, (SQLAlchemyORMAdapter, SQLAlchemyCoreAdapter)):
            with adapter.session_factory() as session:
                if adapter.profile.db_type == "sqlite":
                    result = session.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    ).fetchall()
                    return [row[0] for row in result]
                else:
                    result = session.execute(
                        text("SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"),
                        {"schema": schema or "public"}
                    ).fetchall()
                    return [row[0] for row in result]
        elif isinstance(adapter, MongoDBAdapter):
            return adapter.db.list_collection_names()
        elif isinstance(adapter, CSVAdapter):
            return [f.replace(".csv", "") for f in os.listdir(adapter.base_dir) if f.endswith(".csv")]
        elif isinstance(adapter, SparkAdapter):
            return [table.tableName for table in adapter.spark.catalog.listTables(adapter.schema)]
        elif isinstance(adapter, InMemoryAdapter):
            return list(adapter.data.keys())
        return []

    def get_table(self, db_key: str, table_info: TableInfo) -> DBTable:
        """Get a DBTable instance for a specific table in a datastore."""
        if db_key not in self.adapters:
            raise KeyError(f"No datastore found for key: {db_key}")
        adapter = self.adapters[db_key]
        adapter.table_info = table_info  # Store table_info for key validation
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
                keys="unique_id" if target_db != "mydb" else "_id",
                scd_type="type1",
                datastore_key=target_key,
                columns={"unique_id": "Integer", "category": "String", "amount": "Float"}
            )
            source_table = DBTable(source_adapter, source_table_info)
            target_table = DBTable(target_adapter, target_table_info)

            target_tables = self.list_tables(target_db, target_schema)
            if target_table.table_name not in target_tables:
                print(f"Target table {target_schema}.{target_table.table_name} does not exist. Creating it...")
                self._create_target_table(target_adapter, target_db, target_schema, target_table, source_table)

            data = source_table.read(filters)
            if not data:
                print(f"No data to replicate from {source_schema}.{source_table.table_name}")
                return

            for record in data:
                record_copy = record.copy()
                if target_db == "mydb":
                    record_copy["_id"] = record_copy.pop("unique_id", None)
                target_table.create([record_copy])

            print(f"Replicated {len(data)} records from {source_db}:{source_schema}.{source_table.table_name} "
                  f"to {target_db}:{target_schema}.{target_table.table_name}")
        except Exception as e:
            print(f"Replication failed: {e}")
            raise

    def _create_target_table(self, target_adapter: DatastoreAdapter, target_db: str, target_schema: str, target_table: DBTable, source_table: DBTable):
        """Create the target table if it doesn't exist, inferring schema from the source table."""
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
                            schema[key] = String  # Fallback

            if isinstance(target_adapter, (SQLAlchemyORMAdapter, SQLAlchemyCoreAdapter)):
                metadata = MetaData()
                columns = []
                for col_name, col_type in schema.items():
                    if col_name in target_table.keys:
                        columns.append(Column(col_name, col_type, primary_key=True))
                    else:
                        columns.append(Column(col_name, col_type))
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
                raise ValueError(f"Unsupported adapter type for table creation: {type(target_adapter)}")
        except Exception as e:
            raise ValueError(f"Failed to create target table {target_schema}.{target_table.table_name}: {e}")