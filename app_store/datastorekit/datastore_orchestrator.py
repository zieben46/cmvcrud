# datastorekit/datastore_orchestrator.py
from typing import Dict, List
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.sql import text
from datastorekit.adapters.sqlalchemy_adapter import SQLAlchemyAdapter
from datastorekit.adapters.mongodb_adapter import MongoDBAdapter
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.models.db_table import DBTable
from datastorekit.profile import DatabaseProfile

class DataStoreOrchestrator:
    def __init__(self, env_paths: List[str] = None, profiles: List[DatabaseProfile] = None):
        """Initialize with .env file paths or DatabaseProfile instances.

        Args:
            env_paths: List of paths to .env files (e.g., ['.env/.env.postgres']).
            profiles: List of DatabaseProfile instances (e.g., for SQLite in-memory testing).

        Raises:
            ValueError: If neither env_paths nor profiles is provided, or if configurations are invalid.
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
        """Load a DatabaseProfile from a .env file.

        Args:
            env_path: Path to the .env file.

        Returns:
            DatabaseProfile instance for the datastore.

        Raises:
            ValueError: If the .env file is invalid or unsupported.
        """
        if "postgres" in env_path.lower():
            return DatabaseProfile.postgres(env_path)
        elif "databricks" in env_path.lower():
            return DatabaseProfile.databricks(env_path)
        elif "mongodb" in env_path.lower():
            return DatabaseProfile.mongodb(env_path)
        else:
            raise ValueError(f"Unknown datastore type for {env_path}")

    def _add_adapter(self, profile: DatabaseProfile):
        """Add a DatastoreAdapter for a given profile.

        Args:
            profile: DatabaseProfile instance.
        """
        adapter = self._create_adapter(profile)
        key = f"{profile.dbname}:{profile.schema or 'default'}"
        self.adapters[key] = adapter
        print(f"Initialized datastore: {key}")

    def _create_adapter(self, profile: DatabaseProfile) -> DatastoreAdapter:
        """Create a DatastoreAdapter based on the profile's db_type.

        Args:
            profile: DatabaseProfile instance.

        Returns:
            DatastoreAdapter instance for the datastore.

        Raises:
            ValueError: If the db_type is unsupported.
        """
        if profile.db_type in ["postgres", "databricks", "sqlite"]:
            return SQLAlchemyAdapter(profile)
        elif profile.db_type == "mongodb":
            return MongoDBAdapter(profile)
        else:
            raise ValueError(f"Unsupported db_type: {profile.db_type}")

    def list_adapters(self) -> List[str]:
        """List all managed datastore keys (e.g., 'dbname:schema').

        Returns:
            List of datastore keys.
        """
        return list(self.adapters.keys())

    def list_tables(self, db_name: str, schema: str) -> List[str]:
        """List tables in a specific datastore.

        Args:
            db_name: Database name (e.g., 'spend_plan_db').
            schema: Schema name (e.g., 'safe_user', 'default' for SQLite).

        Returns:
            List of table names.

        Raises:
            KeyError: If the datastore key is not found.
        """
        key = f"{db_name}:{schema or 'default'}"
        if key not in self.adapters:
            raise KeyError(f"No datastore found for key: {key}")
        adapter = self.adapters[key]
        if isinstance(adapter, SQLAlchemyAdapter):
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
        return []

    def get_table(self, db_key: str, table_info: Dict[str, Any]) -> DBTable:
        """Get a DBTable instance for a specific table in a datastore.

        Args:
            db_key: Datastore key (e.g., 'spend_plan_db:safe_user').
            table_info: Dictionary with table metadata (e.g., {'table_name': 'spend_plan', 'scd_type': 'type1'}).

        Returns:
            DBTable instance for the specified table.

        Raises:
            KeyError: If the db_key is not found.
        """
        if db_key not in self.adapters:
            raise KeyError(f"No datastore found for key: {db_key}")
        adapter = self.adapters[db_key]
        return DBTable(adapter, table_info)

    def replicate(self, source_db: str, source_schema: str, source_table: str,
                  target_db: str, target_schema: str, target_table: str, filters: Dict = None):
        """Replicate data from a source table to a target table across datastores, creating the target table if it doesn't exist.

        Args:
            source_db: Source database name (e.g., 'spend_plan_db').
            source_schema: Source schema (e.g., 'safe_user').
            source_table: Source table name (e.g., 'spend_plan').
            target_db: Target database name (e.g., 'mydb').
            target_schema: Target schema (e.g., 'default').
            target_table: Target table name (e.g., 'spend_plan_copy').
            filters: Optional filters for selecting records (e.g., {'category': 'Food'}).

        Raises:
            KeyError: If source or target datastore is not found.
            ValueError: If replication fails due to schema incompatibility or other issues.
        """
        try:
            source_key = f"{source_db}:{source_schema or 'default'}"
            target_key = f"{target_db}:{target_schema or 'default'}"
            source_adapter = self.adapters.get(source_key)
            target_adapter = self.adapters.get(target_key)
            if not source_adapter or not target_adapter:
                raise KeyError(f"Source ({source_key}) or target ({target_key}) datastore not found")

            source_table_info = {"table_name": source_table, "scd_type": "type1", "key": "id"}
            target_table_info = {"table_name": target_table, "scd_type": "type1", "key": "_id" if target_db == "mydb" else "id"}
            source_table = DBTable(source_adapter, source_table_info)
            target_table = DBTable(target_adapter, target_table_info)

            target_tables = self.list_tables(target_db, target_schema)
            if target_table not in target_tables:
                print(f"Target table {target_schema}.{target_table} does not exist. Creating it...")
                self._create_target_table(target_adapter, target_db, target_schema, target_table, source_table)

            data = source_table.read(filters)
            if not data:
                print(f"No data to replicate from {source_schema}.{source_table}")
                return

            for record in data:
                record_copy = record.copy()
                if target_db == "mydb":
                    record_copy["_id"] = record_copy.pop("id", None)
                target_table.create(record_copy)

            print(f"Replicated {len(data)} records from {source_db}:{source_schema}.{source_table} "
                  f"to {target_db}:{target_schema}.{target_table}")
        except Exception as e:
            print(f"Replication failed: {e}")
            raise

    def _create_target_table(self, target_adapter: DatastoreAdapter, target_db: str, target_schema: str, target_table: str, source_table: DBTable):
        """Create the target table if it doesn't exist, inferring schema from the source table.

        Args:
            target_adapter: Target DatastoreAdapter instance.
            target_db: Target database name.
            target_schema: Target schema name.
            target_table: Target table name.
            source_table: Source DBTable instance for schema inference.

        Raises:
            ValueError: If table creation fails.
        """
        try:
            sample_data = source_table.read(filters=None)
            if not sample_data:
                schema = {
                    "id": Integer if target_db != "mydb" else String,
                    "category": String,
                    "amount": Float,
                    "start_date": DateTime,
                    "end_date": DateTime,
                    "is_active": Boolean
                }
            else:
                sample_record = sample_data[0]
                schema = {}
                for key, value in sample_record.items():
                    if key == "id" and target_db == "mydb":
                        schema["_id"] = String
                    elif key == "id":
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

            if isinstance(target_adapter, SQLAlchemyAdapter):
                metadata = MetaData()
                columns = []
                for col_name, col_type in schema.items():
                    if col_name == "id" and col_name != "_id":
                        columns.append(Column(col_name, col_type, primary_key=True))
                    else:
                        columns.append(Column(col_name, col_type))
                Table(target_table, metadata, *columns, schema=target_schema or None if target_adapter.profile.db_type == "sqlite" else "public")
                metadata.create_all(target_adapter.engine)
                print(f"Created SQL table {target_schema}.{target_table}")
            elif isinstance(target_adapter, MongoDBAdapter):
                print(f"MongoDB collection {target_table} will be created on first insert")
            else:
                raise ValueError(f"Unsupported adapter type for table creation: {type(target_adapter)}")
        except Exception as e:
            raise ValueError(f"Failed to create target table {target_schema}.{target_table}: {e}")