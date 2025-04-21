from sqlalchemy import Table, MetaData, select, insert, update, delete, inspect
from sqlalchemy.sql import and_, text
from sqlalchemy.exc import OperationalError
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class SQLAlchemyCoreAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.engine = self.connection.get_engine()
        self.session_factory = self.connection.get_session_factory()
        self.metadata = MetaData()

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys match the table's primary keys."""
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            db_keys = [col.name for col in table.primary_key]
            if not db_keys:
                return  # No primary keys, use table_info.keys
            if set(db_keys) != set(table_info_keys):
                raise ValueError(
                    f"Table {table_name} primary keys {db_keys} do not match TableInfo keys {table_info_keys}"
                )
        except Exception as e:
            if self.profile.db_type == "databricks":
                logger.warning(f"Skipping key validation for Databricks table {table_name}: {e}")
                return
            raise ValueError(f"Failed to validate keys for table {table_name}: {e}")

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as conn:
            conn.execute(insert(table), data)
            conn.commit()

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        with self.engine.connect() as conn:
            result = conn.execution_options(yield_per=chunk_size).execute(query)
            for partition in result.partitions():
                yield [dict(row._mapping) for row in partition]

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as conn:
            for update_data in data:
                query = update(table).where(
                    and_(*(table.c[key] == filters[key] for key in filters))
                ).values(**update_data)
                conn.execute(query)
            conn.commit()

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = delete(table).where(
            and_(*(table.c[key] == filters[key] for key in filters))
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return results."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), parameters or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result.fetchall()]
                return []
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            raise

    def list_tables(self, schema: str) -> List[str]:
        """List tables in the given schema."""
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names(schema=schema)
        except Exception as e:
            logger.error(f"Failed to list tables for schema {schema}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in the schema."""
        try:
            inspector = inspect(self.engine)
            metadata = {}
            for table_name in inspector.get_table_names(schema=schema):
                columns = inspector.get_columns(table_name, schema=schema)
                pk_columns = inspector.get_pk_constraint(table_name, schema=schema).get("constrained_columns", [])
                metadata[table_name] = {
                    "columns": {col["name"]: str(col["type"]) for col in columns},
                    "primary_keys": pk_columns
                }
            return metadata
        except Exception as e:
            logger.error(f"Failed to get table metadata for schema {schema}: {e}")
            return {}