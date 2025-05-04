# ```python
from sqlalchemy import inspect, select, text
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class SQLAlchemyORMAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.engine = self.connection.get_engine()
        self.session_factory = self.connection.get_session_factory()
        self.base = automap_base()

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys match the table's primary keys."""
        self.base.prepare(autoload_with=self.engine)
        try:
            table_obj = self.base.classes[table_name]
            table = table_obj.__table__
            db_keys = [col.name for col in table.primary_key]
            if not db_keys:
                return
            if set(db_keys) != set(table_info_keys):
                raise ValueError(
                    f"Table {table_name} primary keys {db_keys} do not match TableInfo keys {table_info_keys}"
                )
        except KeyError as e:
            logger.warning(f"Table {table_name} not found, skipping key validation: {e}")
            return

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        self.base.prepare(autoload_with=self.engine)
        table_obj = self.base.classes[table_name]
        with self.session_factory() as session:
            for record in data:
                obj = table_obj(**record)
                session.add(obj)
            session.commit()

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        self.base.prepare(autoload_with=self.engine)
        table_obj = self.base.classes[table_name]
        with self.session_factory() as session:
            query = select(table_obj)
            for key, value in filters.items():
                query = query.where(getattr(table_obj, key) == value)
            records = session.execute(query).scalars().all()
            return [{key: getattr(record, key) for key in record.__dict__.keys() if not key.startswith('_')} for record in records]

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        self.base.prepare(autoload_with=self.engine)
        table_obj = self.base.classes[table_name]
        with self.session_factory() as session:
            query = select(table_obj)
            for key, value in filters.items():
                query = query.where(getattr(table_obj, key) == value)
            result = session.execution_options(yield_per=chunk_size).execute(query)
            for partition in result.partitions():
                yield [{key: getattr(record, key) for key in record.__dict__.keys() if not key.startswith('_')} for record in partition]

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        self.base.prepare(autoload_with=self.engine)
        table_obj = self.base.classes[table_name]
        with self.session_factory() as session:
            for update_data in data:
                query = select(table_obj)
                for key, value in filters.items():
                    query = query.where(getattr(table_obj, key) == value)
                records = session.execute(query).scalars().all()
                for record in records:
                    for key, value in update_data.items():
                        setattr(record, key, value)
            session.commit()

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        self.base.prepare(autoload_with=self.engine)
        table_obj = self.base.classes[table_name]
        with self.session_factory() as session:
            query = select(table_obj)
            for key, value in filters.items():
                query = query.where(getattr(table_obj, key) == value)
            records = session.execute(query).scalars().all()
            for record in records:
                session.delete(record)
            session.commit()

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return results."""
        try:
            with self.session_factory() as session:
                result = session.execute(text(sql), parameters or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result.fetchall()]
                session.commit()
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