# datastorekit/adapters/sqlalchemy_orm_adapter.py
from sqlalchemy import inspect, select, text
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import Integer, String, Float, DateTime, Boolean
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from datastorekit.exceptions import DuplicateKeyError, NullValueError, DatastoreOperationError
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
        self.metadata = self.base.metadata

    def get_reflected_keys(self, table_name: str) -> List[str]:
        try:
            self.base.prepare(autoload_with=self.engine)
            table = self.base.classes[table_name].__table__
            return [col.name for col in table.primary_key]
        except KeyError as e:
            logger.error(f"Failed to get reflected keys for table {table_name}: {e}")
            return []

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        """Create a table with the given schema."""
        try:
            type_map = {
                "Integer": Integer,
                "String": String,
                "Float": Float,
                "DateTime": DateTime,
                "Boolean": Boolean
            }
            columns = [
                Column(col_name, type_map.get(col_type, String), primary_key=col_name in ["unique_id", "secondary_key"])
                for col_name, col_type in schema.items()
            ]
            Table(table_name, self.metadata, *columns, schema=schema_name or self.profile.schema or "public")
            self.metadata.create_all(self.engine)
            self.base.prepare(autoload_with=self.engine)  # Refresh ORM mappings
            logger.info(f"Created table {schema_name or self.profile.schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create table {table_name}: {e}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        """Get column names and types for a table."""
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=schema_name or self.profile.schema or "public")
            return {col["name"]: str(col["type"]) for col in columns}
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return {}

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        try:
            self.base.prepare(autoload_with=self.engine)
            table_obj = self.base.classes[table_name]
            with self.session_factory() as session:
                for record in data:
                    obj = table_obj(**record)
                    session.add(obj)
                session.commit()
        except IntegrityError as e:
            error_message = str(e.orig).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during insert on {table_name}: {e.orig}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during insert on {table_name}: {e.orig}")
            raise DatastoreOperationError(f"Error during insert on {table_name}: {e}")
        except Exception as e:
            logger.error(f"Insert failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during insert on {table_name}: {e}")

    def select(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            self.base.prepare(autoload_with=self.engine)
            table_obj = self.base.classes[table_name]
            with self.session_factory() as session:
                query = select(table_obj)
                if filters:
                    for key, value in filters.items():
                        query = query.where(getattr(table_obj, key) == value)
                records = session.execute(query).scalars().all()
                return [{key: getattr(record, key) for key in record.__dict__.keys() if not key.startswith('_')} for record in records]
        except Exception as e:
            logger.error(f"Select failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select on {table_name}: {e}")

    def select_chunks(self, table_name: str, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        try:
            self.base.prepare(autoload_with=self.engine)
            table_obj = self.base.classes[table_name]
            with self.session_factory() as session:
                query = select(table_obj)
                if filters:
                    for key, value in filters.items():
                        query = query.where(getattr(table_obj, key) == value)
                result = session.execution_options(yield_per=chunk_size).execute(query)
                for partition in result.partitions():
                    yield [{key: getattr(record, key) for key in record.__dict__.keys() if not key.startswith('_')} for record in partition]
        except Exception as e:
            logger.error(f"Select chunks failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during select_chunks on {table_name}: {e}")

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None):
        try:
            self.base.prepare(autoload_with=self.engine)
            table_obj = self.base.classes[table_name]
            with self.session_factory() as session:
                query = select(table_obj)
                if filters:
                    for key, value in filters.items():
                        query = query.where(getattr(table_obj, key) == value)
                records = session.execute(query).scalars().all()
                for record in records:
                    for update_data in data:
                        for key, value in update_data.items():
                            setattr(record, key, value)
                session.commit()
        except IntegrityError as e:
            error_message = str(e.orig).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during update on {table_name}: {e.orig}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during update on {table_name}: {e.orig}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")
        except Exception as e:
            logger.error(f"Update failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def delete(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        try:
            self.base.prepare(autoload_with=self.engine)
            table_obj = self.base.classes[table_name]
            with self.session_factory() as session:
                query = select(table_obj)
                if filters:
                    for key, value in filters.items():
                        query = query.where(getattr(table_obj, key) == value)
                records = session.execute(query).scalars().all()
                for record in records:
                    session.delete(record)
                session.commit()
        except Exception as e:
            logger.error(f"Delete failed for table {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        try:
            self.base.prepare(autoload_with=self.engine)
            table_obj = self.base.classes[table_name]
            key_columns = self.get_reflected_keys(table_name)
            with self.session_factory() as session:
                # Inserts
                for record in inserts:
                    obj = table_obj(**record)
                    session.add(obj)
                logger.debug(f"Applied {len(inserts)} inserts to {table_name}")

                # Updates
                for update_data in updates:
                    query = select(table_obj)
                    for key in key_columns:
                        if key in update_data:
                            query = query.where(getattr(table_obj, key) == update_data[key])
                    records = session.execute(query).scalars().all()
                    for record in records:
                        for key, value in update_data.items():
                            if key not in key_columns:
                                setattr(record, key, value)
                logger.debug(f"Applied {len(updates)} updates to {table_name}")

                # Deletes
                for delete_data in deletes:
                    query = select(table_obj)
                    for key in key_columns:
                        if key in delete_data:
                            query = query.where(getattr(table_obj, key) == delete_data[key])
                    records = session.execute(query).scalars().all()
                    for record in records:
                        session.delete(record)
                logger.debug(f"Applied {len(deletes)} deletes to {table_name}")

                session.commit()
        except IntegrityError as e:
            error_message = str(e.orig).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during apply_changes on {table_name}: {e.orig}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during apply_changes on {table_name}: {e.orig}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            with self.session_factory() as session:
                result = session.execute(text(sql), parameters or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result.fetchall()]
                session.commit()
                return []
        except IntegrityError as e:
            error_message = str(e.orig).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during execute_sql: {e.orig}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during execute_sql: {e.orig}")
            raise DatastoreOperationError(f"Error during execute_sql: {e}")
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            raise DatastoreOperationError(f"Error during execute_sql: {e}")

    def list_tables(self, schema: str) -> List[str]:
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names(schema=schema)
        except Exception as e:
            logger.error(f"Failed to list tables for schema {schema}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
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