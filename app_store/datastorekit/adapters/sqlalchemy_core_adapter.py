from sqlalchemy import Table, MetaData, select, insert, update, delete, inspect
from sqlalchemy.sql import and_, text
from sqlalchemy.exc import IntegrityError, OperationalError
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from datastorekit.exceptions import DuplicateKeyError, NullValueError, DatastoreOperationError
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

    def _handle_db_error(self, e: Exception, operation: str, table_name: str):
        """Translate database errors into custom exceptions."""
        if isinstance(e, IntegrityError):
            error_message = str(e.orig).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during {operation} on {table_name}: {e.orig}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during {operation} on {table_name}: {e.orig}")
        raise DatastoreOperationError(f"Error during {operation} on {table_name}: {e}")

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

    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                    updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        """Apply inserts, updates, and deletes atomically using batch operations."""
        key_columns = self.profile.keys.split(",")
        self.validate_keys(table_name, key_columns)
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # Explicit transaction
                    # Batch inserts
                    if inserts:
                        conn.execute(insert(table), inserts)
                        logger.debug(f"Applied {len(inserts)} inserts to {table_name}")

                    # Batch updates
                    if updates:
                        for update_data in updates:
                            update_values = {k: v for k, v in update_data.items() if k not in key_columns}
                            if not update_values:
                                continue
                            where_clause = and_(*(table.c[key] == update_data[key] for key in key_columns))
                            query = update(table).where(where_clause).values(**update_values)
                            conn.execute(query)
                        logger.debug(f"Applied {len(updates)} updates to {table_name}")

                    # Batch deletes
                    if deletes:
                        key_tuples = [tuple(d[key] for key in key_columns) for d in deletes]
                        if key_tuples:
                            where_clause = or_(
                                and_(*(table.c[key] == key_tuple[i] for i, key in enumerate(key_columns)))
                                for key_tuple in key_tuples
                            )
                            query = delete(table).where(where_clause)
                            conn.execute(query)
                        logger.debug(f"Applied {len(deletes)} deletes to {table_name}")

        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            self._handle_db_error(e, "apply_changes", table_name)

    # Existing methods (insert, update, delete, select, etc.) unchanged or updated as needed
    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.profile.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # Use explicit transaction
                    conn.execute(insert(table), data)
        except Exception as e:
            logger.error(f"Insert failed for table {table_name}: {e}")
            self._handle_db_error(e, "insert", table_name)

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.profile.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    for update_data in data:
                        query = update(table).where(
                            and_(*(table.c[key] == filters[key] for key in filters))
                        ).values(**update_data)
                        conn.execute(query)
        except Exception as e:
            logger.error(f"Update failed for table {table_name}: {e}")
            self._handle_db_error(e, "update", table_name)

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.profile.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = delete(table).where(
            and_(*(table.c[key] == filters[key] for key in filters))
        )
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(query)
        except Exception as e:
            logger.error(f"Delete failed for table {table_name}: {e}")
            self._handle_db_error(e, "delete", table_name)

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.profile.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.profile.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        with self.engine.connect() as conn:
            result = conn.execution_options(yield_per=chunk_size).execute(query)
            for partition in result.partitions():
                yield [dict(row._mapping) for row in partition]

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), parameters or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result.fetchall()]
                return []
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            self._handle_db_error(e, "execute_sql", "unknown")

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