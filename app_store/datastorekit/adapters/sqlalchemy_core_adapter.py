from sqlalchemy import Table, MetaData, select, insert, update, delete, inspect, Column, and_, text, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker
from dataclasses import dataclass
from typing import List, Dict, Any, Iterator, Optional, Tuple
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from datastorekit.exceptions import DuplicateKeyError, NullValueError, DatastoreOperationError
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

@dataclass
class DBTable:
    table_name: str
    table: Table

class SQLAlchemyCoreAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.engine = self.connection.get_engine()
        self.session_factory = self.connection.get_session_factory()
        self.metadata = MetaData()
        self.Session = self.session_factory

    @contextmanager
    def get_session(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _handle_db_error(self, e: Exception, operation: str, table_name: str):
        if isinstance(e, IntegrityError):
            error_message = str(e.orig).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during {operation} on {table_name}: {e.orig}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during {operation} on {table_name}: {e.orig}")
        raise DatastoreOperationError(f"Error during {operation} on {table_name}: {e}")

    def get_reflected_keys(self, table_name: str) -> List[str]:
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            return [col.name for col in table.primary_key]
        except Exception as e:
            logger.error(f"Failed to get reflected keys for table {table_name}: {e}")
            return []

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
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
            table = Table(table_name, self.metadata, *columns, schema=schema_name or self.profile.schema or "public")
            self.metadata.create_all(self.engine)
            logger.info(f"Created table {schema_name or self.profile.schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create table {table_name}: {e}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=schema_name or self.profile.schema or "public")
            return {col["name"]: str(col["type"]) for col in columns}
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return {}

    def create(self, table_name: str, records: List[Dict]) -> int:
        dbtable = DBTable(table_name, Table(table_name, self.metadata, autoload_with=self.engine))
        with self.get_session() as session:
            try:
                return self.create_records_with_session(dbtable, records, session)
            except Exception as e:
                self._handle_db_error(e, "create", table_name)

    def create_records_with_session(self, dbtable: DBTable, records: List[Dict], session: sessionmaker) -> int:
        try:
            if not records:
                return 0
            stmt = insert(dbtable.table).values(records)
            result = session.execute(stmt)
            return result.rowcount
        except Exception as e:
            raise Exception(f"Error during bulk insert into {dbtable.table_name}: {e}")

    def read(self, table_name: str, filters: Optional[Dict] = None) -> List[Dict]:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        if filters:
            for key, value in filters.items():
                query = query.where(table.c[key] == value)
        try:
            with self.Session() as session:
                result = session.execute(query).fetchall()
                return [dict(row._mapping) for row in result]
        except Exception as e:
            self._handle_db_error(e, "read", table_name)

    def select_chunks(self, table_name: str, filters: Optional[Dict] = None, chunk_size: int = 100000) -> Iterator[List[Dict]]:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        if filters:
            for key, value in filters.items():
                query = query.where(table.c[key] == value)
        try:
            with self.engine.connect() as conn:
                result = conn.execution_options(yield_per=chunk_size).execute(query)
                for partition in result.partitions():
                    yield [dict(row._mapping) for row in partition]
        except Exception as e:
            self._handle_db_error(e, "select_chunks", table_name)

    def update(self, table_name: str, updates: List[Dict]) -> int:
        dbtable = DBTable(table_name, Table(table_name, self.metadata, autoload_with=self.engine))
        with self.get_session() as session:
            try:
                return self.update_records_with_session(dbtable, updates, session)
            except Exception as e:
                self._handle_db_error(e, "update", table_name)

    def update_records_with_session(self, dbtable: DBTable, updates: List[Dict], session: sessionmaker) -> int:
        try:
            primary_keys = [col.name for col in dbtable.table.primary_key]
            if not primary_keys:
                raise ValueError("Table must have a primary key for update operations.")
            total_updated = 0
            for update_dict in updates:
                if not all(pk in update_dict for pk in primary_keys):
                    raise ValueError(f"Update dictionary missing primary key(s): {primary_keys}")
                stmt = (
                    update(dbtable.table)
                    .where(*[dbtable.table.columns[pk] == update_dict[pk] for pk in primary_keys])
                    .values({k: v for k, v in update_dict.items() if k not in primary_keys})
                )
                result = session.execute(stmt)
                total_updated += result.rowcount
            return total_updated
        except Exception as e:
            raise Exception(f"Error updating {dbtable.table_name}: {e}")

    def delete(self, table_name: str, conditions: List[Dict]) -> int:
        dbtable = DBTable(table_name, Table(table_name, self.metadata, autoload_with=self.engine))
        with self.get_session() as session:
            try:
                return self.delete_records_with_session(dbtable, conditions, session)
            except Exception as e:
                self._handle_db_error(e, "delete", table_name)

    def delete_records_with_session(self, dbtable: DBTable, conditions: List[Dict], session: sessionmaker) -> int:
        try:
            if not conditions:
                raise ValueError("Conditions list cannot be empty.")
            where_clauses = []
            for cond in conditions:
                if not cond:
                    raise ValueError("Condition dictionary cannot be empty.")
                clause = []
                for key, value in cond.items():
                    if key not in dbtable.table.columns:
                        raise ValueError(f"Invalid column name: {key} in table {dbtable.table_name}")
                    clause.append(dbtable.table.columns[key] == value)
                where_clauses.append(and_(*clause))
            stmt = delete(dbtable.table).where(or_(*where_clauses))
            result = session.execute(stmt)
            return result.rowcount
        except Exception as e:
            raise Exception(f"Error deleting from {dbtable.table_name}: {e}")

    def apply_changes(self, table_name: str, changes: List[Dict], session: Optional[sessionmaker] = None) -> Tuple[List[Any], int, int, int]:
        created_session = False
        if session is None:
            session = self.Session()
            created_session = True
        dbtable = DBTable(table_name, Table(table_name, self.metadata, autoload_with=self.engine))
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
            inserted_count = self.create_records_with_session(dbtable, inserts, session) if inserts else 0
            updated_count = self.update_records_with_session(dbtable, updates, session) if updates else 0
            deleted_count = self.delete_records_with_session(dbtable, deletes, session) if deletes else 0
            if created_session:
                session.commit()
            return [], inserted_count, updated_count, deleted_count
        except Exception as e:
            if created_session:
                session.rollback()
            raise Exception(f"Error applying changes to {dbtable.table_name}: {e}")
        finally:
            if created_session:
                session.close()

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