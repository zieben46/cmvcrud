# datastorekit/adapters/sqlalchemy_core_adapter.py
from sqlalchemy import create_engine, Table, MetaData, select, insert, update, delete
from sqlalchemy.sql import and_
from sqlalchemy.exc import OperationalError
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.engine import DBEngine
from typing import List, Dict, Any, Iterator
import logging

logger = logging.getLogger(__name__)

class SQLAlchemyCoreAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.db_engine = DBEngine(profile)
        self.engine = self.db_engine.engine
        self.session_factory = self.db_engine.Session
        self.metadata = MetaData()

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys match the table's primary keys."""
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            db_keys = [col.name for col in table.primary_key]
            if not db_keys:
                return  # No primary keys in DB, use table_info.keys
            if set(db_keys) != set(table_info_keys):
                raise ValueError(
                    f"Table {table_name} primary keys {db_keys} do not match TableInfo keys {table_info_keys}"
                )
        except Exception as e:
            if self.profile.db_type == "databricks":
                return  # Use table_info.keys for Databricks
            raise ValueError(f"Failed to validate keys for table {table_name}: {e}")

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as conn:
            conn.execute(insert(table), data)
            conn.commit()

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchall()
            return [dict(row._mapping) for row in result]

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        for attempt in range(3):
            try:
                with self.engine.connect() as conn:
                    result = conn.execution_options(yield_per=chunk_size).execute(query)
                    for partition in result.partitions():
                        yield [dict(row._mapping) for row in partition]
                break
            except OperationalError as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt == 2:
                    raise

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.connect() as conn:
            for update_data in data:
                query = update(table).where(
                    and_(*(table.c[key] == filters[key] for key in filters))
                ).values(**update_data)
                conn.execute(query)
            conn.commit()

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        query = delete(table).where(
            and_(*(table.c[key] == filters[key] for key in filters))
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()