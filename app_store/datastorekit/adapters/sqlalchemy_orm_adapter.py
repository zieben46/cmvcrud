# datastorekit/adapters/sqlalchemy_orm_adapter.py
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from databricks.sqlalchemy import TIMESTAMP, TINYINT
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.engine import DBEngine
from typing import List, Dict, Any

class SQLAlchemyORMAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.db_engine = DBEngine(profile)
        self.engine = self.db_engine.engine
        self.session_factory = self.db_engine.Session
        self.metadata = MetaData()
        self.base = automap_base()

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys match the table's primary keys."""
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
            table = SampleObject.__table__
            db_keys = [col.name for col in table.primary_key]
            if not db_keys:
                return  # No primary keys in DB, use table_info.keys
            if set(db_keys) != set(table_info_keys):
                raise ValueError(
                    f"Table {table_name} primary keys {db_keys} do not match TableInfo keys {table_info_keys}"
                )
        except KeyError as e:
            # Databricks workaround: Use table_info.keys if ORM mapping fails
            if self.profile.db_type == "databricks":
                return
            raise ValueError(f"Table {table_name} not found or key error: {e}")

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError as e:
            if self.profile.db_type == "databricks":
                # Workaround: Use TableInfo.keys for Databricks
                raise ValueError(f"Table {table_name} not found for Databricks, ensure TableInfo.keys are correct: {e}")
            raise ValueError(f"Table {table_name} not found in the database schema: {e}")
        with self.session_factory() as session:
            for record in data:
                obj = SampleObject(**record)
                session.add(obj)
            session.commit()

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError as e:
            if self.profile.db_type == "databricks":
                raise ValueError(f"Table {table_name} not found for Databricks: {e}")
            raise ValueError(f"Table {table_name} not found in the database schema: {e}")
        with self.session_factory() as session:
            query = select(SampleObject)
            for key, value in filters.items():
                query = query.where(getattr(SampleObject, key) == value)
            records = session.execute(query).scalars().all()
            return [{key: getattr(record, key) for key in record.__dict__.keys() if not key.startswith('_')} for record in records]

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError as e:
            if self.profile.db_type == "databricks":
                raise ValueError(f"Table {table_name} not found for Databricks: {e}")
            raise ValueError(f"Table {table_name} not found in the database schema: {e}")
        with self.session_factory() as session:
            for update_data in data:
                query = select(SampleObject)
                for key, value in filters.items():
                    query = query.where(getattr(SampleObject, key) == value)
                records = session.execute(query).scalars().all()
                for record in records:
                    for key, value in update_data.items():
                        setattr(record, key, value)
            session.commit()

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError as e:
            if self.profile.db_type == "databricks":
                raise ValueError(f"Table {table_name} not found for Databricks: {e}")
            raise ValueError(f"Table {table_name} not found in the database schema: {e}")
        with self.session_factory() as session:
            query = select(SampleObject)
            for key, value in filters.items():
                query = query.where(getattr(SampleObject, key) == value)
            records = session.execute(query).scalars().all()
            for record in records:
                session.delete(record)
            session.commit()