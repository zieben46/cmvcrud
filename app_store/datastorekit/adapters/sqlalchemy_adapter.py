# datastorekit/adapters/sqlalchemy_adapter.py
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from databricks.sqlalchemy import TIMESTAMP, TINYINT
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.engine import DBEngine
from typing import List, Dict, Any

class SQLAlchemyAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.db_engine = DBEngine(profile)
        self.engine = self.db_engine.engine
        self.session_factory = self.db_engine.Session
        self.metadata = MetaData()
        self.base = automap_base()

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        schema = self.profile.schema if self.profile.db_type != "sqlite" else None
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError:
            raise ValueError(f"Table '{table_name}' not found in the database schema")
        with self.session_factory() as session:
            for record in data:
                obj = SampleObject(**record)
                session.add(obj)
            session.commit()

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        schema = self.profile.schema if self.profile.db_type != "sqlite" else None
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError:
            raise ValueError(f"Table '{table_name}' not found in the database schema")
        with self.session_factory() as session:
            query = select(SampleObject)
            for key, value in filters.items():
                query = query.where(getattr(SampleObject, key) == value)
            records = session.execute(query).scalars().all()
            return [{key: getattr(record, key) for key in record.__dict__.keys() if not key.startswith('_')} for record in records]

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        schema = self.profile.schema if self.profile.db_type != "sqlite" else None
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError:
            raise ValueError(f"Table '{table_name}' not found in the database schema")
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
        schema = self.profile.schema if self.profile.db_type != "sqlite" else None
        self.base.prepare(autoload_with=self.engine)
        try:
            SampleObject = self.base.classes[table_name]
        except KeyError:
            raise ValueError(f"Table '{table_name}' not found in the database schema")
        with self.session_factory() as session:
            query = select(SampleObject)
            for key, value in filters.items():
                query = query.where(getattr(SampleObject, key) == value)
            records = session.execute(query).scalars().all()
            for record in records:
                session.delete(record)
            session.commit()