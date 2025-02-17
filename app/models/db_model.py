from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from app.config.db_configs import PostgresConfig
from app.models.base_model import CrudType, SCDType, DatabaseType
import os
from typing import Dict, Callable

# Import the strategies
from app.models.db_scd_strategies import SCDStrategyFactory

metadata = MetaData()

class DatabaseFactory:
    """Factory for creating database components."""
    def __init__(self, getenv_func=os.getenv):
        self.getenv_func = getenv_func
        
    def create_engine(self, database: DatabaseType):
        """Create and return the database engine."""
        if database == DatabaseType.POSTGRES:
            config = PostgresConfig(self.getenv_func)
            engine = create_engine(config.get_url())
            if engine is None:
                raise ValueError("❌ Failed to create database engine")
            return engine
        else:
            raise ValueError(f"⚠️ Unsupported database type: {database}")
    
    def load_metadata(self, engine):
        if not metadata.tables:
            metadata.reflect(bind=engine)
    
    def get_table(self, table_name):
        table = metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"\u26a0\ufe0f Table `{table_name}` does not exist in the database.")
        return table
    
    def get_scd_type(self):
        return SCDType.SCDTYPE1  # Placeholder implementation
    
    def select_scdstrategy(self, scd_type, table):
        return SCDStrategyFactory.get_scdstrategy(scd_type, table)

class DatabaseModel:
    """Handles direct interaction with the database (Data Model Layer)."""

    def __init__(self, database: DatabaseType, table_name: str, factory: DatabaseFactory = None):
        """Initialize the database model with a factory for better testability."""
        self.table_name = table_name
        self.factory = factory or DatabaseFactory()
        self.engine = self.factory.create_engine(database)
        self.factory.load_metadata(self.engine)
        self.table = self.factory.get_table(table_name)
        self.scd_type = self.factory.get_scd_type()
        self.scdstrategy = self.factory.select_scdstrategy(self.scd_type, self.table)

    def execute(self, operation: CrudType, **kwargs):
        """Executes CRUD operations using the selected scdstrategy."""
        if self.engine is None:
            raise ValueError("❌ Database engine is not initialized!")
        
        with self.engine.connect() as conn:
            if operation == CrudType.CREATE:
                return self.scdstrategy.create(conn, **kwargs)
            elif operation == CrudType.READ:
                return self.scdstrategy.read(conn)
            elif operation == CrudType.UPDATE:
                return self.scdstrategy.update(conn, **kwargs)
            elif operation == CrudType.DELETE:
                return self.scdstrategy.delete(conn, **kwargs)
            else:
                raise ValueError(f"Invalid CRUD operation: {operation}")
