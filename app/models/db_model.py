from typing import List, Dict, Optional

from typing import List, Dict
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine, Connection
from app.config.db_configs import PostgresConfig
from app.models.base_model import CrudType, SCDType, DatabaseType, BaseModel
from app.models.db_scd_strategies import SCDStrategyFactory
from app.models.metadata_loader import DatabaseMetadata
import os

# Global Metadata object
metadata = MetaData()

class DatabaseModel(BaseModel):
    """Handles direct interaction with the database without using a factory."""

    def __init__(self, database: DatabaseType, table_name: str):
        self._engine = self.create_engine(database)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self._engine)

        self._table = self.get_table(table_name)
        self._scd_type = self._get_scd_type(database, table_name) 
        self._scdstrategy = SCDStrategyFactory.get_scdstrategy(self._scd_type, self._table)

        # HIT A LU TABLE
        # with self._engine.connect() as conn:
            #   metadata_loader = DatabaseMetadata()  
        #     db_metadata = DatabaseMetadata(conn, metadata, "employees")
        #     self._table = db_metadata.ge
        #     self._scdstrategy = db_metadata.get_scd_strategy()



    def create_engine(self, database: DatabaseType) -> Engine:
        """Create and return the database engine directly."""
        if database == DatabaseType.POSTGRES:
            config = PostgresConfig()
            engine = create_engine(config.get_url())
            if engine is None:
                raise ValueError("❌ Failed to create database engine")
            return engine
        else:
            raise ValueError(f"⚠️ Unsupported database type: {database}")

    def get_table(self, table_name: str) -> Table:
        """Retrieve table from metadata."""
        metadata.reflect(bind=self._engine)
        table = metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"⚠️ Table `{table_name}` does not exist in the database.")
        return table

    def _get_scd_type(self, database: DatabaseType, table_name: str) -> SCDType:
        """Determine SCD type (Placeholder logic)."""
        return SCDType.SCDTYPE1  # Default SCD Type 1 (Can be enhanced)

    def execute(self, operation: CrudType, data: Optional[List[Dict]] = None):
        """Executes CRUD operations using the selected SCD strategy."""
        if self._engine is None:
            raise ValueError("❌ Database engine is not initialized!")

        with self._engine.connect() as conn:
            if operation == CrudType.CREATE:
                return self._scdstrategy.create(conn, data or [])
            elif operation == CrudType.READ:
                return self._scdstrategy.read(conn)
            elif operation == CrudType.UPDATE:
                return self._scdstrategy.update(conn, data or [])
            elif operation == CrudType.DELETE:
                return self._scdstrategy.delete(conn, data or [])
            else:
                raise ValueError(f"Invalid CRUD operation: {operation}")
