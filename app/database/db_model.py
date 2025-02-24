from typing import List, Dict, Optional

from sqlalchemy.orm import Session
from sqlalchemy import event
from typing import List, Dict
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine, Connection

from app.config.enums import CrudType, SCDType, DatabaseType
from app.database.scd.db_scd_strategies import SCDStrategyFactory, SCDType0Strategy, SCDType1Strategy, SCDType2Strategy, SCDStrategyFactory

from app.database.metadata_loader import DatabaseMetadata

import os




class DatabaseModel:
    metadata_cache = MetaData()  # ✅ Static metadata cach
    """Handles database interactions and applies SCD strategies."""

    def __init__(self, db: Session, table_name: str):
        self.db = db
        self.table_name = table_name
        self.metadata = self.__class__.metadata_cache
        self._load_metadata()
        self.table = self._get_table(table_name)
        self.scd_type = self._get_scd_type()
        self.scdstrategy = SCDStrategyFactory.get_scdstrategy(self.scd_type, self.table)
        # event.listen(self.db.bind, "after_create", self._on_schema_change)#sq alchem only

    def _load_metadata(self):
        engine = self.db.bind
        if not engine:
            raise ValueError("❌ Session is not bound to an engine!")
        if not self.metadata.tables:  # Only reflect if cache is empty
            self.metadata.reflect(bind=engine)

    # def _on_schema_change(self, target, connection, **kw):
    #     # logger.info("Schema change detected; refreshing metadata")
    #     self.metadata.clear()
    #     self.metadata.reflect(bind=self.db.bind)
    #     self.table = self._get_table(self.table_name)
    #     self.scdstrategy = SCDStrategyFactory.get_scdstrategy(self.scd_type, self.table)
            
    # Call model.refresh_metadata() after adding a table (e.g., via an API endpoint or admin action).
    def refresh_metadata(self):
        engine = self.db.bind
        """Refresh the metadata cache to detect new tables."""
        self.metadata.clear()  # Clear existing tables
        self.metadata.reflect(bind=engine)  # type: ignore # Re-reflect
        # self.table = self._get_table(self.table_name)  # Update table reference
        # self.scdstrategy = SCDStrategyFactory.get_scdstrategy(self.scd_type, self.table)

    @staticmethod
    def refresh_metadata2(db: Session):
        """Refresh the shared metadata cache to include new tables."""
        engine = db.bind
        if not engine:
            raise ValueError("❌ No engine bound to session")
        logger.info("Refreshing metadata cache")
        DatabaseModel.metadata_cache.clear()  # Clear the shared cache
        DatabaseModel.metadata_cache.reflect(bind=engine)  # Reload schema into shared cache

    def _get_table(self, table_name: str) -> Table:
        """Retrieve the table object from metadata."""
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"⚠️ Table `{table_name}` does not exist in the database.")
        return table        

    def _get_scd_type(self) -> SCDType:
        """Determine SCD type dynamically (for now, default to Type 1)."""
        return SCDType.SCDTYPE1  # Future: Fetch from metadata
    

    def execute(self, operation: CrudType, data: List[Dict]):
        """Executes a CRUD operation using the selected SCD strategy."""
        if operation == CrudType.CREATE:
            return self.scdstrategy.create(self.db, data)
        elif operation == CrudType.READ:
            return self.scdstrategy.read(self.db)
        elif operation == CrudType.UPDATE:
            return self.scdstrategy.update(self.db, data)
        elif operation == CrudType.DELETE:
            return self.scdstrategy.delete(self.db, data)
        else:
            raise ValueError(f"Invalid CRUD operation: {operation}")