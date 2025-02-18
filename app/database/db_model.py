from typing import List, Dict, Optional

from sqlalchemy.orm import Session
from typing import List, Dict
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine, Connection

from app.config.enums import CrudType, SCDType, DatabaseType
from app.database.scd.db_scd_strategies import SCDStrategyFactory, SCDType0Strategy, SCDType1Strategy, SCDType2Strategy, SCDStrategyFactory

from app.database.metadata_loader import DatabaseMetadata

import os

class DatabaseModel:
    """Handles database interactions and applies SCD strategies."""

    def __init__(self, db: Session, table_name: str):
        """Initialize the database model with an active session."""
        self.db = db
        self.table_name = table_name
        self.metadata = MetaData()
        self._load_metadata()

        self.table = self._get_table(table_name)
        self.scd_type = self._get_scd_type()
        # self.scdstrategy = SCDType1Strategy
        self.scdstrategy = SCDStrategyFactory.get_scdstrategy(self.scd_type, self.table)

    def _load_metadata(self):
        """Load metadata once from the database using session.bind."""
        engine = self.db.bind
        if engine is None:
            raise ValueError("❌ Session is not bound to an engine!")

        if not self.metadata.tables:
            self.metadata.reflect(bind=engine)

    def _get_table(self, table_name: str) -> Table:
        """Retrieve the table object from metadata."""
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"⚠️ Table `{table_name}` does not exist in the database.")
        return table        

    def _get_scd_type(self) -> SCDType:
        """Determine SCD type dynamically (for now, default to Type 1)."""
        return SCDType.SCDTYPE1  # Future: Fetch from metadata

    def _select_scdstrategy(self, scd_type: SCDType):
        """Returns the correct SCD strategy for the table."""
        strategy_map = {
            SCDType.SCDTYPE0: SCDType0Strategy,
            SCDType.SCDTYPE1: SCDType1Strategy,
            SCDType.SCDTYPE2: SCDType2Strategy

        }
        # return strategy_map[scd_type](self.db)
        return SCDType1Strategy

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