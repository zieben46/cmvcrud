from sqlalchemy import Table, select
from app.models.base_model import DatabaseType, SCDType
from app.models.db_scd_strategies import SCDStrategyFactory
from typing import Optional

class DatabaseMetadata:
    """Handles database interaction, fetching table metadata and SCD strategy."""

    def __init__(self, conn, metadata, table_name: str):
        """Initialize with an existing connection and metadata."""
        self._conn = conn  # ✅ Use provided connection
        self._metadata = metadata  # ✅ Use provided metadata (already reflected)
        self._table_name = table_name
        self._table = self._get_table()
        self._scd_strategy = self._get_scd_strategy()

    def _get_table(self) -> Optional[Table]:
        """Fetch the table object from metadata."""
        table = self._metadata.tables.get(self._table_name)
        if table is None:
            raise ValueError(f"⚠️ Table `{self._table_name}` does not exist in the database.")
        return table

    def _get_scd_strategy(self):
        """Retrieve the SCD strategy for this table."""
        query = select(self._metadata.tables["table_metadata"].c.scd_type).where(
            self._metadata.tables["table_metadata"].c.table_name == self._table_name
        )
        result = self._conn.execute(query).fetchone()

        if not result:
            raise ValueError(f"⚠️ No SCD type defined for `{self._table_name}` in `table_metadata`.")
        
        scd_type = SCDType(result[0])  # Convert to SCDType Enum
        return SCDStrategyFactory.get_scdstrategy(scd_type, self._table) # type: ignore

    def get_scd_strategy(self):
        """Public method to return the selected SCD strategy."""
        return self._scd_strategy

    def get_table(self):
        return self._table