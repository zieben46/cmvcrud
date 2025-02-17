from abc import ABC, abstractmethod
from app.models.base_model import SCDType
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.engine.base import Connection

###################################################################################
# 🔹 Abstract Base Strategy Class
class SCDStrategy(ABC):
    """Abstract class for SCD type-based CRUD operations."""

    def __init__(self, table: Table):
        self.table = table

    @abstractmethod
    def create(self, conn: Connection, **kwargs):
        pass

    @abstractmethod
    def read(self, conn: Connection):
        pass

    @abstractmethod
    def update(self, conn: Connection, **kwargs):
        pass

    @abstractmethod
    def delete(self, conn: Connection, **kwargs):
        pass


###################################################################################
# 🔹 SCD Type 0 (Read-Only)
class SCDType0Strategy(SCDStrategy):
    """Handles CRUD operations for SCD Type 0 (Read-Only)."""

    def create(self, conn: Connection, **kwargs):
        raise ValueError("❌ Cannot INSERT into an SCD Type 0 table (Read-Only).")

    def read(self, conn: Connection):
        result = conn.execute(self.table.select()).fetchall()
        return pd.DataFrame([dict(row._mapping) for row in result])

    def update(self, conn: Connection, **kwargs):
        raise ValueError("❌ Cannot UPDATE an SCD Type 0 table (Read-Only).")

    def delete(self, conn: Connection, **kwargs):
        raise ValueError("❌ Cannot DELETE from an SCD Type 0 table (Read-Only).")

###################################################################################
# 🔹 SCD Type 1 (Full Overwrite)
class SCDType1Strategy(SCDStrategy):
    """Handles CRUD operations for SCD Type 1 (Full Overwrite)."""

    def create(self, conn: Connection, **kwargs):
        conn.execute(self.table.insert().values(**kwargs))
        conn.commit()

    def read(self, conn: Connection):
        result = conn.execute(self.table.select()).fetchall()
        return pd.DataFrame([dict(row._mapping) for row in result])

    def update(self, conn: Connection, **kwargs):
        update_query = self.table.update().where(self.table.c.id == kwargs["id"]).values(**kwargs)
        conn.execute(update_query)
        conn.commit()

    def delete(self, conn: Connection, **kwargs):
        delete_query = self.table.delete().where(self.table.c.id == kwargs["id"])
        conn.execute(delete_query)
        conn.commit()

###################################################################################
# 🔹 SCD Type 2 (Append-Only with Soft Deletes)
class SCDType2Strategy(SCDStrategy):
    """Handles CRUD operations for SCD Type 2 (Append-Only, Historical Tracking)."""

    def create(self, conn: Connection, **kwargs):
        kwargs["on_time"] = pd.Timestamp.now()
        conn.execute(self.table.insert().values(**kwargs))
        conn.commit()

    def read(self, conn: Connection):
        result = conn.execute(self.table.select()).fetchall()
        df = pd.DataFrame([dict(row._mapping) for row in result])
        if "off_time" in df.columns:
            df = df[df["off_time"].isna()]
        return df

    def update(self, conn: Connection, **kwargs):
        # Soft close previous record
        conn.execute(self.table.update().where(self.table.c.id == kwargs["id"]).values(off_time=pd.Timestamp.now()))
        # Insert new record with updated values
        conn.execute(self.table.insert().values(**kwargs))
        conn.commit()

    def delete(self, conn: Connection, **kwargs):
        # Mark the record as inactive instead of deleting
        conn.execute(self.table.update().where(self.table.c.id == kwargs["id"]).values(off_time=pd.Timestamp.now()))
        conn.commit()

###################################################################################
class SCDStrategyFactory:
    """Factory to return the correct SCD strategy based on table type."""

    @staticmethod
    def get_strategy(scd_type: SCDType, table: Table) -> SCDStrategy:
        """Returns the appropriate SCD strategy instance."""
        strategy_map = {
            SCDType.SCDTYPE0: SCDType0Strategy,
            SCDType.SCDTYPE1: SCDType1Strategy,
            SCDType.SCDTYPE2: SCDType2Strategy
        }

        if scd_type not in strategy_map:
            raise ValueError(f"⚠️ Unsupported SCD type: {scd_type}")

        return strategy_map[scd_type](table) 