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
        self.primary_keys = table.primary_key

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

    # def update(self, conn: Connection, updates: list[dict]):
    #     """
    #     Updates multiple records in the table.

    #     :param conn: Database connection object
    #     :param updates: List of JSON objects, each containing primary key(s) and updated fields.
    #     """
    #     if not isinstance(updates, list):  
    #         raise TypeError("Updates must be a list of dictionaries.")

    #     if not updates:
    #         return  # No updates provided

    #     for update_data in updates:
    #         # Ensure primary key fields exist in the update data
    #         filters = {pk: update_data.pop(pk, None) for pk in self.primary_keys}
            
    #         if None in filters.values():
    #             raise ValueError(f"Each update must include all primary key(s): {self.primary_keys}")

    #         # Build update query
    #         update_query = self.table.update().where(
    #             *[self.table.c[pk] == filters[pk] for pk in self.primary_keys]
    #         ).values(**update_data)

    #         conn.execute(update_query)

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
    """Factory to return the correct SCD scdstrategy based on table type."""

    @staticmethod
    def get_scdstrategy(scd_type: SCDType, table: Table) -> SCDStrategy:
        """Returns the appropriate SCD scdstrategy instance."""
        scdstrategy_map = {
            SCDType.SCDTYPE0: SCDType0Strategy,
            SCDType.SCDTYPE1: SCDType1Strategy,
            SCDType.SCDTYPE2: SCDType2Strategy
        }

        if scd_type not in scdstrategy_map:
            raise ValueError(f"⚠️ Unsupported SCD type: {scd_type}")

        return scdstrategy_map[scd_type](table) 