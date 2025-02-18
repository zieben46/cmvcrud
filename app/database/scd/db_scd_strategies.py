from abc import ABC, abstractmethod
from app.config.enums import SCDType
import pandas as pd
from sqlalchemy import Table
from sqlalchemy.orm import Session
from typing import List, Dict

###################################################################################
# 🔹 Abstract Base Strategy Class
class SCDStrategy(ABC):
    """Abstract class for SCD type-based CRUD operations."""

    def __init__(self, table: Table):
        self.table = table
        self.primary_keys = [col.name for col in table.primary_key]

    @abstractmethod
    def create(self, session: Session, data: List[Dict]):
        pass

    @abstractmethod
    def read(self, session: Session):
        pass

    @abstractmethod
    def update(self, session: Session, data: List[Dict]):
        pass

    @abstractmethod
    def delete(self, session: Session, data: List[Dict]):
        pass


###################################################################################
# 🔹 SCD Type 0 (Read-Only)
class SCDType0Strategy(SCDStrategy):
    """Handles CRUD operations for SCD Type 0 (Read-Only)."""

    def create(self, session: Session, data: List[Dict]):
        raise ValueError("❌ Cannot INSERT into an SCD Type 0 table (Read-Only).")

    def read(self, session: Session):
        result = session.execute(self.table.select()).fetchall()
        return [dict(row._mapping) for row in result]

    def update(self, session: Session, data: List[Dict]):
        raise ValueError("❌ Cannot UPDATE an SCD Type 0 table (Read-Only).")

    def delete(self, session: Session, data: List[Dict]):
        raise ValueError("❌ Cannot DELETE from an SCD Type 0 table (Read-Only).")


###################################################################################
# 🔹 SCD Type 1 (Full Overwrite)
class SCDType1Strategy(SCDStrategy):
    """Handles CRUD operations for SCD Type 1 (Full Overwrite)."""

    def create(self, session: Session, data: List[Dict]):
        session.execute(self.table.insert(), data)  # Batch insert
        session.commit()

    def read(self, session: Session):
        result = session.execute(self.table.select()).fetchall()
        return [dict(row._mapping) for row in result]

    def update(self, session: Session, data: List[Dict]):
        """Updates multiple records in the table using a session."""
        try:
            for entry in data:
                filters = {pk: entry.pop(pk, None) for pk in self.primary_keys}

                if None in filters.values():
                    raise ValueError(f"Each update must include all primary key(s): {self.primary_keys}")

                update_query = self.table.update().where(
                    *[self.table.c[pk] == filters[pk] for pk in self.primary_keys]
                ).values(**entry)

                session.execute(update_query)

            session.commit()  # Commit once after all updates
        except Exception as e:
            session.rollback()  # Rollback on failure
            raise e

    def delete(self, session: Session, data: List[Dict]):
        """Deletes multiple records from the table using a session."""
        try:
            for entry in data:
                filters = {pk: entry.get(pk) for pk in self.primary_keys}

                if None in filters.values():
                    raise ValueError(f"Each delete must include all primary key(s): {self.primary_keys}")

                delete_query = self.table.delete().where(
                    *[self.table.c[pk] == filters[pk] for pk in self.primary_keys]
                )

                session.execute(delete_query)

            session.commit()  # Commit once after all deletes
        except Exception as e:
            session.rollback()  # Rollback on failure
            raise e


###################################################################################
# 🔹 SCD Type 2 (Append-Only with Soft Deletes)
class SCDType2Strategy(SCDStrategy):
    """Handles CRUD operations for SCD Type 2 (Append-Only, Historical Tracking)."""

    def create(self, session: Session, data: List[Dict]):
        for entry in data:
            entry["on_time"] = pd.Timestamp.now()
        session.execute(self.table.insert(), data)
        session.commit()

    def read(self, session: Session):
        result = session.execute(self.table.select()).fetchall()
        records = [dict(row._mapping) for row in result]
        if records and "off_time" in records[0]:  
            records = [record for record in records if record["off_time"] is None]
        return records

    def update(self, session: Session, data: List[Dict]):
        """SCD Type 2 Update: Soft-closes previous records, then inserts new ones."""
        try:
            for entry in data:
                filters = {pk: entry.get(pk) for pk in self.primary_keys}
                if None in filters.values():
                    raise ValueError(f"Each update must include all primary key(s): {self.primary_keys}")

                # Soft close the previous record
                session.execute(
                    self.table.update().where(
                        *[self.table.c[pk] == filters[pk] for pk in self.primary_keys]
                    ).values(off_time=pd.Timestamp.now())
                )

                # Insert the new record with a fresh timestamp
                entry["on_time"] = pd.Timestamp.now()
                session.execute(self.table.insert().values(**entry))

            session.commit()  # Commit once after all operations
        except Exception as e:
            session.rollback()  # Rollback on failure
            raise e

    def delete(self, session: Session, data: List[Dict]):
        """SCD Type 2 Delete: Marks the record as inactive instead of deleting."""
        try:
            for entry in data:
                filters = {pk: entry.get(pk) for pk in self.primary_keys}
                if None in filters.values():
                    raise ValueError(f"Each delete must include all primary key(s): {self.primary_keys}")

                session.execute(
                    self.table.update().where(
                        *[self.table.c[pk] == filters[pk] for pk in self.primary_keys]
                    ).values(off_time=pd.Timestamp.now())
                )

            session.commit()  # Commit once after all deletes
        except Exception as e:
            session.rollback()  # Rollback on failure
            raise e


###################################################################################
# 🔹 Factory Class
class SCDStrategyFactory:
    """Factory to return the correct SCD strategy based on table type."""

    @staticmethod
    def get_scdstrategy(scd_type: SCDType, table: Table) -> SCDStrategy:
        """Returns the appropriate SCD strategy instance."""
        scdstrategy_map = {
            SCDType.SCDTYPE0: SCDType0Strategy,
            SCDType.SCDTYPE1: SCDType1Strategy,
            SCDType.SCDTYPE2: SCDType2Strategy
        }

        if scd_type not in scdstrategy_map:
            raise ValueError(f"⚠️ Unsupported SCD type: {scd_type}")

        return scdstrategy_map[scd_type](table)
    

  
