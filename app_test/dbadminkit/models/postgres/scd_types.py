import logging

from typing import Dict, Any, List
from sqlalchemy import Table
from sqlalchemy.orm import Session
from sqlalchemy.sql import select, insert, update, delete
from app_test.dbadminkit.core.scd_handler import SCDTableHandler
from app_test.dbadminkit.core.crud_types import CRUDOperation

logger = logging.getLogger(__name__)

class SCDType0Handler(SCDTableHandler):
    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 0: Immutable - bulk create."""
        if not data:
            return []
        session.execute(insert(self.table).values(data))
        # Removed session.commit()
        return data

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 0: Read records."""
        stmt = select(self.table)
        if data:
            filters = data[0]
            stmt = stmt.where(*[getattr(self.table.c, k) == v for k, v in filters.items()])
        result = session.execute(stmt).fetchall()
        return [dict(row) for row in result]

    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 0: No updates allowed."""
        return []  # Immutable, so return empty list

    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """SCD Type 0: No deletes allowed."""
        pass  # Immutable, do nothing

class SCDType1Handler(SCDTableHandler):
    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        try:
            if not data:
                return []
            session.execute(insert(self.table).values(data))
            return data
        except Exception as e:
            logger.error(f"Failed to create records in {self.table.name}: {e}")
            raise

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Read records."""
        stmt = select(self.table)
        if data:
            filters = data[0]
            stmt = stmt.where(*[getattr(self.table.c, k) == v for k, v in filters.items()])
        result = session.execute(stmt).fetchall()
        return [dict(row) for row in result]

    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        try:
            if not data:
                return []
            for row in data:
                filters = row.get("filters", {self.key: self._get_key_value(row)})
                stmt = update(self.table).where(
                    *[getattr(self.table.c, k) == v for k, v in filters.items()]
                ).values(**{k: v for k, v in row.items() if k != "filters"})
                session.execute(stmt)
            return data
        except Exception as e:
            logger.error(f"Failed to update records in {self.table.name}: {e}")
            raise

    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """SCD Type 1: Bulk delete."""
        if not data:
            return
        for row in data:
            stmt = delete(self.table).where(self.table.c[self.key] == self._get_key_value(row))
            session.execute(stmt)
        # Removed session.commit()

class SCDType2Handler(SCDTableHandler):
    def __init__(self, table: Table, key: str):
        super().__init__(table, key)
        self.active_col = "is_active"
        self.on_date_col = "on_date"
        self.off_date_col = "off_date"

    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 2: Bulk create with versioning."""
        if not data:
            return []
        for row in data:
            row[self.active_col] = True
            row[self.on_date_col] = row.get(self.on_date_col, "CURRENT_TIMESTAMP")
            row[self.off_date_col] = None
        session.execute(insert(self.table).values(data))
        # Removed session.commit()
        return data

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 2: Read records."""
        stmt = select(self.table)
        if data:
            filters = data[0]
            stmt = stmt.where(*[getattr(self.table.c, k) == v for k, v in filters.items()])
        result = session.execute(stmt).fetchall()
        return [dict(row) for row in result]

    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 2: Bulk update with versioning."""
        if not data:
            return []
        for row in data:
            key_value = self._get_key_value(row)
            filters = row.get("filters", {})
            # Mark existing active record as inactive
            session.execute(
                update(self.table)
                .where(
                    self.table.c[self.key] == key_value,
                    self.table.c[self.active_col] == True,
                    *[getattr(self.table.c, k) == v for k, v in filters.items()]
                )
                .values({self.active_col: False, self.off_date_col: row.get(self.on_date_col, "CURRENT_TIMESTAMP")})
            )
            # Insert new version
            row[self.active_col] = True
            row[self.on_date_col] = row.get(self.on_date_col, "CURRENT_TIMESTAMP")
            row[self.off_date_col] = None
            session.execute(insert(self.table).values(row))
        # Removed session.commit()
        return data

    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """SCD Type 2: Bulk delete."""
        if not data:
            return
        for row in data:
            key_value = self._get_key_value(row)
            stmt = delete(self.table).where(self.table.c[self.key] == key_value)
            session.execute(stmt)
        # Removed session.commit()