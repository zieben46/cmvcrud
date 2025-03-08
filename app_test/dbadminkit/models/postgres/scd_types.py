from typing import Dict, Any, List
from sqlalchemy import Table
from sqlalchemy.orm import Session
from sqlalchemy.sql import select, insert, update, delete
from dbadminkit.core.scd_base import SCDBase
from dbadminkit.core.crud_operations import CRUDOperation

class SCDType0(SCDBase):
    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 0: Immutable - bulk create."""
        if not data:
            return []
        session.execute(insert(self.table).values(data))
        session.commit()
        return data

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 0: Read records."""
        stmt = select(self.table)
        if data:
            # Apply filters from the first dict (assuming uniform filters for simplicity)
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

class SCDType1(SCDBase):
    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Bulk create."""
        if not data:
            return []
        session.execute(insert(self.table).values(data))
        session.commit()
        return data

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Read records."""
        stmt = select(self.table)
        if data:
            filters = data[0]
            stmt = stmt.where(*[getattr(self.table.c, k) == v for k, v in filters.items()])
        result = session.execute(stmt).fetchall()
        return [dict(row) for row in result]

    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Bulk update by overwriting."""
        if not data:
            return []
        for row in data:
            filters = row.get("filters", {self.key: self._get_key_value(row)})
            stmt = update(self.table).where(
                *[getattr(self.table.c, k) == v for k, v in filters.items()]
            ).values(**{k: v for k, v in row.items() if k != "filters"})
            session.execute(stmt)
        session.commit()
        return data

    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """SCD Type 1: Bulk delete."""
        if not data:
            return
        for row in data:
            stmt = delete(self.table).where(self.table.c[self.key] == self._get_key_value(row))
            session.execute(stmt)
        session.commit()

class SCDType2(SCDBase):
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
        session.commit()
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
        session.commit()
        return data

    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """SCD Type 2: Bulk delete."""
        if not data:
            return
        for row in data:
            key_value = self._get_key_value(row)
            stmt = delete(self.table).where(self.table.c[self.key] == key_value)
            session.execute(stmt)
        session.commit()