# models/databricks/table_models.py
from typing import Dict, Any, List
from sqlalchemy import Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import select, insert, update, delete
from dbadminkit.core.table_interface import TableInterface
from dbadminkit.core.crud_operations import CRUDOperation
from dbadminkit.core.crud_base import CRUDBase
from dbadminkit.core.scd_base import SCDBase

class SCDType1(SCDBase):
    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        if not data:
            return []
        session.execute(insert(self.table).values(data))
        session.commit()
        return data

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        stmt = select(self.table)
        if data:
            filters = data[0]
            stmt = stmt.where(*[getattr(self.table.c, k) == v for k, v in filters.items()])
        result = session.execute(stmt).fetchall()
        return [dict(row) for row in result]

    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
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
        if not data:
            return
        for row in data:
            stmt = delete(self.table).where(self.table.c[self.key] == self._get_key_value(row))
            session.execute(stmt)
        session.commit()
