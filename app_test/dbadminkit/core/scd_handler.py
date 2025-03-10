from abc import ABC, abstractmethod
from typing import Dict, Any, List
from sqlalchemy import Table
from sqlalchemy.orm import Session
from app_test.dbadminkit.core.crud_types import CRUDOperation

class SCDTableHandler(ABC):
    def __init__(self, table: Table, key: str):
        """
        Initialize the SCD handler base class.
        
        Args:
            table: SQLAlchemy Table object.
            key: Primary key column name (e.g., "emp_id").
        """
        self.table = table
        self.key = key

    @abstractmethod
    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """Create new records."""
        pass

    @abstractmethod
    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """Read records matching the criteria in data."""
        pass

    @abstractmethod
    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """Update existing records."""
        pass

    @abstractmethod
    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """Delete records."""
        pass

    def _get_key_value(self, data: Dict[str, Any]) -> Any:
        """Extract the key value from a single data dict."""
        return data.get(self.key)