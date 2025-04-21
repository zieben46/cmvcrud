# datastorekit/adapters/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Iterator, Optional

class DatastoreAdapter(ABC):
    @abstractmethod
    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        """Select records in chunks for large datasets."""
        pass

    @abstractmethod
    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def delete(self, table_name: str, filters: Dict[str, Any]):
        pass

    @abstractmethod
    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        pass

    @abstractmethod
    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return results."""
        pass