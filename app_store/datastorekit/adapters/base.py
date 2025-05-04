# datastorekit/adapters/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class DatastoreAdapter(ABC):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.table_info = None

    @abstractmethod
    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]]):
        pass

    @abstractmethod
    def delete(self, table_name: str, filters: Optional[Dict[str, Any]]):
        pass

    @abstractmethod
    def select(self, table_name: str, filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def select_chunks(self, table_name: str, filters: Optional[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
        pass

    @abstractmethod
    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def get_reflected_keys(self, table_name: str) -> List[str]:
        pass

    @abstractmethod
    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def list_tables(self, schema: str) -> List[str]:
        pass

    @abstractmethod
    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        pass

    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        pass

    @abstractmethod
    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        pass