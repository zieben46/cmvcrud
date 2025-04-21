from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterator, Optional
from datastorekit.profile import DatabaseProfile

class DatastoreAdapter(ABC):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.table_info = None  # Will be set by orchestrator

    @abstractmethod
    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys match the table's schema."""
        pass

    @abstractmethod
    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        """Insert records into a table."""
        pass

    @abstractmethod
    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Select records from a table."""
        pass

    @abstractmethod
    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
        """Select records in chunks."""
        pass

    @abstractmethod
    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        """Update records in a table."""
        pass

    @abstractmethod
    def delete(self, table_name: str, filters: Dict[str, Any]):
        """Delete records from a table."""
        pass

    @abstractmethod
    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw query."""
        pass

    @abstractmethod
    def list_tables(self, schema: str) -> List[str]:
        """List tables in the given schema."""
        pass

    @abstractmethod
    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in the schema."""
        pass