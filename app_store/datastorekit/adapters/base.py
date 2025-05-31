# datastorekit/adapters/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Iterator, Tuple

class DatastoreAdapter(ABC):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.table_info = None

    @abstractmethod
    def create(self, table_name: str, records: List[Dict]) -> int:
        """Insert records into the specified table.
        
        Args:
            table_name: Name of the table to insert into.
            records: List of dictionaries containing the data to insert.
        
        Returns:
            Number of rows inserted.
        """
        pass

    @abstractmethod
    def read(self, table_name: str, filters: Optional[Dict] = None) -> List[Dict]:
        """Retrieve records from the specified table with optional filters.
        
        Args:
            table_name: Name of the table to query.
            filters: Optional dictionary of column-value pairs to filter results.
        
        Returns:
            List of dictionaries containing the matching records.
        """
        pass

    @abstractmethod
    def update(self, table_name: str, updates: List[Dict]) -> int:
        """Update records in the specified table based on primary keys.
        
        Args:
            table_name: Name of the table to update.
            updates: List of dictionaries containing primary key(s) and fields to update.
        
        Returns:
            Number of rows updated.
        """
        pass

    @abstractmethod
    def delete(self, table_name: str, conditions: List[Dict]) -> int:
        """Delete records from the specified table based on conditions.
        
        Args:
            table_name: Name of the table to delete from.
            conditions: List of dictionaries with column names and values to match.
        
        Returns:
            Number of rows deleted.
        """
        pass

    @abstractmethod
    def select_chunks(self, table_name: str, filters: Optional[Dict] = None, chunk_size: int = 100000) -> Iterator[List[Dict]]:
        """Retrieve records in chunks from the specified table with optional filters.
        
        Args:
            table_name: Name of the table to query.
            filters: Optional dictionary of column-value pairs to filter results.
            chunk_size: Number of records per chunk.
        
        Returns:
            Iterator yielding lists of dictionaries containing the matching records.
        """
        pass

    @abstractmethod
    def apply_changes(self, table_name: str, changes: List[Dict]) -> Tuple[List[Any], int, int, int]:
        """Apply create, update, and delete operations in one transaction.
        
        Args:
            table_name: Name of the table to apply changes to.
            changes: List of dictionaries, each with an 'operation' key ('create', 'update', 'delete')
                     and data for the operation.
        
        Returns:
            Tuple of (empty list, number of rows inserted, number of rows updated, number of rows deleted).
            The first element is an empty list, as primary key values are not retrieved.
        """
        pass

    @abstractmethod
    def get_reflected_keys(self, table_name: str) -> List[str]:
        """Get the primary key column names for the specified table.
        
        Args:
            table_name: Name of the table.
        
        Returns:
            List of primary key column names.
        """
        pass

    @abstractmethod
    def execute_sql(self, sql: str, parameters: Optional[Dict] = None) -> List[Dict]:
        """Execute a raw SQL query with optional parameters.
        
        Args:
            sql: SQL query string to execute.
            parameters: Optional dictionary of parameters for the query.
        
        Returns:
            List of dictionaries containing the query results.
        """
        pass

    @abstractmethod
    def list_tables(self, schema: str) -> List[str]:
        """List all tables in the specified schema.
        
        Args:
            schema: Name of the schema.
        
        Returns:
            List of table names.
        """
        pass

    @abstractmethod
    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in the specified schema.
        
        Args:
            schema: Name of the schema.
        
        Returns:
            Dictionary mapping table names to their metadata (columns and primary keys).
        """
        pass

    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        """Create a table with the given schema.
        
        Args:
            table_name: Name of the table to create.
            schema: Dictionary defining the column names and their types.
            schema_name: Optional schema name for the table.
        """
        pass

    @abstractmethod
    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        """Get column names and types for the specified table.
        
        Args:
            table_name: Name of the table.
            schema_name: Optional schema name for the table.
        
        Returns:
            Dictionary mapping column names to their types.
        """
        pass