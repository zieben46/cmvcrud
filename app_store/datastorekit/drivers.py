# datastorekit/drivers.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from sqlalchemy import create_engine, Table, MetaData, Column
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator
import logging

logger = logging.getLogger(__name__)

class DatabaseDriver(ABC):
    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, Any]):
        pass

    @abstractmethod
    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def update(self, table_info: Dict[str, str], data: List[Dict[str, Any]], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def sync_to(self, source_table_info: Dict[str, str], target_driver: 'DatabaseDriver', target_table: str, method: str):
        pass

class PostgresDriver(DatabaseDriver):
    def __init__(self, orchestrator: DataStoreOrchestrator, datastore_key: str):
        self.orchestrator = orchestrator
        self.datastore_key = datastore_key
        self.engine = orchestrator.adapters[datastore_key].engine
        self.metadata = MetaData()

    def create_table(self, table_name: str, schema: Dict[str, Any]):
        """Create a table with the given schema."""
        columns = []
        for col_name, col_type in schema.items():
            if col_name == "id":
                columns.append(Column(col_name, col_type, primary_key=True))
            else:
                columns.append(Column(col_name, col_type))
        schema_name = self.orchestrator.adapters[self.datastore_key].profile.schema
        Table(table_name, self.metadata, *columns, schema=schema_name)
        self.metadata.create_all(self.engine)
        logger.info(f"Created table {schema_name}.{table_name} in PostgreSQL")

    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        """Insert records into the table."""
        table = self.orchestrator.get_table(self.datastore_key, table_info)
        table.create(data)

    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Read records from the table."""
        table = self.orchestrator.get_table(self.datastore_key, table_info)
        return table.read(filters)

    def update(self, table_info: Dict[str, str], data: List[Dict[str, Any]], filters: Dict[str, Any]):
        """Update records in the table."""
        table = self.orchestrator.get_table(self.datastore_key, table_info)
        table.update(data, filters)

    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        """Delete records from the table."""
        table = self.orchestrator.get_table(self.datastore_key, table_info)
        table.delete(filters)

    def sync_to(self, source_table_info: Dict[str, str], target_driver: 'DatabaseDriver', target_table: str, method: str):
        """Sync data to the target table."""
        if method != "full_load":
            raise ValueError(f"Unsupported sync method: {method}")
        replicator = DatabricksToPostgresReplicator(self.orchestrator)
        replicator.replicate(
            source_table=source_table_info["table_name"],
            target_table=target_table,
            history_table=f"{source_table_info['table_name']}_history",
            max_changes=100  # Small threshold for testing
        )