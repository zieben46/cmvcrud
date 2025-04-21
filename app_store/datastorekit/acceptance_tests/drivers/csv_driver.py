from typing import Dict, Any, List
import pandas as pd
import os
from ..drivers.database_driver import DatabaseDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
import logging

logger = logging.getLogger(__name__)

class CSVDriver(DatabaseDriver):
    def __init__(self, orchestrator: DataStoreOrchestrator, datastore_key: str):
        self.orchestrator = orchestrator
        self.datastore_key = datastore_key
        self.base_dir = orchestrator.adapters[datastore_key].base_dir

    def create_table(self, table_name: str, schema: Dict[str, Any]):
        """Create a CSV file with the given schema."""
        file_path = os.path.join(self.base_dir, f"{table_name}.csv")
        df = pd.DataFrame(columns=schema.keys())
        df.to_csv(file_path, index=False)
        logger.info(f"Created CSV file {file_path}")

    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        """Insert records into the CSV."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        table.create(data)

    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Read records from the CSV."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        return table.read(filters)

    def update(self, table_info: Dict[str, str], data: List[Dict[str, Any]], filters: Dict[str, Any]):
        """Update records in the CSV."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        table.update(data, filters)

    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        """Delete records from the CSV."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        table.delete(filters)

    def sync_to(self, source_table_info: Dict[str, str], target_driver: 'DatabaseDriver', target_table: str, method: str):
        """Sync data to the target table."""
        if method != "full_load":
            raise ValueError(f"Unsupported sync method: {method}")
        source_table = source_table_info["table_name"]
        source_schema = self.orchestrator.adapters[self.datastore_key].profile.schema
        target_datastore_key = target_driver.datastore_key
        target_db, target_schema = target_datastore_key.split(":")
        self.orchestrator.replicate(
            source_db=self.orchestrator.adapters[self.datastore_key].profile.dbname,
            source_schema=source_schema,
            source_table=source_table,
            target_db=target_db,
            target_schema=target_schema,
            target_table=target_table
        )