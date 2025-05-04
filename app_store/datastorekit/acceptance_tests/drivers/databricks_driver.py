# datastorekit/drivers/databricks_driver.py
from typing import Dict, Any, List
from sqlalchemy import Table, MetaData, Column, text
from .database_driver import DatabaseDriver
from .sqlalchemy_core_driver import SQLAlchemyCoreDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
import logging

logger = logging.getLogger(__name__)

class DatabricksDriver(SQLAlchemyCoreDriver):
    def __init__(self, orchestrator: DataStoreOrchestrator, datastore_key: str):
        super().__init__(orchestrator, datastore_key)

    def sync_to(self, source_table_info: Dict[str, str], target_driver: 'DatabaseDriver', target_table: str, method: str):
        if method not in ["full_load", "incremental"]:
            raise ValueError(f"Unsupported sync method: {method}")
        source_table = source_table_info["table_name"]
        source_schema = self.orchestrator.adapters[self.datastore_key].profile.schema or "default"
        target_datastore_key = target_driver.datastore_key
        target_db, target_schema = target_datastore_key.split(":")
        self.orchestrator.sync_tables(
            source_db=self.orchestrator.adapters[self.datastore_key].profile.dbname,
            source_schema=source_schema,
            source_table_name=source_table,
            target_db=target_db,
            target_schema=target_schema,
            target_table_name=target_table,
            filters=None,
            chunk_size=1000 if method == "full_load" else None
        )
        logger.info(f"Synchronized {source_schema}.{source_table} to {target_schema}.{target_table} using {method}")