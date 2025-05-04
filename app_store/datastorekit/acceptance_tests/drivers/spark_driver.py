from typing import Dict, Any, List
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from ..drivers.database_driver import DatabaseDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
import logging

logger = logging.getLogger(__name__)

class SparkDriver(DatabaseDriver):
    def __init__(self, orchestrator: DataStoreOrchestrator, datastore_key: str):
        self.orchestrator = orchestrator
        self.datastore_key = datastore_key
        self.spark = orchestrator.adapters[datastore_key].spark
        self.schema_name = orchestrator.adapters[datastore_key].profile.schema

    def create_table(self, table_name: str, schema: Dict[str, Any]):
        """Create a Delta table with the given schema."""
        spark_schema = StructType([
            StructField("unique_id", IntegerType(), False),
            StructField("category", StringType(), True),
            StructField("amount", FloatType(), True)
        ])
        empty_df = self.spark.createDataFrame([], spark_schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(f"{self.schema_name}.{table_name}")
        logger.info(f"Created Delta table {self.schema_name}.{table_name}")

    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        """Insert records into the table."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        table.create(data)

    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Read records from the table."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        return table.read(filters)

    def update(self, table_info: Dict[str, str], data: List[Dict[str, Any]], filters: Dict[str, Any]):
        """Update records in the table."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        table.update(data, filters)

    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        """Delete records from the table."""
        table = self.orchestrator.get_table(self.datastore_key, TableInfo(**table_info))
        table.delete(filters)

    def sync_to(self, source_table_info: Dict[str, str], target_driver: 'DatabaseDriver', target_table: str, method: str):
        if method != "full_load":
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
            chunk_size=1000
        )
        logger.info(f"Synchronized {source_schema}.{source_table} to {target_schema}.{target_table} using {method}")