# datastorekit/adapters/spark_adapter.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from datastorekit.adapters.base import DatastoreAdapter
from typing import List, Dict, Any, Iterator, Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class SparkAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        self.db_engine = DBEngine(profile)
        self.spark = self.db_engine.get_spark()
        self.catalog = profile.catalog
        self.schema = profile.schema

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys are present in the table schema."""
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            columns = set(df.columns)
            if not all(key in columns for key in table_info_keys):
                raise ValueError(
                    f"Table {table_name} columns {columns} do not include all TableInfo keys {table_info_keys}"
                )
        except Exception as e:
            logger.warning(f"Failed to validate keys for table {table_name}: {e}")
            return  # Assume TableInfo.keys if table not found

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        """Insert records into a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        try:
            df = self.spark.createDataFrame(data)
            df.write.format("delta").mode("append").saveAsTable(f"{self.catalog}.{self.schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to insert into {table_name}: {e}")
            raise

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Select records from a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            for key, value in filters.items():
                df = df.filter(col(key) == value)
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            logger.error(f"Failed to select from {table_name}: {e}")
            return []

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            for key, value in filters.items():
                df = df.filter(col(key) == value)
            # Repartition based on chunk size
            num_partitions = max(1, df.count() // chunk_size + 1)
            df = df.repartition(num_partitions)
            rdd = df.rdd.map(lambda row: row.asDict())
            for partition in rdd.glom().collect():
                yield partition
        except Exception as e:
            logger.error(f"Failed to select chunks from {table_name}: {e}")
            return

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        """Update records in a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            for key, value in filters.items():
                df = df.filter(col(key) == value)
            update_df = self.spark.createDataFrame(data)
            delta_table = DeltaTable.forName(self.spark, f"{self.catalog}.{self.schema}.{table_name}")
            condition = " AND ".join([f"target.{key} = source.{key}" for key in filters.keys()])
            delta_table.alias("target").merge(
                update_df.alias("source"),
                condition
            ).whenMatchedUpdateAll().execute()
        except Exception as e:
            logger.error(f"Failed to update {table_name}: {e}")
            raise

    def delete(self, table_name: str, filters: Dict[str, Any]):
        """Delete records from a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            for key, value in filters.items():
                df = df.filter(col(key) == value)
            delta_table = DeltaTable.forName(self.spark, f"{self.catalog}.{self.schema}.{table_name}")
            condition = " AND ".join([f"target.{key} = {value}" for key, value in filters.items()])
            delta_table.delete(condition)
        except Exception as e:
            logger.error(f"Failed to delete from {table_name}: {e}")
            raise

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return results."""
        try:
            # Spark SQL doesn't support bind parameters directly; substitute manually if provided
            if parameters:
                formatted_sql = sql
                for key, value in parameters.items():
                    formatted_sql = formatted_sql.replace(f":{key}", str(value))
                sql = formatted_sql
            df = self.spark.sql(sql)
            if df.rdd.isEmpty():
                return []
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            raise