from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from typing import List, Dict, Any, Iterator, Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class SparkAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.spark = self.connection.get_spark()
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
            return

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        """Insert records into a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(","))
        try:
            df = self.spark.createDataFrame(data)
            df.write.format("delta").mode("append").saveAsTable(f"{self.catalog}.{self.schema}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to insert into {table_name}: {e}")
            raise

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Select records from a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(","))
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            for key, value in filters.items():
                df = df.filter(col(key) == value)
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            logger.error(f"Failed to select from {table_name}: {e}")
            return []

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        """Select records in chunks from a Delta table."""
        self.validate_keys(table_name, self.table_info.keys.split(","))
        try:
            df = self.spark.table(f"{self.catalog}.{self.schema}.{table_name}")
            for key, value in filters.items():
                df = df.filter(col(key) == value)
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
        self.validate_keys(table_name, self.table_info.keys.split(","))
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
        self.validate_keys(table_name, self.table_info.keys.split(","))
        try:
            delta_table = DeltaTable.forName(self.spark, f"{self.catalog}.{self.schema}.{table_name}")
            condition = " AND ".join([f"target.{key} = {value}" for key, value in filters.items()])
            delta_table.delete(condition)
        except Exception as e:
            logger.error(f"Failed to delete from {table_name}: {e}")
            raise

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return results."""
        try:
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

    def list_tables(self, schema: str) -> List[str]:
        """List tables in the given schema."""
        try:
            df = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{schema}")
            return [row["tableName"] for row in df.collect()]
        except Exception as e:
            logger.error(f"Failed to list tables for schema {schema}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in the schema."""
        try:
            metadata = {}
            for table_name in self.list_tables(schema):
                df = self.spark.sql(f"DESCRIBE TABLE {self.catalog}.{schema}.{table_name}")
                columns = {row["col_name"]: row["data_type"] for row in df.collect() if row["col_name"]}
                # Delta tables may not have explicit primary keys; use table_info if available
                pk_columns = self.table_info.keys.split(",") if self.table_info else []
                metadata[table_name] = {
                    "columns": columns,
                    "primary_keys": pk_columns
                }
            return metadata
        except Exception as e:
            logger.error(f"Failed to get table metadata for schema {schema}: {e}")
            return {}
        
    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        key_columns = self.profile.keys.split(",")  # e.g., ["key1", "key2"]
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Batch inserts
            if inserts:
                insert_df = self.spark.createDataFrame(inserts)
                insert_df.write.mode("append").saveAsTable(table_name)
                logger.debug(f"Applied {len(inserts)} inserts to {table_name}")

            # Batch updates (using merge)
            if updates:
                update_df = self.spark.createDataFrame(updates)
                merge_condition = " AND ".join(f"t.{key} = s.{key}" for key in key_columns)
                delta_table.alias("t").merge(
                    update_df.alias("s"),
                    merge_condition
                ).whenMatchedUpdateAll().execute()
                logger.debug(f"Applied {len(updates)} updates to {table_name}")

            # Batch deletes
            if deletes:
                delete_conditions = [
                    " AND ".join(f"{key} = {repr(d[key])}" for key in key_columns) for d in deletes
                ]
                if delete_conditions:
                    delta_table.delete(" OR ".join(f"({cond})" for cond in delete_conditions))
                logger.debug(f"Applied {len(deletes)} deletes to {table_name}")

        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            if "duplicate key" in str(e).lower():
                raise DuplicateKeyError(f"Duplicate key error during apply_changes on {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")