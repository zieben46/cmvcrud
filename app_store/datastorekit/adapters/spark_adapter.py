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

    def get_reflected_keys(self, table_name: str) -> List[str]:
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            schema = delta_table.toDF().schema
            return ["unique_id"]  # Placeholder; customize based on Delta table metadata
        except Exception as e:
            logger.error(f"Failed to get reflected keys for table {table_name}: {e}")
            return []

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
        try:
            df = self.spark.createDataFrame(data)
            df.write.mode("append").saveAsTable(table_name)
        except Exception as e:
            logger.error(f"Insert failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during insert on {table_name}: {e}")

    def select(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        df = self.spark.table(table_name)
        if filters:
            for key, value in filters.items():
                df = df.filter(df[key] == value)
        return [row.asDict() for row in df.collect()]

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

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None):
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            for update_data in data:
                update_df = self.spark.createDataFrame([update_data])
                condition = " AND ".join(f"t.{k} = s.{k}" for k in (filters.keys() if filters else update_data.keys()))
                delta_table.alias("t").merge(
                    update_df.alias("s"),
                    condition
                ).whenMatchedUpdateAll().execute()
        except Exception as e:
            logger.error(f"Update failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def delete(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            if filters:
                condition = " AND ".join(f"{k} = {repr(v)}" for k, v in filters.items())
                delta_table.delete(condition)
            else:
                delta_table.delete()
        except Exception as e:
            logger.error(f"Delete failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

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
        key_columns = self.get_reflected_keys(table_name)
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            if inserts:
                insert_df = self.spark.createDataFrame(inserts)
                insert_df.write.mode("append").saveAsTable(table_name)
                logger.debug(f"Applied {len(inserts)} inserts to {table_name}")
            if updates:
                update_df = self.spark.createDataFrame(updates)
                merge_condition = " AND ".join(f"t.{key} = s.{key}" for key in key_columns)
                delta_table.alias("t").merge(
                    update_df.alias("s"),
                    merge_condition
                ).whenMatchedUpdateAll().execute()
                logger.debug(f"Applied {len(updates)} updates to {table_name}")
            if deletes:
                delete_conditions = [
                    " AND ".join(f"{key} = {repr(d[key])}" for key in key_columns if key in d) for d in deletes
                ]
                if delete_conditions:
                    delta_table.delete(" OR ".join(f"({cond})" for cond in delete_conditions))
                logger.debug(f"Applied {len(deletes)} deletes to {table_name}")
        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")