# datastorekit/adapters/spark_adapter.py
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, BooleanType
from delta.tables import DeltaTable
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DatastoreOperationError
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class SparkAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.spark = self.connection.get_spark_session()

    def get_reflected_keys(self, table_name: str) -> List[str]:
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            schema = delta_table.toDF().schema
            return ["unique_id", "secondary_key"]  # Assume composite key
        except Exception as e:
            logger.error(f"Failed to get reflected keys for table {table_name}: {e}")
            return []

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        """Create a Delta table with the given schema."""
        try:
            type_map = {
                "Integer": IntegerType(),
                "String": StringType(),
                "Float": FloatType(),
                "DateTime": TimestampType(),
                "Boolean": BooleanType()
            }
            fields = [
                StructField(col_name, type_map.get(col_type, StringType()), nullable=True)
                for col_name, col_type in schema.items()
            ]
            spark_schema = StructType(fields)
            df = self.spark.createDataFrame([], spark_schema)
            df.write.mode("overwrite").saveAsTable(f"{schema_name or 'default'}.{table_name}")
            logger.info(f"Created Delta table {schema_name or 'default'}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create table {table_name}: {e}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        """Get column names and types for a Delta table."""
        try:
            delta_table = DeltaTable.forName(self.spark, f"{schema_name or 'default'}.{table_name}")
            schema = delta_table.toDF().schema
            type_map = {
                IntegerType: "INTEGER",
                StringType: "STRING",
                FloatType: "FLOAT",
                TimestampType: "TIMESTAMP",
                BooleanType: "BOOLEAN"
            }
            return {field.name: type_map.get(type(field.dataType), "STRING") for field in schema.fields}
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return {}

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

    def select_chunks(self, table_name: str, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        df = self.spark.table(table_name)
        if filters:
            for key, value in filters.items():
                df = df.filter(df[key] == value)
        # Simulate chunking by repartitioning
        df = df.repartition(chunk_size)
        for partition in df.rdd.mapPartitions(lambda iter: [list(iter)]).collect():
            yield [row.asDict() for row in partition]

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

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            df = self.spark.sql(sql, parameters or {})
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            raise DatastoreOperationError(f"Error during execute_sql: {e}")

    def list_tables(self, schema: str) -> List[str]:
        try:
            return [table.tableName for table in self.spark.catalog.listTables(schema)]
        except Exception as e:
            logger.error(f"Failed to list tables for schema {schema}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        metadata = {}
        for table_name in self.list_tables(schema):
            columns = self.get_table_columns(table_name, schema)
            metadata[table_name] = {
                "columns": columns,
                "primary_keys": ["unique_id", "secondary_key"]
            }
        return metadata