from typing import Dict, Any, List
from sqlalchemy import Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from dbadminkit.core.table_interface import TableInterface
from app_test.dbadminkit.core.crud_types import CRUDOperation
from dbadminkit.core.crud_base import CRUDBase
from app_test.dbadminkit.core.scd_handler import SCDTableHandler
import pandas as pd

class SCDType1Handler(SCDTableHandler):
    def __init__(self, table: str, key: str, spark: SparkSession):
        self.table = table  # Table name as string for PySpark
        self.key = key
        self.spark = spark

    def create(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Bulk create with PySpark."""
        if not data:
            return []
        df = self.spark.createDataFrame(data)
        df.write.mode("append").saveAsTable(self.table)
        return data

    def read(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Read with PySpark."""
        df = self.spark.table(self.table)
        if data:
            filters = data[0]
            for k, v in filters.items():
                df = df.filter(col(k) == v)
        return [row.asDict() for row in df.collect()]

    def update(self, data: List[Dict[str, Any]], session: Session) -> List[Dict[str, Any]]:
        """SCD Type 1: Bulk update with PySpark (overwrite)."""
        if not data:
            return []
        update_df = self.spark.createDataFrame(data)
        existing_df = self.spark.table(self.table)
        # Merge (upsert) logic
        merged_df = existing_df.alias("existing").join(
            update_df.alias("updates"),
            col("existing." + self.key) == col("updates." + self.key),
            "left_outer"
        ).select(
            *[col("updates." + c).alias(c) if c in update_df.columns else col("existing." + c)
              for c in existing_df.columns]
        )
        merged_df.write.mode("overwrite").saveAsTable(self.table)
        return data

    def delete(self, data: List[Dict[str, Any]], session: Session) -> None:
        """SCD Type 1: Bulk delete with PySpark."""
        if not data:
            return
        delete_keys = [row[self.key] for row in data]
        df = self.spark.table(self.table).filter(~col(self.key).isin(delete_keys))
        df.write.mode("overwrite").saveAsTable(self.table)