import pandas as pd
from pyspark.sql import SparkSession

from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from dbadminkit.core.table_interface import TableInterface
from app_test.dbadminkit.core.crud_types import CRUDOperation
from app_test.dbadminkit.models.databricks.scd_types import SCDType1Handler
from dbadminkit.core.crud_base import CRUDBase

class DBTable(TableInterface):
    def __init__(self, engine: Engine, spark: SparkSession, table_info: Dict[str, Any]):
        self.engine = engine  # Keep for schema reflection if needed
        self.spark = spark
        self.table_info = table_info
        self.table_name = table_info["table_name"]
        self.key = table_info.get("key", "id")
        self.scd_type = table_info.get("scd_type", "type1")
        self.scd_handler = self._get_scd_handler()
        self.crud_base = CRUDBase(self.table_name, self.engine, self.key, self.scd_handler)

    def get_scdtype(self) -> str:
        return self.scd_type

    def _get_scd_handler(self):
        if self.scd_type == "type1":  # Only Type 1 for now
            return SCDType1Handler(self.table_name, self.key, self.spark)
        else:
            raise NotImplementedError("Only SCD Type 1 implemented for Databricks with PySpark")

    def perform_crud(self, crud_type: CRUDOperation, data: Dict[str, Any]) -> Any:
        with self.engine.begin() as session:  # Still using session for compatibility
            if crud_type == CRUDOperation.CREATE:
                return self.scd_handler.create([data], session)[0]
            elif crud_type == CRUDOperation.READ:
                return self.scd_handler.read([data], session)
            elif crud_type == CRUDOperation.UPDATE:
                return self.scd_handler.update([data], session)[0]
            elif crud_type == CRUDOperation.DELETE:
                self.scd_handler.delete([data], session)
                return None

    def process_cdc_logs(self, cdc_records: List[Dict[str, Any]]) -> int:
        return self.crud_base.process_cdc_logs(cdc_records)

    def process_dataframe_edits(self, original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
        return self.crud_base.process_dataframe_edits(original_df, edited_df)

    def get_db_table_schema(self) -> Dict[str, Any]:
        inspector = inspect(self.engine)
        columns = inspector.get_columns(self.table_name)
        return {col["name"]: str(col["type"]) for col in columns}