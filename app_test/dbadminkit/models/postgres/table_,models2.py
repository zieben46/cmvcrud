import logging
import pandas as pd
import pyodbc
from typing import Dict, Any, List
from sqlalchemy import create_engine, Table, MetaData, Column, BigInteger, String, Boolean, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from dbadminkit.core.table_interface import TableInterface
from app_test.dbadminkit.core.crud_types import CRUDOperation
from app_test.dbadminkit.models.databricks.scd_types import SCDType2Handler  # Use your SCDType2Handler
from dbadminkit.core.crud_base import CRUDBase
from sqlalchemy.sql import text
import math
from datetime import datetime

logger = logging.getLogger(__name__)

class DBTable(TableInterface):
    def __init__(self, postgres_engine: Engine, databricks_conn_str: str, table_info: Dict[str, Any]):
        self.pg_engine = postgres_engine
        self.db_conn_str = databricks_conn_str
        self.db_conn = pyodbc.connect(databricks_conn_str)  # Databricks SQL connection
        self.table_info = table_info
        self.table_name = table_info["table_name"]
        self.key = table_info.get("key", "user_id")
        self.scd_type = table_info.get("scd_type", "type2")
        self.metadata = MetaData()
        self.table = Table(self.table_name, self.metadata, autoload_with=self.pg_engine)
        self.history_table = Table(
            "delta_history",
            self.metadata,
            Column("version", BigInteger, primary_key=True),
            Column("timestamp", DateTime),
            Column("operation", String),
            Column("record_count", BigInteger),
            Column("ingested_at", DateTime, server_default="CURRENT_TIMESTAMP")
        )
        self.metadata.create_all(self.pg_engine)
        self.scd_handler = self._get_scd_handler()
        self.crud_base = CRUDBase(self.table, self.pg_engine, self.key, self.scd_handler)
        self.chunk_size = 50000  # For initial load
        self.cdf_chunk_size = 1000  # For CDF

    def get_scdtype(self) -> str:
        return self.scd_type

    def _get_scd_handler(self):
        if self.scd_type == "type2":
            return SCDType2Handler(self.table, self.key)
        else:
            raise NotImplementedError("Only SCD Type 2 implemented for Databricks SQL to PostgreSQL")

    def perform_crud(self, crud_type: CRUDOperation, data: Dict[str, Any]) -> Any:
        with self.pg_engine.begin() as session:
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
        from sqlalchemy import inspect
        inspector = inspect(self.pg_engine)
        columns = inspector.get_columns(self.table_name)
        return {col["name"]: str(col["type"]) for col in columns}

    def read_databricks_chunk(self, query: str, params: Dict = None) -> iter:
        """Stream chunks from Databricks SQL."""
        cursor = self.db_conn.cursor()
        cursor.execute(query, params or {})
        while True:
            rows = cursor.fetchmany(self.chunk_size if "table_changes" not in query else self.cdf_chunk_size)
            if not rows:
                break
            chunk = pd.DataFrame([dict(zip([col[0] for col in cursor.description], row)) for row in rows])
            yield chunk
        cursor.close()

    def transfer_initial_load(self, databricks_table: str, total_records: int = 20000000):
        """Transfer full table from Databricks to PostgreSQL."""
        query = f"SELECT * FROM {databricks_table} ORDER BY {self.key}"
        total_chunks = math.ceil(total_records / self.chunk_size)
        
        for i, chunk_df in enumerate(self.read_databricks_chunk(query)):
            if chunk_df.empty:
                break
            # Convert chunk to CDC-like format for CRUDBase
            cdc_records = [
                {"operation": "INSERT", "data": row, "timestamp": datetime.utcnow()}
                for _, row in chunk_df.iterrows()
            ]
            self.process_cdc_logs(cdc_records)
            
            # Log to history table
            with self.pg_engine.begin() as conn:
                conn.execute(
                    self.history_table.insert().values(
                        version=0,
                        timestamp=datetime.utcnow(),
                        operation="INITIAL_LOAD",
                        record_count=len(chunk_df)
                    )
                )
            print(f"Processed chunk {i+1}/{total_chunks}")

        print(f"Initial load of {total_records} records completed.")

    def transfer_cdf_changes(self, databricks_table: str, start_version: int):
        """Process CDF changes from Databricks."""
        query = f"""
        SELECT *, _change_type, _commit_version AS version
        FROM table_changes('{databricks_table}', {start_version})
        WHERE _change_type IN ('insert', 'update_postimage', 'delete')
        """
        for chunk_df in self.read_databricks_chunk(query):
            if chunk_df.empty:
                break
            # Map Delta CDF to CDC format
            cdc_records = []
            for _, row in chunk_df.iterrows():
                operation = {
                    "insert": "INSERT",
                    "update_postimage": "UPDATE",
                    "delete": "DELETE"
                }[row["_change_type"]]
                data = row.drop(labels=["_change_type", "version"]).to_dict()
                cdc_records.append({
                    "operation": operation,
                    "data": data,
                    "timestamp": datetime.utcnow(),
                    "version": row["version"]
                })
            # Process via CRUDBase
            processed = self.process_cdc_logs(cdc_records)
            
            # Log to history table
            if processed > 0:
                with self.pg_engine.begin() as conn:
                    conn.execute(
                        self.history_table.insert().values(
                            version=cdc_records[0]["version"],
                            timestamp=datetime.utcnow(),
                            operation="CDF_UPDATE",
                            record_count=processed
                        )
                    )
            print(f"Processed CDF chunk: {processed} records")

        print(f"CDF changes from version {start_version} processed.")

    def sync_history(self, databricks_table: str):
        """Sync DESCRIBE HISTORY to PostgreSQL."""
        query = f"DESCRIBE HISTORY {databricks_table}"
        history_df = next(self.read_databricks_chunk(query))
        if not history_df.empty:
            history_df = history_df[["version", "timestamp", "operation"]]
            history_df["record_count"] = 0
            cdc_records = [
                {
                    "operation": "INSERT",
                    "data": row.to_dict(),
                    "timestamp": datetime.utcnow()
                }
                for _, row in history_df.iterrows()
            ]
            self.crud_base.process_cdc_logs(cdc_records)  # Use CRUDBase for history
        print("History synced.")









#         from dbadminkit.core.engine import DBConfig, DBMode
# from app_test.dbadminkit.core.crud_types import CRUDOperation

# # Configure PostgreSQL
# postgres_config = DBConfig(
#     database="postgres",
#     mode=DBMode.LIVE,
#     connection_string="postgresql://user:password@host:port/dbname"
# )

# # Configure Databricks SQL
# databricks_conn_str = (
#     "Driver={Databricks};"
#     "Host={host};Port=443;HTTPPath={http_path};"
#     "AuthMech=3;UID=token;PWD={personal_access_token}"
# )

# # Initialize DBManager
# db_manager = DBManager(postgres_config)

# # Table info
# table_info = {
#     "table_name": "users",
#     "key": "user_id",
#     "scd_type": "type2"
# }

# # Get DBTable instance
# table = db_manager.get_table(table_info)

# # Override Databricks connection (since DBManager uses engine, we patch DBTable)
# table.db_conn_str = databricks_conn_str
# table.db_conn = pyodbc.connect(databricks_conn_str)

# # Initial load
# table.transfer_initial_load(databricks_table="users_delta")

# # Daily CDF updates
# latest_version = 1  # Query delta_history for MAX(version)
# table.transfer_cdf_changes(databricks_table="users_delta", start_version=latest_version)

# # Sync history
# table.sync_history(databricks_table="users_delta")