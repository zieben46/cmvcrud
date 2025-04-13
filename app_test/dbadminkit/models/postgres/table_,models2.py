import logging
import pandas as pd
import pyodbc
from typing import Dict, Any, List, Union
from sqlalchemy import create_engine, Table, MetaData, Column, BigInteger, String, Boolean, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy import inspect
from sqlalchemy.types import VARCHAR, BIGINT, TIMESTAMP, BOOLEAN
from dbadminkit.core.table_interface import TableInterface
from app_test.dbadminkit.core.crud_types import CRUDOperation
from app_test.dbadminkit.models.databricks.scd_types import (
    SCDType0Handler,
    SCDType1Handler,
    SCDType2Handler
)
from dbadminkit.core.crud_base import CRUDBase
import math
from datetime import datetime

logger = logging.getLogger(__name__)

class DBTable(TableInterface):
    """Manages a PostgreSQL table with data ingested from Databricks."""
    def __init__(self, table_info: Union[TableInfo, Dict[str, Any]]):
        """Initialize with TableInfo object or dictionary."""
        # Convert dict to TableInfo if necessary
        if isinstance(table_info, Dict):
            self._table_info = TableInfo.from_dict(table_info)
        else:
            self._table_info = table_info

        # Initialize attributes
        self._pg_engine = None
        self._db_conn = None
        self._metadata = None
        self._table = None
        self._history_table = None
        self._scd_handler = None
        self._crud_base = None
        self._chunk_size = 50000  # For initial load
        self._cdf_chunk_size = 1000  # For daily CDF

    # Properties
    @property
    def table_name(self) -> str:
        """Name of the PostgreSQL table."""
        return self._table_info.table_name

    @property
    def key(self) -> str:
        """Primary key field."""
        return self._table_info.key

    @property
    def scd_type(self) -> str:
        """SCD type (type0, type1, type2)."""
        return self._table_info.scd_type

    @property
    def pg_engine(self) -> Engine:
        """SQLAlchemy engine for PostgreSQL."""
        if self._pg_engine is None:
            try:
                self._pg_engine = create_engine(self._table_info.postgres_url, pool_size=5)
            except Exception as e:
                logger.error(f"Failed to create PostgreSQL engine: {e}")
                raise
        return self._pg_engine

    @property
    def db_conn(self) -> pyodbc.Connection:
        """Databricks SQL connection."""
        if self._db_conn is None:
            try:
                self._db_conn = pyodbc.connect(self._table_info.databricks_conn_str)
            except Exception as e:
                logger.error(f"Failed to connect to Databricks SQL: {e}")
                raise
        return self._db_conn

    @property
    def table(self) -> Table:
        """SQLAlchemy Table object."""
        if self._table is None:
            try:
                self._table = Table(self.table_name, self.metadata, autoload_with=self.pg_engine)
            except Exception as e:
                logger.error(f"Table {self.table_name} not found: {e}")
                raise ValueError(f"Table {self.table_name} must exist or be created")
        return self._table

    @property
    def metadata(self) -> MetaData:
        """SQLAlchemy MetaData object."""
        if self._metadata is None:
            self._metadata = MetaData()
        return self._metadata

    @property
    def history_table(self) -> Table:
        """Delta history table for auditing."""
        if self._history_table is None:
            self._history_table = Table(
                "delta_history",
                self.metadata,
                Column("version", BigInteger, primary_key=True),
                Column("timestamp", DateTime),
                Column("operation", String),
                Column("record_count", BigInteger),
                Column("ingested_at", DateTime, server_default="CURRENT_TIMESTAMP")
            )
            try:
                self.metadata.create_all(self.pg_engine)
            except Exception as e:
                logger.error(f"Failed to create delta_history table: {e}")
                raise
        return self._history_table

    @property
    def scd_handler(self):
        """SCD handler for CRUD operations."""
        if self._scd_handler is None:
            handlers = {
                "type0": SCDType0Handler,
                "type1": SCDType1Handler,
                "type2": SCDType2Handler
            }
            handler_class = handlers.get(self.scd_type)
            if not handler_class:
                raise ValueError(f"Unsupported SCD type: {self.scd_type}")
            self._scd_handler = handler_class(self.table, self.key)
        return self._scd_handler

    @property
    def crud_base(self) -> CRUDBase:
        """CRUD base for processing operations."""
        if self._crud_base is None:
            self._crud_base = CRUDBase(self.table, self.pg_engine, self.key, self.scd_handler)
        return self._crud_base

    def get_scdtype(self) -> str:
        """Return the SCD type."""
        return self.scd_type

    def create_table(self, schema: Dict[str, str], drop_if_exists: bool = False):
        """Create a new PostgreSQL table from a schema dictionary."""
        if drop_if_exists:
            with self.pg_engine.begin() as conn:
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                except Exception as e:
                    logger.error(f"Failed to drop table {self.table_name}: {e}")
                    raise

        # Map string types to SQLAlchemy types
        type_map = {
            "VARCHAR": VARCHAR,
            "BIGINT": BIGINT,
            "TIMESTAMP": TIMESTAMP,
            "BOOLEAN": BOOLEAN,
            "STRING": String,
            "INTEGER": Integer
        }

        columns = []
        for col_name, col_type in schema.items():
            col_kwargs = {}
            if "PRIMARY KEY" in col_type.upper():
                col_type = col_type.replace(" PRIMARY KEY", "")
                col_kwargs["primary_key"] = True
            sa_type = type_map.get(col_type.upper(), VARCHAR)
            columns.append(Column(col_name, sa_type, **col_kwargs))

        # Add SCD Type 2 columns if needed
        if self.scd_type == "type2":
            columns.extend([
                Column("on_date", TIMESTAMP),
                Column("off_date", TIMESTAMP),
                Column("is_active", BOOLEAN, default=True)
            ])

        # Create table
        try:
            self._table = Table(self.table_name, self.metadata, *columns)
            self.metadata.create_all(self.pg_engine)
            self._scd_handler = None  # Reset to force re-initialization
            self._crud_base = None
            logger.info(f"Created table {self.table_name} with schema {schema}")
        except Exception as e:
            logger.error(f"Failed to create table {self.table_name}: {e}")
            raise

    def perform_crud(self, crud_type: CRUDOperation, data: Dict[str, Any]) -> Any:
        """Perform a single CRUD operation."""
        try:
            with self.pg_engine.begin() as session:
                if crud_type == CRUDOperation.CREATE:
                    return self.scd_handler.create([data], session)[0]
                elif crud_type == CRUDOperation.READ:
                    result = self.scd_handler.read([data], session)
                    return result[0] if result else None
                elif crud_type == CRUDOperation.UPDATE:
                    return self.scd_handler.update([data], session)[0]
                elif crud_type == CRUDOperation.DELETE:
                    self.scd_handler.delete([data], session)
                    return None
        except Exception as e:
            logger.error(f"CRUD operation {crud_type} failed: {e}")
            raise

    def process_cdc_logs(self, cdc_records: List[Dict[str, Any]]) -> int:
        """Process CDC records from Databricks CDF."""
        try:
            return self.crud_base.process_cdc_logs(cdc_records)
        except Exception as e:
            logger.error(f"Failed to process CDC logs: {e}")
            raise

    def process_dataframe_edits(self, original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
        """Process DataFrame edits."""
        try:
            return self.crud_base.process_dataframe_edits(original_df, edited_df)
        except Exception as e:
            logger.error(f"Failed to process DataFrame edits: {e}")
            raise

    def get_db_table_schema(self) -> Dict[str, Any]:
        """Get the schema of the PostgreSQL table."""
        try:
            inspector = inspect(self.pg_engine)
            columns = inspector.get_columns(self.table_name)
            return {col["name"]: str(col["type"]) for col in columns}
        except Exception as e:
            logger.error(f"Failed to get schema for {self.table_name}: {e}")
            raise

    def read_databricks_chunk(self, query: str, params: Dict = None) -> iter:
        """Stream chunks from Databricks SQL."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, params or {})
            while True:
                chunk_size = self._cdf_chunk_size if "table_changes" in query else self._chunk_size
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                chunk = pd.DataFrame([dict(zip([col[0] for col in cursor.description], row)) for row in rows])
                yield chunk
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to read Databricks chunk: {e}")
            raise

    def transfer_initial_load(self, databricks_table: str, total_records: int = 20000000):
        """Transfer full table from Databricks to PostgreSQL."""
        query = f"SELECT * FROM {databricks_table} ORDER BY {self.key}"
        total_chunks = math.ceil(total_records / self._chunk_size)

        try:
            for i, chunk_df in enumerate(self.read_databricks_chunk(query)):
                if chunk_df.empty:
                    break
                cdc_records = [
                    {"operation": "INSERT", "data": row, "timestamp": datetime.utcnow()}
                    for _, row in chunk_df.iterrows()
                ]
                processed = self.process_cdc_logs(cdc_records)

                with self.pg_engine.begin() as conn:
                    conn.execute(
                        self.history_table.insert().values(
                            version=0,
                            timestamp=datetime.utcnow(),
                            operation="INITIAL_LOAD",
                            record_count=processed
                        )
                    )
                logger.info(f"Processed chunk {i+1}/{total_chunks}: {processed} records")
            logger.info(f"Initial load of {total_records} records completed")
        except Exception as e:
            logger.error(f"Initial load failed: {e}")
            raise

    def transfer_cdf_changes(self, databricks_table: str, start_version: int):
        """Process CDF changes from Databricks."""
        query = f"""
        SELECT *, _change_type, _commit_version AS version
        FROM table_changes('{databricks_table}', {start_version})
        WHERE _change_type IN ('insert', 'update_postimage', 'delete')
        """
        try:
            for chunk_df in self.read_databricks_chunk(query):
                if chunk_df.empty:
                    break
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
                processed = self.process_cdc_logs(cdc_records)

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
                logger.info(f"Processed CDF chunk: {processed} records")
            logger.info(f"CDF changes from version {start_version} processed")
        except Exception as e:
            logger.error(f"CDF transfer failed: {e}")
            raise

    def sync_history(self, databricks_table: str):
        """Sync DESCRIBE HISTORY to PostgreSQL."""
        query = f"DESCRIBE HISTORY {databricks_table}"
        try:
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
                self.crud_base.process_cdc_logs(cdc_records)
            logger.info("History synced")
        except Exception as e:
            logger.error(f"History sync failed: {e}")
            raise