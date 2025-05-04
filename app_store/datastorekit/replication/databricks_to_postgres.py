# datastorekit/replication/databricks_to_postgres.py
import logging
import os
from typing import Dict, List, Iterator, Tuple
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from databricks.sqlalchemy import TIMESTAMP, TINYINT
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.utils import detect_changes
from datastorekit.exceptions import DatastoreOperationError
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabricksToPostgresReplicator:
    def __init__(self, orchestrator: DataStoreOrchestrator, full_load_batch_size: int = None, cdf_batch_size: int = None):
        """Initialize replicator with configurable batch sizes."""
        self.orchestrator = orchestrator
        self.source_db = "databricks_db"
        self.source_schema = "default"
        self.target_db = "spend_plan_db"
        self.target_schema = "safe_user"
        self.batch_size = int(os.getenv("FULL_LOAD_BATCH_SIZE", 100000)) if full_load_batch_size is None else full_load_batch_size
        self.cdf_batch_size = int(os.getenv("CDF_BATCH_SIZE", 1000)) if cdf_batch_size is None else cdf_batch_size

    def replicate(self, source_table: str, target_table: str, history_table: str, max_changes: int = 20_000_000):
        """
        Replicate data from a Databricks table to a PostgreSQL table using Change Data Feed and sync_tables logic.

        Args:
            source_table: Name of the source table in Databricks.
            target_table: Name of the target table in PostgreSQL.
            history_table: Name of the history table in PostgreSQL to track versions.
            max_changes: Maximum number of changes to process incrementally before falling back to full load.

        Returns:
            bool: True if replication succeeds, False otherwise.
        """
        try:
            # Define table info
            source_table_info = TableInfo(
                table_name=source_table,
                keys="unique_id,secondary_key",
                scd_type="type2",
                datastore_key=f"{self.source_db}:{self.source_schema}",
                columns={
                    "unique_id": "Integer",
                    "secondary_key": "String",
                    "category": "String",
                    "amount": "Float",
                    "start_date": "DateTime",
                    "end_date": "DateTime",
                    "is_active": "Boolean"
                }
            )
            target_table_info = TableInfo(
                table_name=target_table,
                keys="unique_id,secondary_key",
                scd_type="type2",
                datastore_key=f"{self.target_db}:{self.target_schema}",
                columns={
                    "unique_id": "Integer",
                    "secondary_key": "String",
                    "category": "String",
                    "amount": "Float",
                    "start_date": "DateTime",
                    "end_date": "DateTime",
                    "is_active": "Boolean"
                }
            )
            history_table_info = TableInfo(
                table_name=history_table,
                keys="version",
                scd_type="type0",
                datastore_key=f"{self.target_db}:{self.target_schema}",
                columns={
                    "version": "Integer",
                    "timestamp": "DateTime",
                    "operation": "String",
                    "operation_parameters": "String",
                    "num_affected_rows": "BigInteger"
                }
            )

            # Get table objects
            source_table = self.orchestrator.get_table(f"{self.source_db}:{self.source_schema}", source_table_info)
            target_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", target_table_info)
            history_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", history_table_info)

            # Check history table and get max version
            target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
            history_table_exists = history_table.table_name in target_tables
            max_version = self._get_max_history_version(history_table) if history_table_exists else None

            # Fetch Databricks history
            history_data = self._fetch_databricks_history(source_table, max_version)
            if not history_data:
                logger.info("No history data found, performing full load via sync_tables.")
                self._save_history(history_table, [{
                    "version": max_version + 1 if max_version is not None else 1,
                    "timestamp": datetime.now(),
                    "operation": "FULL_LOAD",
                    "operation_parameters": "",
                    "num_affected_rows": 0  # Updated after load
                }])
                num_records = self._full_load(source_table, target_table)
                self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_records)
                return True

            # Save history
            self._save_history(history_table, history_data)

            # Analyze history
            total_changes, version_range, has_schema_change = self._analyze_history(history_data)

            # Decide between full load and incremental merge
            table_exists = target_table.table_name in target_tables
            if not table_exists or total_changes > max_changes or has_schema_change:
                logger.info(f"Target table missing, high changes ({total_changes}), or schema change detected. Performing full load...")
                self._save_history(history_table, [{
                    "version": max_version + 1 if max_version is not None else 1,
                    "timestamp": datetime.now(),
                    "operation": "FULL_LOAD",
                    "operation_parameters": "",
                    "num_affected_rows": 0
                }])
                num_records = self._full_load(source_table, target_table)
                self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_records)
            else:
                logger.info(f"Performing incremental merge with version range {version_range}")
                self._save_history(history_table, [{
                    "version": max_version + 1 if max_version is not None else 1,
                    "timestamp": datetime.now(),
                    "operation": "INCREMENTAL_MERGE",
                    "operation_parameters": f"version_range={version_range}",
                    "num_affected_rows": total_changes
                }])
                self._incremental_merge(source_table, target_table, version_range)
                self._update_history(history_table, max_version + 1 if max_version is not None else 1, total_changes)

            logger.info("Replication completed successfully")
            return True

        except Exception as e:
            logger.error(f"Replication failed: {e}")
            return False

    def _get_max_history_version(self, history_table: DBTable) -> int:
        """Get the maximum version from the history table."""
        with history_table.adapter.session_factory() as session:
            result = session.execute(
                text(f"SELECT MAX(version) FROM {self.target_schema}.{history_table.table_name}")
            ).scalar()
            return result if result is not None else 0

    def _fetch_databricks_history(self, source_table: DBTable, max_version: int = None) -> List[Dict]:
        """Fetch the history of the Databricks table using DESCRIBE HISTORY since max_version."""
        with source_table.adapter.session_factory() as session:
            query = f"DESCRIBE HISTORY {self.source_schema}.{source_table.table_name}"
            if max_version is not None:
                query += f" FROM VERSION AS OF {max_version}"
            result = session.execute(text(query)).fetchall()
            history_data = [
                {
                    "version": row["version"],
                    "timestamp": row["timestamp"],
                    "operation": row["operation"],
                    "operation_parameters": str(row["operationParameters"]),
                    "num_affected_rows": row["operationMetrics"].get("numOutputRows", 0) if row["operationMetrics"] else 0
                }
                for row in result
            ]
            return history_data

    def _save_history(self, history_table: DBTable, history_data: List[Dict]):
        """Save Databricks history to PostgreSQL history table (SCD Type 0)."""
        target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
        if history_table.table_name not in target_tables:
            logger.info(f"History table {history_table.table_name} does not exist. Initializing...")
            metadata = MetaData()
            columns = [
                Column("version", Integer, primary_key=True),
                Column("timestamp", DateTime),
                Column("operation", String),
                Column("operation_parameters", String),
                Column("num_affected_rows", BigInteger)
            ]
            Table(history_table.table_name, metadata, *columns, schema=self.target_schema)
            metadata.create_all(history_table.adapter.engine)
            logger.info(f"Initialized PostgreSQL history table {self.target_schema}.{history_table.table_name}")

        history_table.create(history_data)

    def _update_history(self, history_table: DBTable, version: int, num_affected_rows: int):
        """Update the history table with the number of affected rows for a specific version."""
        with history_table.adapter.session_factory() as session:
            session.execute(
                text(f"""
                    UPDATE {self.target_schema}.{history_table.table_name}
                    SET num_affected_rows = :num_affected_rows
                    WHERE version = :version
                """),
                {"num_affected_rows": num_affected_rows, "version": version}
            )
            session.commit()

    def _analyze_history(self, history_data: List[Dict]) -> Tuple[int, Tuple[int, int], bool]:
        """Analyze history to calculate total changes, version range, and schema changes."""
        total_changes = sum(int(record["num_affected_rows"]) for record in history_data if record["num_affected_rows"])
        version_range = (min(record["version"] for record in history_data), max(record["version"] for record in history_data))
        has_schema_change = any(record["operation"] in ["CREATE TABLE", "ALTER TABLE", "REPLACE TABLE"] for record in history_data)
        return total_changes, version_range, has_schema_change

    def _full_load(self, source_table: DBTable, target_table: DBTable) -> int:
        """Perform a full load using sync_tables with chunking."""
        logger.info(f"Starting full load into {self.target_schema}.{target_table.table_name}")
        self.orchestrator.sync_tables(
            source_db=self.source_db,
            source_schema=self.source_schema,
            source_table=source_table.table_name,
            target_db=self.target_db,
            target_schema=self.target_schema,
            target_table=target_table.table_name,
            filters=None,
            chunk_size=self.batch_size
        )
        # Estimate number of records inserted (since sync_tables doesn't return this)
        total_records = len(source_table.read({}))
        logger.info(f"Completed full load of approximately {total_records} records into {self.target_schema}.{target_table.table_name}")
        return total_records

    def _incremental_merge(self, source_table: DBTable, target_table: DBTable, version_range: Tuple[int, int]):
        """Perform an incremental merge using Databricks Change Data Feed and process_changes."""
        start_version, end_version = version_range
        logger.info(f"Fetching CDF changes for {self.source_schema}.{source_table.table_name} from version {start_version} to {end_version}")

        # Fetch CDF changes
        with source_table.adapter.session_factory() as session:
            cdf_query = f"""
                SELECT * FROM table_changes('{self.source_schema}.{source_table.table_name}', {start_version}, {end_version})
            """
            changes = []
            for chunk in session.execution_options(yield_per=self.cdf_batch_size).execute(text(cdf_query)).partitions():
                changes.extend([dict(row._mapping) for row in chunk])

        total_changes = len(changes)
        logger.info(f"Processing {total_changes} CDF changes into {self.target_schema}.{target_table.table_name}")

        # Convert CDF changes to DataFrames for process_changes
        source_changes = []
        for change in changes:
            if change["_change_type"] in ["insert", "update_postimage"]:
                # Include only relevant columns (exclude CDF metadata)
                record = {k: v for k, v in change.items() if not k.startswith("_")}
                source_changes.append(record)
            elif change["_change_type"] == "delete":
                # Deletes are handled by process_changes
                pass

        # Get current target table data
        target_data = target_table.read({})
        source_df = pd.DataFrame(source_changes) if source_changes else pd.DataFrame(columns=target_table_info.columns.keys())
        target_df = pd.DataFrame(target_data) if target_data else pd.DataFrame(columns=target_table_info.columns.keys())

        # Apply changes using process_changes with chunking
        target_adapter = self.orchestrator.adapters[f"{self.target_db}:{self.target_schema}"]
        self.orchestrator.process_changes(
            adapter=target_adapter,
            table_name=target_table.table_name,
            source_df=source_df,
            target_df=target_df,
            chunk_size=self.cdf_batch_size
        )

        logger.info(f"Completed incremental merge of {total_changes} changes into {self.target_schema}.{target_table.table_name}")