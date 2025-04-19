# datastorekit/replication/databricks_to_postgres.py
import logging
from typing import Dict, List
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from databricks.sqlalchemy import TIMESTAMP, TINYINT
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.profile import DatabaseProfile
from datastorekit.models.db_table import DBTable
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class DatabricksToPostgresReplicator:
    def __init__(self, orchestrator: DataStoreOrchestrator):
        self.orchestrator = orchestrator
        self.source_db = "databricks_db"
        self.source_schema = "default"
        self.target_db = "spend_plan_db"
        self.target_schema = "safe_user"

    def replicate(self, source_table: str, target_table: str, history_table: str, max_changes: int = 20_000_000):
        """Replicate data from a Databricks table to a PostgreSQL table, maintaining history.

        Args:
            source_table: Source table name in Databricks (e.g., 'spend_plan').
            target_table: Target table name in PostgreSQL (e.g., 'spend_plan').
            history_table: History table name in PostgreSQL (e.g., 'spend_plan_history').
            max_changes: Maximum number of changes before full reload (default: 20,000,000).

        Returns:
            bool: True if replication succeeds, False otherwise.
        """
        try:
            # Get source and target tables
            source_table_info = {"table_name": source_table, "scd_type": "type1", "key": "id"}
            target_table_info = {"table_name": target_table, "scd_type": "type1", "key": "id"}
            history_table_info = {"table_name": history_table, "scd_type": "type0", "key": "version"}
            source_table = self.orchestrator.get_table(f"{self.source_db}:{self.source_schema}", source_table_info)
            target_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", target_table_info)
            history_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", history_table_info)

            # Fetch Databricks history
            history_data = self._fetch_databricks_history(source_table)
            if not history_data:
                logger.error("No history data found for source table")
                return False

            # Save history to PostgreSQL (SCD Type 0)
            self._save_history(history_table, history_data)

            # Calculate changes and check for schema changes
            total_changes, version_range, has_schema_change = self._analyze_history(history_data)

            # Check if target table exists
            target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
            table_exists = target_table.table_name in target_tables

            if not table_exists:
                logger.info(f"Target table {target_table.table_name} does not exist. Initializing...")
                self._initialize_table(source_table, target_table)
                self._full_load(source_table, target_table)
            elif total_changes > max_changes or has_schema_change:
                logger.info(f"High changes ({total_changes}) or schema change detected. Re-initializing table...")
                self._drop_and_initialize_table(source_table, target_table)
                self._full_load(source_table, target_table)
            else:
                logger.info(f"Performing incremental merge with version range {version_range}")
                self._incremental_merge(source_table, target_table, version_range)

            logger.info("Replication completed successfully")
            return True
        except Exception as e:
            logger.error(f"Replication failed: {e}")
            return False

    def _fetch_databricks_history(self, source_table: DBTable) -> List[Dict]:
        """Fetch the history of the Databricks table using DESCRIBE HISTORY."""
        with source_table.adapter.session_factory() as session:
            result = session.execute(
                text(f"DESCRIBE HISTORY {self.source_schema}.{source_table.table_name}")
            ).fetchall()
            history_data = [
                {
                    "version": row["version"],
                    "timestamp": row["timestamp"],
                    "operation": row["operation"],
                    "operationParameters": str(row["operationParameters"]),
                    "num_affected_rows": row["operationMetrics"].get("numOutputRows", 0) if row["operationMetrics"] else 0
                }
                for row in result
            ]
            return history_data

    def _save_history(self, history_table: DBTable, history_data: List[Dict]):
        """Save Databricks history to PostgreSQL history table (SCD Type 0)."""
        history_table.create(history_data)

    def _analyze_history(self, history_data: List[Dict]) -> tuple:
        """Analyze history to calculate total changes, version range, and schema changes."""
        total_changes = sum(int(record["num_affected_rows"]) for record in history_data if record["num_affected_rows"])
        version_range = (min(record["version"] for record in history_data), max(record["version"] for record in history_data))
        has_schema_change = any(record["operation"] in ["CREATE TABLE", "ALTER TABLE", "REPLACE TABLE"] for record in history_data)
        return total_changes, version_range, has_schema_change

    def _initialize_table(self, source_table: DBTable, target_table: DBTable):
        """Initialize the PostgreSQL target table with the Databricks schema."""
        with source_table.adapter.session_factory() as session:
            schema_df = session.execute(
                text(f"DESCRIBE {self.source_schema}.{source_table.table_name}")
            ).fetchall()
            schema = {}
            for row in schema_df:
                field_name = row["col_name"]
                field_type = row["data_type"]
                if field_name == "id":
                    schema[field_name] = Integer
                elif field_type in ["string", "varchar"]:
                    schema[field_name] = String
                elif field_type in ["float", "double"]:
                    schema[field_name] = Float
                elif field_type == "boolean":
                    schema[field_name] = Boolean
                elif field_type in ["timestamp", "date"]:
                    schema[field_name] = DateTime
                else:
                    schema[field_name] = String  # Fallback

        metadata = MetaData()
        columns = [Column("id", Integer, primary_key=True)] + [
            Column(name, col_type) for name, col_type in schema.items() if name != "id"
        ]
        Table(target_table.table_name, metadata, *columns, schema=self.target_schema)
        metadata.create_all(target_table.adapter.engine)
        logger.info(f"Initialized PostgreSQL table {self.target_schema}.{target_table.table_name}")

    def _drop_and_initialize_table(self, source_table: DBTable, target_table: DBTable):
        """Drop and re-initialize the PostgreSQL target table."""
        with target_table.adapter.session_factory() as session:
            session.execute(text(f"DROP TABLE IF EXISTS {self.target_schema}.{target_table.table_name}"))
            session.commit()
        self._initialize_table(source_table, target_table)

    def _full_load(self, source_table: DBTable, target_table: DBTable):
        """Perform a full load of all records from Databricks to PostgreSQL."""
        records = source_table.read({})
        target_table.create(records)
        logger.info(f"Inserted {len(records)} records into {self.target_schema}.{target_table.table_name}")

    def _incremental_merge(self, source_table: DBTable, target_table: DBTable, version_range: tuple):
        """Perform an incremental merge using Databricks Change Data Feed."""
        start_version, end_version = version_range
        with source_table.adapter.session_factory() as session:
            cdf_query = f"""
                SELECT * FROM table_changes('{self.source_schema}.{source_table.table_name}', {start_version}, {end_version})
            """
            changes = session.execute(text(cdf_query)).fetchall()
            changes = [dict(row) for row in changes]
        with target_table.adapter.session_factory() as session:
            for change in changes:
                if change["_change_type"] in ["insert", "update_postimage"]:
                    session.execute(
                        text(f"INSERT INTO {self.target_schema}.{target_table.table_name} ({', '.join(change.keys())}) VALUES ({', '.join([':' + k for k in change.keys()])}) "
                             f"ON CONFLICT (id) DO UPDATE SET {', '.join([f'{k}=EXCLUDED.{k}' for k in change.keys() if k != 'id'])}"),
                        change
                    )
                elif change["_change_type"] == "delete":
                    session.execute(
                        text(f"DELETE FROM {self.target_schema}.{target_table.table_name} WHERE id = :id"),
                        {"id": change["id"]}
                    )
            session.commit()
        logger.info(f"Merged {len(changes)} changes into {self.target_schema}.{target_table.table_name}")