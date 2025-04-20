# datastorekit/replication/databricks_to_postgres.py
import logging
import os
from typing import Dict, List, Iterator
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from databricks.sqlalchemy import TIMESTAMP, TINYINT
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from typing import List, Dict, Any

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
        """Replicate data from a Databricks table to a PostgreSQL table, maintaining history."""
        try:
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
            source_table = self.orchestrator.get_table(f"{self.source_db}:{self.source_schema}", source_table_info)
            target_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", target_table_info)
            history_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", history_table_info)

            target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
            history_table_exists = history_table.table_name in target_tables

            max_version = self._get_max_history_version(history_table) if history_table_exists else None
            history_data = self._fetch_databricks_history(source_table, max_version)
            if not history_data:
                logger.info("No history data found, performing full load.")
                self._drop_and_initialize_table(source_table, target_table)
                self._full_load(source_table, target_table)
                return True

            self._save_history(history_table, history_data)

            total_changes, version_range, has_schema_change = self._analyze_history(history_data)

            table_exists = target_table.table_name in target_tables

            if not table_exists or total_changes > max_changes or has_schema_change:
                logger.info(f"Target table missing, high changes ({total_changes}), or schema change detected. Performing full load...")
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
                query += f" STARTING VERSION {max_version}"
            result = session.execute(text(query)).fetchall()
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
        target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
        if history_table.table_name not in target_tables:
            logger.info(f"History table {history_table.table_name} does not exist. Initializing...")
            metadata = MetaData()
            columns = [Column("version", Integer, primary_key=True)] + [
                Column(name, getattr(sqlalchemy, col_type)) for name, col_type in history_table.table_info.columns.items() if name != "version"
            ]
            Table(history_table.table_name, metadata, *columns, schema=self.target_schema)
            metadata.create_all(history_table.adapter.engine)
            logger.info(f"Initialized PostgreSQL history table {self.target_schema}.{history_table.table_name}")

        history_table.create(history_data)

    def _analyze_history(self, history_data: List[Dict]) -> tuple:
        """Analyze history to calculate total changes, version range, and schema changes."""
        total_changes = sum(int(record["num_affected_rows"]) for record in history_data if record["num_affected_rows"])
        version_range = (min(record["version"] for record in history_data), max(record["version"] for record in history_data))
        has_schema_change = any(record["operation"] in ["CREATE TABLE", "ALTER TABLE", "REPLACE TABLE"] for record in history_data)
        return total_changes, version_range, has_schema_change

    def _initialize_table(self, source_table: DBTable, target_table: DBTable):
        """Initialize the PostgreSQL target table with the source schema."""
        schema = target_table.table_info.columns if target_table.table_info.columns else {}
        if not schema:
            try:
                with source_table.adapter.session_factory() as session:
                    schema_df = session.execute(
                        text(f"DESCRIBE {self.source_schema}.{source_table.table_name}")
                    ).fetchall()
                    for row in schema_df:
                        field_name = row["col_name"]
                        field_type = row["data_type"]
                        if field_name in target_table.keys:
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
            except Exception as e:
                logger.warning(f"Failed to describe source table, using TableInfo.columns: {e}")

        metadata = MetaData()
        columns = [Column(key, Integer, primary_key=True) for key in target_table.keys] + [
            Column(name, col_type) for name, col_type in schema.items() if name not in target_table.keys
        ]
        Table(target_table.table_name, metadata, *columns, schema=self.target_schema)
        target_table.table_info.columns = schema  # Update TableInfo columns
        metadata.create_all(target_table.adapter.engine)
        logger.info(f"Initialized PostgreSQL table {self.target_schema}.{target_table.table_name}")

    def _drop_and_initialize_table(self, source_table: DBTable, target_table: DBTable):
        """Drop and re-initialize the PostgreSQL target table."""
        with target_table.adapter.session_factory() as session:
            session.execute(text(f"DROP TABLE IF EXISTS {self.target_schema}.{target_table.table_name}"))
            session.commit()
        self._initialize_table(source_table, target_table)

    def _full_load(self, source_table: DBTable, target_table: DBTable):
        """Perform a full load of all records from source to PostgreSQL with chunking."""
        total_records = 0
        checkpoint_file = f"checkpoint_{target_table.table_name}.txt"
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                total_records = int(f.read())
        logger.info(f"Starting full load into {self.target_schema}.{target_table.table_name}")
        with create_engine(target_table.adapter.engine.url).connect() as conn:
            for chunk in source_table.read_chunks({}, chunk_size=self.batch_size):
                if not chunk:
                    continue
                total_records += len(chunk)
                for attempt in range(3):
                    try:
                        columns = ','.join(chunk[0].keys())
                        values = '\n'.join([
                            '\t'.join([str(record.get(k, '')) for k in chunk[0].keys()])
                            for record in chunk
                        ])
                        conn.execute(
                            text(f"COPY {self.target_schema}.{target_table.table_name} ({columns}) FROM STDIN"),
                            {"data": values}
                        )
                        conn.commit()
                        with open(checkpoint_file, 'w') as f:
                            f.write(str(total_records))
                        break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt+1} failed: {e}")
                        if attempt == 2:
                            raise
                logger.info(f"Inserted chunk of {len(chunk)} records, total: {total_records}")
        os.remove(checkpoint_file) if os.path.exists(checkpoint_file) else None
        logger.info(f"Completed full load of {total_records} records into {self.target_schema}.{target_table.table_name}")

    def _incremental_merge(self, source_table: DBTable, target_table: DBTable, version_range: tuple):
        """Perform an incremental merge using Databricks Change Data Feed with batching."""
        start_version, end_version = version_range
        with source_table.adapter.session_factory() as session:
            cdf_query = f"""
                SELECT * FROM table_changes('{self.source_schema}.{source_table.table_name}', {start_version}, {end_version})
            """
            changes = []
            for chunk in session.execution_options(yield_per=self.cdf_batch_size).execute(text(cdf_query)).partitions():
                changes.extend([dict(row._mapping) for row in chunk])

        total_changes = len(changes)
        logger.info(f"Starting incremental merge of {total_changes} changes into {self.target_schema}.{target_table.table_name}")

        with target_table.adapter.session_factory() as session:
            for i in range(0, total_changes, self.cdf_batch_size):
                batch = changes[i:i + self.cdf_batch_size]
                for change in batch:
                    if change["_change_type"] in ["insert", "update_postimage"]:
                        key_conditions = ', '.join([f'{k}=EXCLUDED.{k}' for k in target_table.keys])
                        session.execute(
                            text(f"INSERT INTO {self.target_schema}.{target_table.table_name} ({', '.join(change.keys())}) "
                                 f"VALUES ({', '.join([':' + k for k in change.keys()])}) "
                                 f"ON CONFLICT ({', '.join(target_table.keys)}) DO UPDATE SET {key_conditions}"),
                            change
                        )
                    elif change["_change_type"] == "delete":
                        session.execute(
                            text(f"DELETE FROM {self.target_schema}.{target_table.table_name} WHERE " + 
                                 " AND ".join([f"{k} = :{k}" for k in target_table.keys])),
                            {k: change[k] for k in target_table.keys}
                        )
                session.commit()
                logger.info(f"Processed batch {i // self.cdf_batch_size + 1} of {total_changes // self.cdf_batch_size + 1} ({len(batch)} changes)")

        logger.info(f"Completed incremental merge of {total_changes} changes into {self.target_schema}.{target_table.table_name}")