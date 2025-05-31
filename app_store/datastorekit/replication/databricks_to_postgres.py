# datastorekit/replication/databricks_to_postgres_sql.py
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.exceptions import DatastoreOperationError
import logging
import os

logger = logging.getLogger(__name__)

class DatabricksToPostgresSQLReplicator:
    def __init__(self, orchestrator: DataStoreOrchestrator, full_load_batch_size: int = None, cdf_batch_size: int = None):
        """Initialize the replicator for Databricks to PostgreSQL data replication.
        
        Args:
            orchestrator: DataStoreOrchestrator instance to manage adapters and tables.
            full_load_batch_size: Batch size for full load operations (defaults to env var FULL_LOAD_BATCH_SIZE or 100000).
            cdf_batch_size: Batch size for incremental CDF operations (defaults to env var CDF_BATCH_SIZE or 1000).
        """
        self.orchestrator = orchestrator
        self.source_db = "databricks_db"
        self.source_schema = "default"
        self.target_db = "postgres_db"
        self.target_schema = "public"
        self.batch_size = int(os.getenv("FULL_LOAD_BATCH_SIZE", 100000)) if full_load_batch_size is None else full_load_batch_size
        self.cdf_batch_size = int(os.getenv("CDF_BATCH_SIZE", 1000)) if cdf_batch_size is None else cdf_batch_size

    def replicate(self, source_table: str, target_table: str, history_table: str, max_changes: int = 20_000_000) -> bool:
        """Replicate data from a Databricks table to a PostgreSQL table.
        
        Args:
            source_table: Name of the source table in Databricks.
            target_table: Name of the target table in PostgreSQL.
            history_table: Name of the history table in PostgreSQL to track replication.
            max_changes: Maximum number of changes before triggering a full load (default: 20M).
        
        Returns:
            True if replication succeeds, False otherwise.
        """
        try:
            # Initialize table metadata
            source_table_info = TableInfo(
                table_name=source_table,
                keys=None,
                scd_type="type2",
                datastore_key=f"{self.source_db}:{self.source_schema}"
            )
            target_table_info = TableInfo(
                table_name=target_table,
                keys=None,
                scd_type="type2",
                datastore_key=f"{self.target_db}:{self.target_schema}"
            )
            history_table_info = TableInfo(
                table_name=history_table,
                keys="version",
                scd_type="type0",
                datastore_key=f"{self.target_db}:{self.target_schema}"
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
                logger.info("No history data found, performing full load.")
                history_record = {
                    "version": max_version + 1 if max_version is not None else 1,
                    "timestamp": datetime.now(),
                    "operation": "FULL_LOAD",
                    "operation_parameters": "",
                    "num_affected_rows": 0
                }
                history_table.create([history_record])
                num_records = self._full_load(source_table, target_table)
                self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_records)
                return True

            # Save history and analyze changes
            self._save_history(history_table, history_data)
            total_changes, version_range, has_schema_change = self._analyze_history(history_data)

            # Decide between full load or incremental merge
            table_exists = target_table.table_name in target_tables
            if not table_exists or total_changes > max_changes or has_schema_change:
                logger.info(f"Target table missing, high changes ({total_changes}), or schema change detected. Performing full load...")
                history_record = {
                    "version": max_version + 1 if max_version is not None else 1,
                    "timestamp": datetime.now(),
                    "operation": "FULL_LOAD",
                    "operation_parameters": "",
                    "num_affected_rows": 0
                }
                history_table.create([history_record])
                num_records = self._full_load(source_table, target_table)
                self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_records)
            else:
                logger.info(f"Performing incremental merge with version range {version_range}")
                history_record = {
                    "version": max_version + 1 if max_version is not None else 1,
                    "timestamp": datetime.now(),
                    "operation": "INCREMENTAL_MERGE",
                    "operation_parameters": f"version_range={version_range}",
                    "num_affected_rows": 0
                }
                history_table.create([history_record])
                num_changes = self._incremental_merge(source_table, target_table, version_range)
                self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_changes)

            logger.info("Replication completed successfully")
            return True

        except Exception as e:
            logger.error(f"Replication failed: {e}")
            return False

    def _full_load(self, source_table: DBTable, target_table: DBTable) -> int:
        """Perform a full load from Databricks to PostgreSQL.
        
        Args:
            source_table: Source DBTable object (Databricks).
            target_table: Target DBTable object (PostgreSQL).
        
        Returns:
            Approximate number of records loaded.
        """
        logger.info(f"Starting full load into {self.target_schema}.{target_table.table_name}")
        self.orchestrator.sync_tables(
            source_db=self.source_db,
            source_schema=self.source_schema,
            source_table_name=source_table.table_name,
            target_db=self.target_db,
            target_schema=self.target_schema,
            target_table_name=target_table.table_name,
            filters=None,
            chunk_size=self.batch_size
        )
        # Approximate record count from source
        source_count_sql = f"""
            SELECT COUNT(*) FROM {self.source_db}.{self.source_schema}.{source_table.table_name}
        """
        count_result = source_table.adapter.execute_sql(source_count_sql)
        total_records = count_result[0]["count"] if count_result else 0
        logger.info(f"Completed full load of approximately {total_records} records into {self.target_schema}.{target_table.table_name}")
        return total_records

    def _incremental_merge(self, source_table: DBTable, target_table: DBTable, version_range: Tuple[int, int]) -> int:
        """Perform an incremental merge using Databricks Change Data Feed.
        
        Args:
            source_table: Source DBTable object (Databricks).
            target_table: Target DBTable object (PostgreSQL).
            version_range: Tuple of (start_version, end_version) for CDF.
        
        Returns:
            Number of changes applied.
        """
        start_version, end_version = version_range
        logger.info(f"Fetching CDF changes for {self.source_schema}.{source_table.table_name} from version {start_version} to {end_version}")

        # Fetch CDF changes using SQL
        cdf_query = f"""
            SELECT * FROM table_changes('{self.source_schema}.{source_table.table_name}', {start_version}, {end_version})
        """
        changes = []
        for chunk in source_table.adapter.select_chunks(source_table.table_name, chunk_size=self.cdf_batch_size):
            changes.extend(chunk)

        total_changes = len(changes)
        logger.info(f"Processing {total_changes} CDF changes into {self.target_schema}.{target_table.table_name}")

        target_adapter = self.orchestrator.adapters[f"{self.target_db}:{self.target_schema}"]
        processed_changes = 0

        # Process changes in chunks
        for i in range(0, total_changes, self.cdf_batch_size):
            chunk = changes[i:i + self.cdf_batch_size]
            operations = []
            for change in chunk:
                change_type = change.get("_change_type")
                record = {k: v for k, v in change.items() if not k.startswith("_")}
                operation = {
                    "operation": (
                        "create" if change_type in ["insert", "update_postimage"] else
                        "update" if change_type == "update_preimage" else
                        "delete" if change_type == "delete" else None
                    ),
                    **record
                }
                if operation["operation"]:
                    operations.append(operation)

            try:
                target_adapter.apply_changes(
                    table_name=target_table.table_name,
                    changes=operations
                )
                processed_changes += len(chunk)
                logger.debug(f"Applied chunk {i // self.cdf_batch_size + 1} with {len(chunk)} changes")
            except Exception as e:
                logger.error(f"Failed to apply chunk {i // self.cdf_batch_size + 1}: {e}")
                raise DatastoreOperationError(f"Failed to apply chunk: {e}")

        logger.info(f"Completed incremental merge of {total_changes} changes into {self.target_schema}.{target_table.table_name}")
        return total_changes

    def _get_max_history_version(self, history_table: DBTable) -> Optional[int]:
        """Get the maximum version from the history table.
        
        Args:
            history_table: DBTable object for the history table.
        
        Returns:
            Maximum version number or None if table is empty.
        """
        try:
            sql = f"SELECT MAX(version) as max_version FROM {self.target_schema}.{history_table.table_name}"
            result = history_table.adapter.execute_sql(sql)
            return result[0]["max_version"] if result and result[0]["max_version"] is not None else 0
        except Exception as e:
            logger.error(f"Failed to get max history version: {e}")
            return None

    def _fetch_databricks_history(self, source_table: DBTable, max_version: Optional[int] = None) -> List[Dict]:
        """Fetch history data from Databricks table.
        
        Args:
            source_table: Source DBTable object (Databricks).
            max_version: Optional maximum version to filter history.
        
        Returns:
            List of history records.
        """
        try:
            query = f"DESCRIBE HISTORY {self.source_schema}.{source_table.table_name}"
            if max_version is not None:
                query += f" FROM VERSION AS OF {max_version}"
            result = source_table.adapter.execute_sql(query)
            history_data = [
                {
                    "version": row["version"],
                    "timestamp": row["timestamp"],
                    "operation": row["operation"],
                    "operation_parameters": str(row.get("operationParameters", "")),
                    "num_affected_rows": int(row.get("operationMetrics", {}).get("numOutputRows", 0))
                }
                for row in result
            ]
            return history_data
        except Exception as e:
            logger.error(f"Failed to fetch Databricks history: {e}")
            return []

    def _save_history(self, history_table: DBTable, history_data: List[Dict]):
        """Save history data to the PostgreSQL history table.
        
        Args:
            history_table: DBTable object for the history table.
            history_data: List of history records to save.
        """
        target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
        if history_table.table_name not in target_tables:
            logger.info(f"History table {history_table.table_name} does not exist. Initializing...")
            history_schema = {
                "version": "Integer",
                "timestamp": "DateTime",
                "operation": "String",
                "operation_parameters": "String",
                "num_affected_rows": "BigInteger"
            }
            history_table.adapter.create_table(history_table.table_name, history_schema, self.target_schema)
            logger.info(f"Initialized PostgreSQL history table {self.target_schema}.{history_table.table_name}")

        if history_data:
            history_table.create(history_data)

    def _update_history(self, history_table: DBTable, version: int, num_affected_rows: int):
        """Update the history table with the number of affected rows.
        
        Args:
            history_table: DBTable object for the history table.
            version: Version number to update.
            num_affected_rows: Number of affected rows to record.
        """
        try:
            sql = f"""
                UPDATE {self.target_schema}.{history_table.table_name}
                SET num_affected_rows = :num_affected_rows
                WHERE version = :version
            """
            history_table.adapter.execute_sql(sql, {"num_affected_rows": num_affected_rows, "version": version})
        except Exception as e:
            logger.error(f"Failed to update history table: {e}")
            raise DatastoreOperationError(f"Failed to update history table: {e}")

    def _analyze_history(self, history_data: List[Dict]) -> Tuple[int, Tuple[int, int], bool]:
        """Analyze history data to determine total changes and schema changes.
        
        Args:
            history_data: List of history records.
        
        Returns:
            Tuple of (total_changes, version_range, has_schema_change).
        """
        total_changes = sum(int(record["num_affected_rows"]) for record in history_data if record["num_affected_rows"])
        version_range = (
            min(record["version"] for record in history_data),
            max(record["version"] for record in history_data)
        ) if history_data else (0, 0)
        has_schema_change = any(record["operation"] in ["CREATE TABLE", "ALTER TABLE", "REPLACE TABLE"] for record in history_data)
        return total_changes, version_range, has_schema_change