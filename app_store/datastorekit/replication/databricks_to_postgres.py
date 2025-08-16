from typing import Dict, List, Tuple, Optional
from datetime import datetime
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.exceptions import DatastoreOperationError
import logging
import os
from collections import defaultdict

logger = logging.getLogger(__name__)

class DatabricksToPostgresSQLReplicator:
    def __init__(self, orchestrator: DataStoreOrchestrator, full_load_batch_size: int = None, cdf_batch_size: int = None):
        self.orchestrator = orchestrator
        self.source_db = "databricks_db"
        self.source_schema = "default"
        self.target_db = "postgres_db"
        self.target_schema = "public"
        self.batch_size = int(os.getenv("FULL_LOAD_BATCH_SIZE", 100000)) if full_load_batch_size is None else full_load_batch_size
        self.cdf_batch_size = int(os.getenv("CDF_BATCH_SIZE", 1000)) if cdf_batch_size is None else cdf_batch_size

    def _get_session(self, adapter):
        return adapter.get_session()

    def replicate(self, source_table: str, target_table: str, history_table: str, max_changes: int = 20_000_000) -> bool:
        try:
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
            source_table = self.orchestrator.get_table(f"{self.source_db}:{self.source_schema}", source_table_info)
            target_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", target_table_info)
            history_table = self.orchestrator.get_table(f"{self.target_db}:{self.target_schema}", history_table_info)
            target_adapter = self.orchestrator.adapters[f"{self.target_db}:{self.target_schema}"]
            target_tables = self.orchestrator.list_tables(self.target_db, self.target_schema)
            history_table_exists = history_table.table_name in target_tables
            max_version = self._get_max_history_version(history_table) if history_table_exists else None
            history_data = self._fetch_databricks_history(source_table, max_version)
            with self._get_session(target_adapter) as session:
                if not history_data:
                    logger.info("No history data found, performing full load.")
                    history_record = {
                        "version": max_version + 1 if max_version is not None else 1,
                        "timestamp": datetime.now(),
                        "operation": "FULL_LOAD",
                        "operation_parameters": "",
                        "num_affected_rows": 0
                    }
                    self._save_history(history_table, [history_record], session)
                    num_records = self._full_load(source_table, target_table, session)
                    self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_records, session)
                    return True
                self._save_history(history_table, history_data, session)
                total_changes, version_range, has_schema_change = self._analyze_history(history_data)
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
                    self._save_history(history_table, [history_record], session)
                    num_records = self._full_load(source_table, target_table, session)
                    self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_records, session)
                else:
                    logger.info(f"Performing incremental merge with version range {version_range}")
                    history_record = {
                        "version": max_version + 1 if max_version is not None else 1,
                        "timestamp": datetime.now(),
                        "operation": "INCREMENTAL_MERGE",
                        "operation_parameters": f"version_range={version_range}",
                        "num_affected_rows": 0
                    }
                    self._save_history(history_table, [history_record], session)
                    num_changes = self._incremental_merge(source_table, target_table, version_range, session)
                    self._update_history(history_table, max_version + 1 if max_version is not None else 1, num_changes, session)
            logger.info("Replication completed successfully")
            return True
        except Exception as e:
            logger.error(f"Replication failed: {e}")
            return False

    def _full_load(self, source_table: DBTable, target_table: DBTable, session) -> int:
        logger.info(f"Starting full load into {self.target_schema}.{target_table.table_name}")
        self.orchestrator.sync_tables(
            source_db=self.source_db,
            source_schema=self.source_schema,
            source_table_name=source_table.table_name,
            target_db=self.target_db,
            target_schema=self.target_schema,
            target_table_name=target_table.table_name,
            filters=None,
            chunk_size=self.batch_size,
            session=session
        )
        source_count_sql = f"""
            SELECT COUNT(*) FROM {self.source_db}.{self.source_schema}.{source_table.table_name}
        """
        count_result = source_table.adapter.execute_sql(source_count_sql)
        total_records = count_result[0]["count"] if count_result else 0
        logger.info(f"Completed full load of approximately {total_records} records into {self.target_schema}.{target_table.table_name}")
        return total_records

    def _incremental_merge(self, source_table: DBTable, target_table: DBTable, version_range: Tuple[int, int], session) -> int:
        start_version, end_version = version_range
        logger.info(f"Fetching CDF changes for {self.source_schema}.{source_table.table_name} from version {start_version} to {end_version}")
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
        changes_by_version = defaultdict(list)
        for change in changes:
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
                version = change.get("_commit_version", float("inf"))
                changes_by_version[version].append(operation)
        key_columns = target_adapter.get_reflected_keys(target_table.table_name) or (target_adapter.profile.keys.split(",") if target_adapter.profile.keys else [])
        if not key_columns:
            raise ValueError(f"No primary keys defined for table {target_table.table_name}")
        for version in sorted(changes_by_version.keys()):
            version_changes = changes_by_version[version]
            logger.debug(f"Processing {len(version_changes)} changes for version {version}")
            for i in range(0, len(version_changes), self.cdf_batch_size):
                chunk = version_changes[i:i + self.cdf_batch_size]
                try:
                    target_adapter.apply_changes(
                        table_name=target_table.table_name,
                        changes=chunk,
                        session=session
                    )
                    processed_changes += len(chunk)
                    logger.debug(f"Applied chunk {i // self.cdf_batch_size + 1} with {len(chunk)} changes for version {version}")
                except Exception as e:
                    logger.error(f"Failed to apply chunk {i // self.cdf_batch_size + 1} for version {version}: {e}")
                    raise DatastoreOperationError(f"Failed to apply chunk for version {version}: {e}")
        logger.info(f"Completed incremental merge of {total_changes} changes into {self.target_schema}.{target_table.table_name}")
        return total_changes

    def _get_max_history_version(self, history_table: DBTable) -> Optional[int]:
        try:
            sql = f"SELECT MAX(version) as max_version FROM {self.target_schema}.{history_table.table_name}"
            result = history_table.adapter.execute_sql(sql)
            return result[0]["max_version"] if result and result[0]["max_version"] is not None else 0
        except Exception as e:
            logger.error(f"Failed to get max history version: {e}")
            return None

    def _fetch_databricks_history(self, source_table: DBTable, max_version: Optional[int] = None) -> List[Dict]:
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

    def _save_history(self, history_table: DBTable, history_data: List[Dict], session):
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
        if history_data:
            history_table.adapter.create_records_with_session(
                DBTable(history_table.table_name, Table(history_table.table_name, history_table.adapter.metadata, autoload_with=history_table.adapter.engine)),
                history_data,
                session
            )

    def _update_history(self, history_table: DBTable, version: int, num_affected_rows: int, session):
        try:
            sql = f"""
                UPDATE {self.target_schema}.{history_table.table_name}
                SET num_affected_rows = :num_affected_rows
                WHERE version = :version
            """
            session.execute(text(sql), {"num_affected_rows": num_affected_rows, "version": version})
        except Exception as e:
            logger.error(f"Failed to update history table: {e}")
            raise DatastoreOperationError(f"Failed to update history table: {e}")

    def _analyze_history(self, history_data: List[Dict]) -> Tuple[int, Tuple[int, int], bool]:
        total_changes = sum(int(record["num_affected_rows"]) for record in history_data if record["num_affected_rows"])
        version_range = (
            min(record["version"] for record in history_data),
            max(record["version"] for record in history_data)
        ) if history_data else (0, 0)
        has_schema_change = any(record["operation"] in ["CREATE TABLE", "ALTER TABLE", "REPLACE TABLE"] for record in history_data)
        return total_changes, version_range, has_schema_change