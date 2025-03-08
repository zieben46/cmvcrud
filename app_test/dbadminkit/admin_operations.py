from typing import List, Dict, Any
from sqlalchemy.sql import select
from sqlalchemy.orm import Session
from dbadminkit.core.engine import DBEngine
from dbadminkit.models.postgres.table_models import DBTable as PostgresDBTable
from dbadminkit.models.postgres.scd_types import SCDType1
from dbadminkit.models.databricks.table_models import DBTable as DatabricksDBTable
from dbadminkit.core.crud_operations import CRUDOperation

class AdminDBOps:
    def __init__(self, config):
        self.engine = DBEngine(config)
        self.db_type = config.mode

    def get_table(self, table_info: Dict[str, Any]):
        if "databricks" in self.db_type.value.lower():
            return DatabricksDBTable(self.engine.engine, table_info)
        return PostgresDBTable(self.engine.engine, table_info)

    def perform_crud(self, table_info: Dict[str, Any], crud_type: CRUDOperation, data: Dict[str, Any]) -> Any:
        table = self.get_table(table_info)
        return table.perform_crud(crud_type, data)

    def process_cdc_logs(self, table_info: Dict[str, Any], cdc_records: List[Dict[str, Any]]) -> int:
        table = self.get_table(table_info)
        return table.process_cdc_logs(cdc_records)

    def process_dataframe_edits(self, table_info: Dict[str, Any], original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
        table = self.get_table(table_info)
        return table.process_dataframe_edits(original_df, edited_df)

    def get_schema(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        table = self.get_table(table_info)
        return table.get_db_table_schema()

    def sync_scd2_versions(self, source_ops: 'AdminDBOps', source_table_info: Dict[str, Any], 
                          target_table_info: Dict[str, Any], min_version: int, max_version: int) -> int:
        source_table = source_ops.get_table(source_table_info)
        target_table = self.get_table(target_table_info)
        key = source_table_info.get("key", "id")
        on_date_col = source_table_info.get("on_date_col", "on_date")
        off_date_col = source_table_info.get("off_date_col", "off_date")
        version_col = source_table_info.get("version_col", "version")

        source_stmt = (
            select(source_table.table)
            .where(
                source_table.table.c[version_col] >= min_version,
                source_table.table.c[version_col] <= max_version
            )
            .order_by(source_table.table.c[key], source_table.table.c[version_col])
        )
        with source_ops.engine.begin() as session:
            source_rows = [dict(row) for row in session.execute(source_stmt).fetchall()]

        applied_count = 0
        prev_row = None
        with self.engine.begin() as session:
            for row in source_rows:
                data = {k: v for k in row}
                key_value = row[key]
                target_exists = target_table.perform_crud(
                    target_table_info, CRUDOperation.READ, {key: key_value, version_col: row[version_col]}
                )
                if prev_row and prev_row[key] == key_value and prev_row[off_date_col] is None and row[off_date_col] is not None:
                    target_table.perform_crud(
                        target_table_info, CRUDOperation.DELETE, {key: key_value}
                    )
                    applied_count += 1
                if not target_exists:
                    target_table.perform_crud(target_table_info, CRUDOperation.CREATE, data)
                    applied_count += 1
                else:
                    target_row = target_exists[0]
                    if any(target_row[k] != v for k, v in data.items() if k != off_date_col):
                        target_table.perform_crud(
                            target_table_info, CRUDOperation.UPDATE, 
                            {**data, "filters": {key: key_value, version_col: row[version_col]}}
                        )
                        applied_count += 1
                if row[off_date_col] is not None and prev_row and prev_row[key] == key_value:
                    prev_version = row[version_col] - 1
                    prev_exists = target_table.perform_crud(
                        target_table_info, CRUDOperation.READ, {key: key_value, version_col: prev_version}
                    )
                    if prev_exists and prev_exists[0][off_date_col] is None:
                        target_table.perform_crud(
                            target_table_info, CRUDOperation.UPDATE,
                            {"filters": {key: key_value, version_col: prev_version}, off_date_col: row[on_date_col]}
                        )
                        applied_count += 1
                prev_row = row
        return applied_count