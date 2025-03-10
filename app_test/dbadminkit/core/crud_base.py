from typing import Dict, Any, List
from sqlalchemy import Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
import pandas as pd
from app_test.dbadminkit.core.crud_types import CRUDOperation
from app_test.dbadminkit.core.scd_handler import SCDTableHandler

class CRUDBase:
    def __init__(self, table: Table, engine: Engine, key: str, scd_handler: SCDTableHandler):
        self.table = table
        self.engine = engine
        self.key = key
        self.scd_handler = scd_handler

    def process_cdc_logs(self, cdc_records: List[Dict[str, Any]]) -> int:
        if not cdc_records:
            return 0
        sorted_records = sorted(cdc_records, key=lambda x: x.get("timestamp", ""))
        inserts = [r["data"] for r in sorted_records if r.get("operation") == "INSERT"]
        updates = [r["data"] for r in sorted_records if r.get("operation") == "UPDATE"]
        deletes = [{self.key: r.get(self.key) or r.get("data", {}).get(self.key)} 
                  for r in sorted_records if r.get("operation") == "DELETE"]
        applied_count = 0
        with self.engine.begin() as session:  # Use a transaction
            if inserts:
                self.scd_handler.create(inserts, session)
                applied_count += len(inserts)
            if updates:
                self.scd_handler.update(updates, session)
                applied_count += len(updates)
            if deletes:
                self.scd_handler.delete(deletes, session)
                applied_count += len(deletes)
        return applied_count

    def process_dataframe_edits(self, original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
        applied_count = 0
        with self.engine.begin() as session:  # Use a transaction
            # Detect deletes
            deleted_rows = original_df[~original_df[self.key].isin(edited_df[self.key])]
            if not deleted_rows.empty:
                delete_data = [{self.key: row[self.key]} for _, row in deleted_rows.iterrows()]
                self.scd_handler.delete(delete_data, session)
                applied_count += len(delete_data)

            # Detect inserts and updates
            insert_data = []
            update_data = []
            for _, row in edited_df.iterrows():
                row_dict = row.to_dict()
                existing = original_df[original_df[self.key] == row[self.key]]
                if existing.empty:
                    insert_data.append(row_dict)
                elif not existing.iloc[0].equals(row):
                    update_data.append({**row_dict, "filters": {self.key: row[self.key]}})
            
            if insert_data:
                self.scd_handler.create(insert_data, session)
                applied_count += len(insert_data)
            if update_data:
                self.scd_handler.update(update_data, session)
                applied_count += len(update_data)
        
        return applied_count