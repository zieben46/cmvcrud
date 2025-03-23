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

    def process_list_edits(self, original_data: List[Dict], new_data: List[Dict]) -> int:
    applied_count = 0
    try:
        with self.engine.begin() as session:
            original_dict = {row[self.key]: row for row in original_data}
            new_dict = {row[self.key]: row for row in new_data}

            # Deletes (unlikely here, but included for completeness)
            delete_data = [{self.key: k} for k in original_dict if k not in new_dict]
            if delete_data:
                self.scd_handler.delete(delete_data, session)
                applied_count += len(delete_data)

            # Inserts and updates
            insert_data = []
            update_data = []
            for key, new_row in new_dict.items():
                if key not in original_dict:
                    insert_data.append(new_row)
                elif original_dict[key] != new_row:
                    update_data.append({**new_row, "filters": {self.key: key}})

            if insert_data:
                self.scd_handler.create(insert_data, session)
                applied_count += len(insert_data)
            if update_data:
                self.scd_handler.update(update_data, session)
                applied_count += len(update_data)
    except Exception as e:
        raise Exception(f"Failed to process edits: {str(e)}")
    
    return applied_count