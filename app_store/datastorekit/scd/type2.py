# datastorekit/scd/type2.py
from datastorekit.scd.base import SCDHandler
from datetime import datetime
from typing import List, Dict, Any

class Type2Handler(SCDHandler):
    def create(self, data: List[Dict[str, Any]]):
        for record in data:
            record["start_date"] = datetime.now()
            record["end_date"] = None
            record["is_active"] = True
        self.adapter.insert(self.table_name, data)

    def read(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self.adapter.select(self.table_name, filters)

    def update(self, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        for update_data in data:
            update_data["start_date"] = datetime.now()
            update_data["end_date"] = None
            update_data["is_active"] = True
            self.adapter.update(self.table_name, [{"end_date": datetime.now(), "is_active": False}], filters)
            self.adapter.insert(self.table_name, [update_data])

    def delete(self, filters: Dict[str, Any]):
        self.adapter.delete(self.table_name, filters)