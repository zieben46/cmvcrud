# datastorekit/scd/type2.py
from datastorekit.scd.base import SCDHandler
from datetime import datetime
from typing import List, Dict, Any, Union, Optional

class Type2Handler(SCDHandler):
    def create(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]):
        data = [data] if isinstance(data, dict) else data
        for record in data:
            record["start_date"] = datetime.now()
            record["end_date"] = None
            record["is_active"] = True
        self.adapter.insert(self.table_name, data)

    def read(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        read_filters = filters.copy() if filters else {}
        if "is_active" not in read_filters:
            read_filters["is_active"] = True
        return self.adapter.select(self.table_name, read_filters)

    def update(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], filters: Optional[Dict[str, Any]] = None):
        data = [data] if isinstance(data, dict) else data
        for update_data in data:
            update_data["start_date"] = datetime.now()
            update_data["end_date"] = None
            update_data["is_active"] = True
            # End-date existing records
            self.adapter.update(self.table_name, [{"end_date": datetime.now(), "is_active": False}], filters)
            # Insert new version
            self.adapter.insert(self.table_name, [update_data])

    def delete(self, filters: Optional[Dict[str, Any]] = None):
        self.adapter.delete(self.table_name, filters)