# datastorekit/models/db_table.py
from typing import Dict, Any, List
from datastorekit.scd.type0 import Type0Handler
from datastorekit.scd.type1 import Type1Handler
from datastorekit.scd.type2 import Type2Handler

class DBTable:
    def __init__(self, adapter: DatastoreAdapter, table_info: Dict[str, Any]):
        self.adapter = adapter
        self.table_info = table_info
        self.table_name = table_info["table_name"]
        self.key = table_info.get("key", "id")
        self.scd_type = table_info.get("scd_type", "type1")
        self.scd_handler = self._get_scd_handler()

    def _get_scd_handler(self):
        if self.scd_type == "type0":
            return Type0Handler(self.adapter, self.table_name, self.key)
        elif self.scd_type == "type1":
            return Type1Handler(self.adapter, self.table_name, self.key)
        elif self.scd_type == "type2":
            return Type2Handler(self.adapter, self.table_name, self.key)
        else:
            raise ValueError(f"Unsupported SCD type: {self.scd_type}")

    def create(self, data: List[Dict[str, Any]]):
        self.scd_handler.create(data)

    def read(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self.scd_handler.read(filters)

    def update(self, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.scd_handler.update(data, filters)

    def delete(self, filters: Dict[str, Any]):
        self.scd_handler.delete(filters)