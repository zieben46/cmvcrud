# datastorekit/scd/type0.py
from datastorekit.scd.base import SCDHandler
from typing import List, Dict, Any

class Type0Handler(SCDHandler):
    def create(self, data: List[Dict[str, Any]]):
        self.adapter.insert(self.table_name, data)

    def read(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self.adapter.select(self.table_name, filters)

    def update(self, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        raise NotImplementedError("SCD Type 0 does not support updates")

    def delete(self, filters: Dict[str, Any]):
        raise NotImplementedError("SCD Type 0 does not support deletes")