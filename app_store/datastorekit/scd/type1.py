# datastorekit/scd/type1.py
from datastorekit.scd.base import SCDHandler
from typing import List, Dict, Any, Union, Optional

class Type1Handler(SCDHandler):
    def create(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]):
        data = [data] if isinstance(data, dict) else data
        self.adapter.insert(self.table_name, data)

    def read(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.adapter.select(self.table_name, filters)

    def update(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], filters: Optional[Dict[str, Any]] = None):
        data = [data] if isinstance(data, dict) else data
        self.adapter.update(self.table_name, data, filters)

    def delete(self, filters: Optional[Dict[str, Any]] = None):
        self.adapter.delete(self.table_name, filters)