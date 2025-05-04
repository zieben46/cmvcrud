# datastorekit/models/db_table.py
from typing import Dict, List, Iterator, Optional, Union, Any
from datastorekit.scd.type0 import Type0Handler
from datastorekit.scd.type1 import Type1Handler
from datastorekit.scd.type2 import Type2Handler
from datastorekit.adapters.base import DatastoreAdapter

class DBTable:
    def __init__(self, adapter: DatastoreAdapter, table_info: Dict[str, Any]):
        self.adapter = adapter
        self.table_info = table_info
        self.table_name = table_info.table_name
        self.keys = table_info.keys.split(",") if table_info.keys else []
        self.scd_type = table_info.scd_type
        self.scd_handler = self._get_scd_handler()
        self.columns = self.adapter.get_table_columns(self.table_name, schema_name=self.adapter.profile.schema)
        self._validate_keys()

    def _validate_keys(self):
        reflected_keys = self.adapter.get_reflected_keys(self.table_name)
        if self.keys and set(self.keys) != set(reflected_keys):
            raise ValueError(
                f"Supplied keys {self.keys} do not match reflected table keys {reflected_keys} for table {self.table_name}"
            )
        self.keys = reflected_keys or self.keys

    def _get_scd_handler(self):
        if self.scd_type == "type0":
            return Type0Handler(self.adapter, self.table_name, self.keys)
        elif self.scd_type == "type1":
            return Type1Handler(self.adapter, self.table_name, self.keys)
        elif self.scd_type == "type2":
            return Type2Handler(self.adapter, self.table_name, self.keys)
        else:
            raise ValueError(f"Unsupported SCD type: {self.scd_type}")

    def create(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]):
        self.scd_handler.create(data)

    def read(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.scd_handler.read(filters)

    def read_chunks(self, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        for chunk in self.scd_handler.read_chunks(filters, chunk_size):
            yield chunk

    def update(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], filters: Optional[Dict[str, Any]] = None):
        self.scd_handler.update(data, filters)

    def delete(self, filters: Optional[Dict[str, Any]] = None):
        self.scd_handler.delete(filters)

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return self.adapter.execute_sql(sql, parameters)