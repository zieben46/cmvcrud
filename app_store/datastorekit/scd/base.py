# datastorekit/scd/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datastorekit.adapters.base import DatastoreAdapter

class SCDHandler(ABC):
    def __init__(self, adapter: DatastoreAdapter, table_name: str, keys: Optional[Union[str, List[str]]] = None):
        self.adapter = adapter
        self.table_name = table_name
        self.keys = [keys] if isinstance(keys, str) else (keys or [])

    @abstractmethod
    def create(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]):
        pass

    @abstractmethod
    def read(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def update(self, data: Union[Dict[str, Any], List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None):
        pass

    @abstractmethod
    def delete(self, filters: Optional[Dict[str, Any]] = None):
        pass