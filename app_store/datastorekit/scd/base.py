# datastorekit/scd/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datastorekit.adapters.base import DatastoreAdapter

class SCDHandler(ABC):
    def __init__(self, adapter: DatastoreAdapter, table_name: str, keys: Optional[List[str]] = None):
        self.adapter = adapter
        self.table_name = table_name
        self.keys = keys or []

    @abstractmethod
    def create(self, records: List[Dict]) -> int:
        pass

    @abstractmethod
    def read(self, filters: Optional[Dict] = None) -> List[Dict]:
        pass

    @abstractmethod
    def update(self, updates: List[Dict]) -> int:
        pass

    @abstractmethod
    def delete(self, conditions: List[Dict]) -> int:
        pass