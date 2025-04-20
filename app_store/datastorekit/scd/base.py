# datastorekit/scd/base.py
from abc import ABC, abstractmethod
from typing import List, Union

class SCDHandler(ABC):
    def __init__(self, adapter: DatastoreAdapter, table_name: str, keys: Union[str, List[str]]):
        self.adapter = adapter
        self.table_name = table_name
        self.keys = [keys] if isinstance(keys, str) else keys

    @abstractmethod
    def create(self, data: dict):
        pass

    @abstractmethod
    def read(self, filters: dict) -> list:
        pass

    @abstractmethod
    def update(self, data: dict, filters: dict):
        pass

    @abstractmethod
    def delete(self, filters: dict):
        pass