# datastorekit/scd/base.py
from abc import ABC, abstractmethod

class SCDHandler(ABC):
    def __init__(self, adapter: DatastoreAdapter, table_name: str, key: str):
        self.adapter = adapter
        self.table_name = table_name
        self.key = key

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