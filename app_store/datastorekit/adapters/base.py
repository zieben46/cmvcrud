# datastorekit/adapters/base.py
from abc import ABC, abstractmethod

class DatastoreAdapter(ABC):
    @abstractmethod
    def insert(self, table_name: str, data: dict):
        pass

    @abstractmethod
    def select(self, table_name: str, filters: dict) -> list:
        pass

    @abstractmethod
    def update(self, table_name: str, data: dict, filters: dict):
        pass

    @abstractmethod
    def delete(self, table_name: str, filters: dict):
        pass