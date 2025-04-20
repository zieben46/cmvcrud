# datastorekit/acceptance_tests/drivers/database_driver.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class DatabaseDriver(ABC):
    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, Any]):
        pass

    @abstractmethod
    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def update(self, table_info: Dict[str, str], data: List[Dict[str, Any]], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def sync_to(self, source_table_info: Dict[str, str], target_driver: 'DatabaseDriver', target_table: str, method: str):
        pass