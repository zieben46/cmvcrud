from abc import ABC, abstractmethod
from typing import Dict, Any
from app_test.dbadminkit.core.crud_types import CRUDOperation

class TableInterface(ABC):
    @abstractmethod
    def get_scdtype(self) -> str:
        pass

    @abstractmethod
    def perform_crud(self, crud_type: CRUDOperation, data: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def get_db_table_schema(self) -> Dict[str, Any]:
        pass
    