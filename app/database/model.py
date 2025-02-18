from typing import List, Dict, Optional
from abc import ABC, abstractmethod  # ✅ Import abstract base class tools
from app.config.enums import CrudType  # ✅ Import CrudType Enum


class BaseModel(ABC):
    """Abstract interface enforcing CRUD operations for any data source."""

    @abstractmethod
    def execute(self, operation: CrudType, data: List[Dict]) -> Optional[List[Dict]]:
        """Executes the given CRUD operation based on SCD type."""
        pass