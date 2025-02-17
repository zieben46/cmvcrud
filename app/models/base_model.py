
from abc import ABC, abstractmethod
import pandas as pd
from enum import Enum
from typing import List, Dict
from sqlalchemy import create_engine

class ModelType(Enum):
    DATABASE = "db"
    CSV = "csv"
    TEST = "www.lite"

class ViewType(Enum):
    UI = "User Interface"
    CLI = "Command Line Interface"

class DatabaseType(Enum):
    POSTGRES = "postgres"

class CrudType(Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"

class SCDType(Enum):
    SCDTYPE0 = "scd_type0"
    SCDTYPE1 = "scd_type1"
    SCDTYPE2 = "scd_type2"

class BaseModel(ABC):
    """Abstract interface enforcing CRUD operations for any data source."""

    @abstractmethod
    def execute(self, operation: CrudType, data: List[Dict]) -> None:
        """Executes the given CRUD operation based on SCD type."""
        pass
