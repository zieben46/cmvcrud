
from abc import ABC, abstractmethod
import pandas as pd
from enum import Enum
from typing import List, Dict
from sqlalchemy import create_engine

from model.db_model import DatabaseModel
from model.csv_model import CSVModel

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

    def __init__(self):
        self.df = pd.DataFrame()  # Initialize an empty DataFrame

    @abstractmethod
    def execute(self, operation: CrudType, **kwargs) -> None:
        """Executes the given CRUD operation based on SCD type."""
        pass

# class Controller:
#     def __init__(self, mode, db_engine=None, csv_path=None):
#         if mode == "db":
#             if not db_engine:
#                 raise ValueError("⚠️ `db_engine` is required for database mode.")
#             self.handler = DBController(db_engine)
#         elif mode == "csv":
#             if not csv_path:
#                 raise ValueError("⚠️ `csv_path` is required for CSV mode.")
#             self.handler = CSVController(csv_path)
#         else:
#             raise ValueError("⚠️ Invalid mode. Choose 'db' or 'csv'.")

    # def read(self, identifier=None):
    #     return self.handler.read(identifier) if identifier else self.handler.read()

    # def list_tables(self):
    #     if isinstance(self.handler, DBController):
    #         return self.handler.list_tables()
    #     raise ValueError("⚠️ list_tables() is only available in database mode.")
