
from abc import ABC, abstractmethod
import pandas as pd
from enum import Enum
from typing import List, Dict, Optional
from sqlalchemy import create_engine


class ModelType(Enum):
    DATABASE = "db"
    CSV = "csv"
    TEST = "www.lite"

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


