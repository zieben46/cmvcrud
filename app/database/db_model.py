from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Optional
from enum import Enum
from pydantic import create_model

class CrudType(Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"

class ReflectedTableModel:
    def __init__(self, engine, table_specs: Dict[str, Any]) -> None:
        self.engine = engine
        self.table_name = table_specs['table_name']  # Extract table_name from table_specs
        self.is_locked = table_specs.get('is_locked', False)
        self.locked_by = table_specs.get('locked_by', None)
        self.scd_type = table_specs.get('scd_type', 0)
        self.created_at = table_specs.get('created_at', None)
        self.updated_at = table_specs.get('updated_at', None)
        
        # Reflect only the table from table_specs
        self.Base = automap_base()
        try:
            self.Base.prepare(
                engine,
                reflect=True,
                only=[self.table_name]
            )
        except Exception as e:
            raise RuntimeError(f"Failed to reflect table '{self.table_name}': {str(e)}")
        
        try:
            self.table_class = getattr(self.Base.classes, self.table_name)
        except AttributeError:
            raise ValueError(f"Table '{self.table_name}' not found in the database")

    def generate_table_model(self) -> type:
        fields = {col.name: (Optional[col.type.python_type], None) for col in self.table_class.__table__.columns}
        return create_model(f"{self.table_name.capitalize()}Model", **fields)

    def execute(self, session: Session, operation: CrudType, data: Optional[List[Dict]] = None) -> Any:
        try:
            if operation == CrudType.CREATE:
                return self._create(session, data)
            elif operation == CrudType.READ:
                return self._read(session)
            elif operation == CrudType.UPDATE:
                return self._update(session, data)
            elif operation == CrudType.DELETE:
                return self._delete(session, data)
            else:
                raise ValueError(f"Invalid CRUD operation: {operation}")
        except ValueError as e:
            raise
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Failed to execute {operation.value}: {str(e)}")
        finally:
            session.commit()
