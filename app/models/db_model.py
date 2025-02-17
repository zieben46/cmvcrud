from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from app.config.db_configs import PostgresConfig
from app.models.base_model import CrudType, SCDType, DatabaseType
import os
from typing import Optional

# Import the strategies
from app.models.db_scd_strategies import SCDType0Strategy, SCDType1Strategy, SCDType2Strategy, SCDStrategyFactory

metadata = MetaData()

class DatabaseModel:
    """Handles direct interaction with the database (Data Model Layer)."""

    def __init__(self, database: DatabaseType, table_name: str):
        """Initialize the database model and determine the SCD type."""
        if database == DatabaseType.POSTGRES:
            config = PostgresConfig(os.getenv)
            url = config.get_url()
        else:
            raise ValueError(f"⚠️ Unsupported database type: {database}")

        self.table_name = table_name
        self.engine = create_engine(url)
        self.session = sessionmaker(bind=self.engine)()

        if not metadata.tables:
            metadata.reflect(bind=self.engine)

        self.table: Optional[Table] = metadata.tables.get(table_name)

        if self.table is None:
            raise ValueError(f"⚠️ Table `{table_name}` does not exist in the database.")

        self.scd_type = self._get_scd_type()
        self.strategy = SCDStrategyFactory.get_strategy(self.scd_type, self.table)

    def execute(self, operation: CrudType, **kwargs):
        """Executes CRUD operations using the selected strategy."""
        with self.engine.connect() as conn:
            if operation == CrudType.CREATE:
                return self.strategy.create(conn, **kwargs)
            elif operation == CrudType.READ:
                return self.strategy.read(conn)
            elif operation == CrudType.UPDATE:
                return self.strategy.update(conn, **kwargs)
            elif operation == CrudType.DELETE:
                return self.strategy.delete(conn, **kwargs)
            else:
                raise ValueError(f"Invalid CRUD operation: {operation}")
            

    def _get_scd_type(self) -> SCDType:
        """Fetch the SCD type for this table from `ALL_TABLES_DATA`."""
        # with self.engine.connect() as conn:
        #     query = select(metadata.tables["ALL_TABLES_DATA"].c.scd_type).where(
        #         metadata.tables["ALL_TABLES_DATA"].c.table_name == self.table_name
        #     )
        #     result = conn.execute(query).fetchone()

        # if result is None:
        #     raise ValueError(f"⚠️ No SCD type defined for `{self.table_name}` in `ALL_TABLES_DATA`.")
        return SCDType.SCDTYPE1
        return SCDType(result[0])