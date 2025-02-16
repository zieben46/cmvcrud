from sqlalchemy import Table, create_engine, inspect
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import create_engine, Table, MetaData
import pandas as pd
from model.base_model import CrudType, SCDType, DatabaseType, BaseModel
import os
from typing import Optional

from sqlalchemy.exc import OperationalError, NoSuchModuleError

from configs.db_configs import PostgresConfig

metadata = MetaData()

class DatabaseModel (BaseModel):
    """Handles direct interaction with the database (Data Model Layer)."""

    def __init__(self, database: DatabaseType, table_name: str):

        if database == DatabaseType.POSTGRES:
            config = PostgresConfig(os.getenv)
            url = config.get_url()
        else:
            raise ValueError(f"⚠️ Unsupported database type: {database}")
        
        self.engine = create_engine(url)
        self.session = sessionmaker(bind=self.engine)()

        if not metadata.tables:
            metadata.reflect(bind=self.engine)

        self.table: Optional[Table] = metadata.tables.get(table_name)

        if self.table is None:
            print("⚠️ Available tables:", metadata.tables.keys())  # Debugging
            raise ValueError(f"⚠️ Table `{table_name}` does not exist in the database.")

            if not self.table:
                raise ValueError(f"⚠️ Table `{table_name}` does not exist.")

    def execute(self, operation: CrudType, scd_type: SCDType, **kwargs):
        """Executes a CRUD operation on the specific table."""
        if operation == CrudType.CREATE:
            self._create(scd_type, **kwargs)
        elif operation == CrudType.READ:
            return self._read(scd_type)
        elif operation == CrudType.UPDATE:
            self._update(scd_type, **kwargs)
        elif operation == CrudType.DELETE:
            self._delete(scd_type, **kwargs)
        else:
            raise ValueError(f"⚠️ Invalid CRUD operation: {operation}")

    def _create(self, scd_type: SCDType, **kwargs):
        """Handles CREATE operation based on SCD type."""
        with self.engine.connect() as conn:
            if scd_type == SCDType.SCDTYPE1:
                conn.execute(self.table.insert().values(**kwargs))
            elif scd_type == SCDType.SCDTYPE2:
                kwargs["on_time"] = pd.Timestamp.now()
                conn.execute(self.table.insert().values(**kwargs))
            else:
                raise ValueError(f"⚠️ Unsupported SCD type: {scd_type}")
            conn.commit()
        print(f"✅ Inserted record into `{self.table_name}` with SCD type `{scd_type}`.")

    def _read(self, scd_type: SCDType):
        """Reads data based on SCD type."""
        with self.engine.connect() as conn:
            result = conn.execute(self.table.select()).fetchall()
        df = pd.DataFrame([dict(row._mapping) for row in result])

        if scd_type == SCDType.SCDTYPE2 and "off_time" in df.columns:
            df = df[df["off_time"].isna()]
        return df

    def _update(self, scd_type: SCDType, **kwargs):
        """Updates a record based on SCD type."""
        with self.engine.connect() as conn:
            if scd_type == SCDType.SCDTYPE1:
                update_query = self.table.update().values(**kwargs)
            elif scd_type == SCDType.SCDTYPE2:
                conn.execute(self.table.update().where(self.table.c.id == kwargs["id"]).values(off_time=pd.Timestamp.now()))
                conn.execute(self.table.insert().values(**kwargs))
                conn.commit()
                return
            result = conn.execute(update_query)
            conn.commit()
        print(f"🔄 Updated record in `{self.table_name}` with SCD type `{scd_type}`.")

    def _delete(self, scd_type: SCDType, **kwargs):
        """Handles DELETE operations for different SCD types."""
        with self.engine.connect() as conn:
            if scd_type == SCDType.SCDTYPE2:
                conn.execute(self.table.update().where(self.table.c.id == kwargs["id"]).values(off_time=pd.Timestamp.now()))
                conn.commit()
                print(f"🗑️ Soft deleted `{kwargs['id']}` (marked as inactive) in `{self.table_name}`.")
                return
            
            delete_query = self.table.delete().where(self.table.c.id == kwargs["id"])
            result = conn.execute(delete_query)
            conn.commit()

        print(f"🗑️ Deleted `{kwargs['id']}` from `{self.table_name}`.")

    def _test_connection(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except OperationalError:
            raise ConnectionError("❌ Unable to connect to the database. Check credentials and host.")
