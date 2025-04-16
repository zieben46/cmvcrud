import logging
from typing import Dict, Any, List, Optional
from sqlalchemy import Table
from sqlalchemy.orm import Session
from sqlalchemy.sql import select, insert, update, delete

logger = logging.getLogger(__name__)

class SCDHandler:
    def __init__(self, table: Table, primary_keys: List[str]):
        """Initialize with a SQLAlchemy Table and list of primary key columns."""
        self.table = table
        self.primary_keys = primary_keys
        self.engine = table.bind  # Engine from table's connection

        # Validate primary keys
        if not primary_keys:
            raise ValueError("At least one primary key must be specified")
        for pk in primary_keys:
            if pk not in self.table.c:
                raise ValueError(f"Primary key {pk} not found in table {self.table.name}")

    def create(self, session: Session, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new record and return its primary key(s)."""
        try:
            # Insert data
            result = session.execute(
                insert(self.table).values(**data)
            )
            session.commit()
            # Fetch inserted primary key(s)
            pk_values = {pk: data.get(pk) for pk in self.primary_keys}
            return pk_values if all(pk_values.values()) else None
        except Exception as e:
            logger.error(f"Create failed for {self.table.name}: {e}")
            session.rollback()
            raise

    def read(self, session: Session, key_values: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Read a record by primary key(s)."""
        try:
            # Build WHERE clause for primary keys
            conditions = [
                getattr(self.table.c, pk) == key_values.get(pk)
                for pk in self.primary_keys
            ]
            if not all(key_values.get(pk) for pk in self.primary_keys):
                raise ValueError("All primary key values must be provided")

            # Select record
            stmt = select(self.table).where(*conditions)
            result = session.execute(stmt).fetchone()
            if result:
                return dict(result._mapping)
            return None
        except Exception as e:
            logger.error(f"Read failed for {self.table.name}, keys={key_values}: {e}")
            raise

    def update(self, session: Session, key_values: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Update a record by primary key(s)."""
        try:
            # Build WHERE clause for primary keys
            conditions = [
                getattr(self.table.c, pk) == key_values.get(pk)
                for pk in self.primary_keys
            ]
            if not all(key_values.get(pk) for pk in self.primary_keys):
                raise ValueError("All primary key values must be provided")

            # Update data
            stmt = update(self.table).where(*conditions).values(**data)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount > 0
        except Exception as e:
            logger.error(f"Update failed for {self.table.name}, keys={key_values}: {e}")
            session.rollback()
            raise

    def delete(self, session: Session, key_values: Dict[str, Any]) -> bool:
        """Delete a record by primary key(s)."""
        try:
            # Build WHERE clause for primary keys
            conditions = [
                getattr(self.table.c, pk) == key_values.get(pk)
                for pk in self.primary_keys
            ]
            if not all(key_values.get(pk) for pk in self.primary_keys):
                raise ValueError("All primary key values must be provided")

            # Delete record
            stmt = delete(self.table).where(*conditions)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount > 0
        except Exception as e:
            logger.error(f"Delete failed for {self.table.name}, keys={key_values}: {e}")
            session.rollback()
            raise

def transfer_initial_load(self, databricks_table: str, total_records: int = 20000000):
    """Transfer full table from Databricks to PostgreSQL."""
    query = f"SELECT * FROM {databricks_table} ORDER BY {self.key}"
    total_chunks = math.ceil(total_records / self._chunk_size)

    try:
        for i, chunk_df in enumerate(self.read_databricks_chunk(query)):
            if chunk_df.empty:
                break
            cdc_records = [
                {"operation": "INSERT", "data": row, "timestamp": datetime.utcnow()}
                for _, row in chunk_df.iterrows()
            ]
            processed = self.process_cdc_logs(cdc_records)

            with self.pg_engine.begin() as conn:
                conn.execute(
                    self.history_table.insert().values(
                        version=0,
                        timestamp=datetime.utcnow(),
                        operation="INITIAL_LOAD",
                        record_count=processed
                    )
                )
            logger.info(f"Processed chunk {i+1}/{total_chunks}: {processed} records")
        logger.info(f"Initial load of {total_records} records completed")
    except Exception as e:
        logger.error(f"Initial load failed: {e}")
        raise


def read_databricks_chunk(self, query: str, params: Dict = None) -> iter:
    try:
        cursor = self.db_conn.cursor()
        cursor.execute(query, params or {})
        while True:
            chunk_size = self._cdf_chunk_size if "table_changes" in query else self._chunk_size
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            chunk = pd.DataFrame([dict(zip([col[0] for col in cursor.description], row)) for row in rows])
            yield chunk
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to read Databricks chunk: {e}")
        raise