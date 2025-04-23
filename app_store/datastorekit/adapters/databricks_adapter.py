from databricks.sql import connect
from sqlalchemy import Table, MetaData, select, insert, update, delete, inspect
from sqlalchemy.sql import and_, text
from sqlalchemy.exc import OperationalError
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.connection import DatastoreConnection
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class DatabricksAdapter(DatastoreAdapter):
    def __init__(self, profile: DatastoreProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.engine = self.connection.get_engine()
        self.session_factory = self.connection.get_session_factory()
        self.metadata = MetaData()
        # Initialize Databricks SQL Connector
        self.db_conn = connect(
            server_hostname=profile.server_hostname,
            http_path=profile.http_path,
            access_token=profile.access_token,
            catalog=profile.catalog,
            schema=profile.schema
        )

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys match the table's primary keys."""
        try:
            # Use Databricks SQL Connector to get primary keys
            cursor = self.db_conn.cursor()
            cursor.execute(f"DESCRIBE TABLE {self.profile.catalog}.{self.profile.schema}.{table_name}")
            columns = cursor.fetchall()
            cursor.close()
            db_keys = [col[0] for col in columns if "PRIMARY KEY" in col[1]]
            if not db_keys:
                return  # No primary keys, use table_info.keys
            if set(db_keys) != set(table_info_keys):
                raise ValueError(
                    f"Table {table_name} primary keys {db_keys} do not match TableInfo keys {table_info_keys}"
                )
        except Exception as e:
            logger.warning(f"Skipping key validation for Databricks table {table_name}: {e}")
            return

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=f"{self.profile.catalog}.{self.profile.schema}")
        try:
            with self.engine.connect() as conn:
                conn.execute(insert(table), data)
                conn.commit()
        except OperationalError as e:
            logger.error(f"Insert failed for {table_name}: {e}")
            raise

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=f"{self.profile.catalog}.{self.profile.schema}")
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchall()
                return [dict(row._mapping) for row in result]
        except OperationalError as e:
            logger.error(f"Select failed for {table_name}: {e}")
            raise

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=f"{self.profile.catalog}.{self.profile.schema}")
        query = select(table)
        for key, value in filters.items():
            query = query.where(table.c[key] == value)
        try:
            with self.engine.connect() as conn:
                result = conn.execution_options(yield_per=chunk_size).execute(query)
                for partition in result.partitions():
                    yield [dict(row._mapping) for row in partition]
        except OperationalError as e:
            logger.error(f"Select chunks failed for {table_name}: {e}")
            raise

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=f"{self.profile.catalog}.{self.profile.schema}")
        try:
            with self.engine.connect() as conn:
                for update_data in data:
                    query = update(table).where(
                        and_(*(table.c[key] == filters[key] for key in filters))
                    ).values(**update_data)
                    conn.execute(query)
                conn.commit()
        except OperationalError as e:
            logger.error(f"Update failed for {table_name}: {e}")
            raise

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(","))
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=f"{self.profile.catalog}.{self.profile.schema}")
        query = delete(table).where(
            and_(*(table.c[key] == filters[key] for key in filters))
        )
        try:
            with self.engine.connect() as conn:
                conn.execute(query)
                conn.commit()
        except OperationalError as e:
            logger.error(f"Delete failed for {table_name}: {e}")
            raise

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query using Databricks SQL Connector."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, parameters or {})
            if cursor.description:  # Check if query returns rows
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return results
            cursor.close()
            return []
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            raise

    def list_tables(self, schema: str) -> List[str]:
        """List tables in the given schema using Databricks SQL Connector."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(f"SHOW TABLES IN {self.profile.catalog}.{schema}")
            tables = [row[1] for row in cursor.fetchall()]  # Table name is in second column
            cursor.close()
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables for schema {schema}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in the schema using Databricks SQL Connector."""
        try:
            cursor = self.db_conn.cursor()
            metadata = {}
            cursor.execute(f"SHOW TABLES IN {self.profile.catalog}.{schema}")
            table_names = [row[1] for row in cursor.fetchall()]
            for table_name in table_names:
                cursor.execute(f"DESCRIBE TABLE {self.profile.catalog}.{schema}.{table_name}")
                columns = cursor.fetchall()
                col_dict = {col[0]: col[1] for col in columns if col[0]}  # Column name: type
                # Primary keys (Databricks may not always expose via DESCRIBE)
                pk_columns = [col[0] for col in columns if "PRIMARY KEY" in col[1]]
                metadata[table_name] = {
                    "columns": col_dict,
                    "primary_keys": pk_columns
                }
            cursor.close()
            return metadata
        except Exception as e:
            logger.error(f"Failed to get table metadata for schema {schema}: {e}")
            return {}

    def __del__(self):
        """Ensure Databricks connection is closed."""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()