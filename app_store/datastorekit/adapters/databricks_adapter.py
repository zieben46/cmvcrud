# datastorekit/adapters/databricks_sql_adapter.py
from databricks.sql import connect
from databricks.sql.exc import DatabaseError
from sqlalchemy.sql import text
from typing import List, Dict, Any, Iterator, Optional, Tuple
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DatastoreOperationError, DuplicateKeyError, NullValueError
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class DBTable:
    table_name: str
    schema_name: str
    catalog_name: str

class DatabricksSQLAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection_config = {
            "server_hostname": profile.host,
            "http_path": profile.http_path,
            "access_token": profile.access_token,
            "catalog": profile.catalog or "hive_metastore",
            "schema": profile.schema or "default"
        }
        self.connection = None

    def _get_connection(self):
        """Establish a connection to Databricks if not already connected."""
        if self.connection is None:
            try:
                self.connection = connect(**self.connection_config)
            except Exception as e:
                logger.error(f"Failed to connect to Databricks: {e}")
                raise DatastoreOperationError(f"Failed to connect to Databricks: {e}")
        return self.connection

    def _handle_db_error(self, e: Exception, operation: str, table_name: str):
        """Handle Databricks-specific database errors."""
        if isinstance(e, DatabaseError):
            error_message = str(e).lower()
            if "duplicate key" in error_message or "unique constraint" in error_message:
                raise DuplicateKeyError(f"Duplicate key error during {operation} on {table_name}: {e}")
            if "not-null constraint" in error_message or "cannot be null" in error_message:
                raise NullValueError(f"Null value error during {operation} on {table_name}: {e}")
        raise DatastoreOperationError(f"Error during {operation} on {table_name}: {e}")

    def get_reflected_keys(self, table_name: str) -> List[str]:
        """Get primary key column names for the specified table."""
        try:
            dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
            query = f"""
                DESCRIBE TABLE EXTENDED {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
            """
            result = self.execute_sql(query)
            primary_keys = []
            for row in result:
                if row.get("col_name") == "# Partition Information" or row.get("col_name") == "":
                    break
                if row.get("comment") and "PRIMARY KEY" in row.get("comment").upper():
                    primary_keys.append(row.get("col_name"))
            return primary_keys or (self.profile.keys.split(",") if self.profile.keys else [])
        except Exception as e:
            logger.error(f"Failed to get reflected keys for table {table_name}: {e}")
            return []

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        """Create a table with the given schema."""
        try:
            dbtable = DBTable(table_name, schema_name or self.profile.schema or "default", self.profile.catalog or "hive_metastore")
            type_map = {
                "Integer": "INT",
                "String": "STRING",
                "Float": "FLOAT",
                "DateTime": "TIMESTAMP",
                "Boolean": "BOOLEAN"
            }
            columns = [f"{col_name} {type_map.get(col_type, 'STRING')}" for col_name, col_type in schema.items()]
            primary_keys = self.profile.keys.split(",") if self.profile.keys else []
            if primary_keys:
                columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            column_definitions = ", ".join(columns)
            sql = f"""
                CREATE TABLE IF NOT EXISTS {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                ({column_definitions})
            """
            self.execute_sql(sql)
            logger.info(f"Created table {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create table {table_name}: {e}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        """Get column names and types for the specified table."""
        try:
            dbtable = DBTable(table_name, schema_name or self.profile.schema or "default", self.profile.catalog or "hive_metastore")
            sql = f"""
                DESCRIBE TABLE {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
            """
            result = self.execute_sql(sql)
            columns = {}
            for row in result:
                if row.get("col_name") == "# Partition Information" or row.get("col_name") == "":
                    break
                columns[row["col_name"]] = row["data_type"].upper()
            return columns
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return {}

    def create(self, table_name: str, records: List[Dict]) -> int:
        """Insert records into the specified table using SQL."""
        dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
        try:
            if not records:
                return 0
            columns = list(records[0].keys())
            placeholders = ", ".join([f":{col}" for col in columns])
            columns_str = ", ".join(columns)
            sql = f"""
                INSERT INTO {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                ({columns_str}) VALUES ({placeholders})
            """
            rowcount = 0
            for record in records:
                result = self.execute_sql(sql, record)
                rowcount += 1  # Databricks SQL Connector doesn't return rowcount for INSERT, so we count records
            return rowcount
        except Exception as e:
            self._handle_db_error(e, "create", table_name)

    def read(self, table_name: str, filters: Optional[Dict] = None) -> List[Dict]:
        """Retrieve records from the specified table using SQL with optional filters."""
        dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
        try:
            where_clause = ""
            params = {}
            if filters:
                conditions = [f"{key} = :{key}" for key in filters.keys()]
                where_clause = f" WHERE {' AND '.join(conditions)}"
                params = filters
            sql = f"""
                SELECT * FROM {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                {where_clause}
            """
            return self.execute_sql(sql, params)
        except Exception as e:
            self._handle_db_error(e, "read", table_name)

    def select_chunks(self, table_name: str, filters: Optional[Dict] = None, chunk_size: int = 100000) -> Iterator[List[Dict]]:
        """Retrieve records in chunks using SQL with optional filters."""
        dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
        try:
            where_clause = ""
            params = {}
            if filters:
                conditions = [f"{key} = :{key}" for key in filters.keys()]
                where_clause = f" WHERE {' AND '.join(conditions)}"
                params = filters
            sql = f"""
                SELECT * FROM {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                {where_clause}
            """
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield [dict(row) for row in rows]
        except Exception as e:
            self._handle_db_error(e, "select_chunks", table_name)

    def update(self, table_name: str, updates: List[Dict]) -> int:
        """Update records in the specified table using SQL based on primary keys."""
        dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
        try:
            primary_keys = self.get_reflected_keys(table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for update operations.")

            total_updated = 0
            for update_dict in updates:
                if not all(pk in update_dict for pk in primary_keys):
                    raise ValueError(f"Update dictionary missing primary key(s): {primary_keys}")

                set_clause = ", ".join([f"{k} = :{k}" for k, v in update_dict.items() if k not in primary_keys])
                where_clause = " AND ".join([f"{pk} = :{pk}" for pk in primary_keys])
                sql = f"""
                    UPDATE {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                    SET {set_clause}
                    WHERE {where_clause}
                """
                result = self.execute_sql(sql, update_dict)
                total_updated += 1  # Databricks SQL Connector doesn't return rowcount for UPDATE
            return total_updated
        except Exception as e:
            self._handle_db_error(e, "update", table_name)

    def delete(self, table_name: str, conditions: List[Dict]) -> int:
        """Delete records from the specified table using SQL based on conditions."""
        dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
        try:
            if not conditions:
                raise ValueError("Conditions list cannot be empty.")

            total_deleted = 0
            for cond in conditions:
                if not cond:
                    raise ValueError("Condition dictionary cannot be empty.")
                where_clause = " AND ".join([f"{key} = :{key}" for key in cond.keys()])
                sql = f"""
                    DELETE FROM {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                    WHERE {where_clause}
                """
                result = self.execute_sql(sql, cond)
                total_deleted += 1  # Databricks SQL Connector doesn't return rowcount for DELETE
            return total_deleted
        except Exception as e:
            self._handle_db_error(e, "delete", table_name)

    def apply_changes(self, table_name: str, changes: List[Dict]) -> Tuple[List[Any], int, int, int]:
        """Apply create, update, and delete operations in one transaction using SQL."""
        dbtable = DBTable(table_name, self.profile.schema or "default", self.profile.catalog or "hive_metastore")
        try:
            inserts = []
            updates = []
            deletes = []

            for change in changes:
                if "operation" not in change:
                    raise ValueError("Each change dictionary must include an 'operation' key")
                operation = change["operation"]
                data = {k: v for k, v in change.items() if k != "operation"}

                if operation == "create":
                    inserts.append(data)
                elif operation == "update":
                    updates.append(data)
                elif operation == "delete":
                    deletes.append(data)
                else:
                    raise ValueError(f"Invalid operation: {operation}")

            inserted_ids = []
            inserted_count = 0
            updated_count = 0
            deleted_count = 0
            primary_keys = self.get_reflected_keys(table_name)

            conn = self._get_connection()
            with conn.cursor() as cursor:
                try:
                    # Perform inserts
                    if inserts:
                        columns = list(inserts[0].keys())
                        placeholders = ", ".join([f":{col}" for col in columns])
                        columns_str = ", ".join(columns)
                        sql = f"""
                            INSERT INTO {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                            ({columns_str}) VALUES ({placeholders})
                            RETURNING {', '.join(primary_keys)}
                        """
                        for record in inserts:
                            cursor.execute(sql, record)
                            inserted_ids.append(cursor.fetchone()[0] if len(primary_keys) == 1 else dict(zip(primary_keys, cursor.fetchone())))
                        inserted_count = len(inserts)

                    # Perform updates
                    if updates:
                        for update_dict in updates:
                            if not all(pk in update_dict for pk in primary_keys):
                                raise ValueError(f"Update dictionary missing primary key(s): {primary_keys}")
                            set_clause = ", ".join([f"{k} = :{k}" for k, v in update_dict.items() if k not in primary_keys])
                            where_clause = " AND ".join([f"{pk} = :{pk}" for pk in primary_keys])
                            sql = f"""
                                UPDATE {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                                SET {set_clause}
                                WHERE {where_clause}
                            """
                            cursor.execute(sql, update_dict)
                            updated_count += 1  # Approximate count

                    # Perform deletes
                    if deletes:
                        for cond in deletes:
                            if not cond:
                                raise ValueError("Condition dictionary cannot be empty.")
                            where_clause = " AND ".join([f"{key} = :{key}" for key in cond.keys()])
                            sql = f"""
                                DELETE FROM {dbtable.catalog_name}.{dbtable.schema_name}.{dbtable.table_name}
                                WHERE {where_clause}
                            """
                            cursor.execute(sql, cond)
                            deleted_count += 1  # Approximate count

                    conn.commit()
                    return inserted_ids, inserted_count, updated_count, deleted_count
                except Exception as e:
                    conn.rollback()
                    raise
        except Exception as e:
            self._handle_db_error(e, "apply_changes", table_name)

    def execute_sql(self, sql: str, parameters: Optional[Dict] = None) -> List[Dict]:
        """Execute a raw SQL query on Databricks."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, parameters or {})
                if cursor.description:  # Query returns results
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            self._handle_db_error(e, "execute_sql", "unknown")

    def list_tables(self, schema: str) -> List[str]:
        """List all tables in the specified schema."""
        try:
            catalog = self.profile.catalog or "hive_metastore"
            sql = f"""
                SHOW TABLES IN {catalog}.{schema}
            """
            result = self.execute_sql(sql)
            return [row["tableName"] for row in result]
        except Exception as e:
            logger.error(f"Failed to list tables in schema {schema}: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all tables in the specified schema."""
        try:
            metadata = {}
            for table_name in self.list_tables(schema):
                columns = self.get_table_columns(table_name, schema)
                primary_keys = self.get_reflected_keys(table_name)
                metadata[table_name] = {
                    "columns": columns,
                    "primary_keys": primary_keys
                }
            return metadata
        except Exception as e:
            logger.error(f"Failed to get table metadata for schema {schema}: {e}")
            return {}