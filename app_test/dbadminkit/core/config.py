from enum import Enum
from dataclasses import dataclass
import os
from dotenv import load_dotenv

# Load environment variables from .env file (optional)
load_dotenv()

class DBMode(Enum):
    LIVE = "live"
    TEST = "test"
    IN_MEMORY = "in_memory"

@dataclass
class DBConfig:
    mode: DBMode
    connection_string: str = None

    @staticmethod
    def live_postgres(
        username: str = os.getenv("PG_USERNAME"),
        password: str = os.getenv("PG_PASSWORD"),
        host: str = os.getenv("PG_HOST", "localhost"),
        port: str = os.getenv("PG_PORT", "5432"),
        dbname: str = os.getenv("PG_DBNAME")
    ) -> 'DBConfig':
        """
        Create a live PostgreSQL configuration with credentials from environment variables.
        
        Args:
            username: Postgres username (default from PG_USERNAME env var).
            password: Postgres password (default from PG_PASSWORD env var).
            host: Postgres host (default from PG_HOST, fallback to "localhost").
            port: Postgres port (default from PG_PORT, fallback to "5432").
            dbname: Postgres database name (default from PG_DBNAME env var).
        
        Returns:
            DBConfig instance for live PostgreSQL.
        """
        if not all([username, password, dbname]):
            raise ValueError("Username, password, and dbname must be provided via arguments or environment variables")
        conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        return DBConfig(mode=DBMode.LIVE, connection_string=conn_str)

    @staticmethod
    def test_postgres(
        username: str = os.getenv("PG_TEST_USERNAME"),
        password: str = os.getenv("PG_TEST_PASSWORD"),
        host: str = os.getenv("PG_TEST_HOST", "localhost"),
        port: str = os.getenv("PG_TEST_PORT", "5432"),
        dbname: str = os.getenv("PG_TEST_DBNAME")
    ) -> 'DBConfig':
        """
        Create a test PostgreSQL configuration with credentials from environment variables.
        
        Args:
            Same as live_postgres, but with test-specific env vars (PG_TEST_*).
        
        Returns:
            DBConfig instance for test PostgreSQL.
        """
        if not all([username, password, dbname]):
            raise ValueError("Username, password, and dbname must be provided via arguments or environment variables")
        conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        return DBConfig(mode=DBMode.TEST, connection_string=conn_str)

    @staticmethod
    def live_databricks(
        host: str = os.getenv("DATABRICKS_HOST"),
        token: str = os.getenv("DATABRICKS_TOKEN"),
        http_path: str = os.getenv("DATABRICKS_HTTP_PATH"),
        catalog: str = os.getenv("DATABRICKS_CATALOG", "hive_metastore"),
        schema: str = os.getenv("DATABRICKS_SCHEMA", "default")
    ) -> 'DBConfig':
        """
        Create a live Databricks configuration with credentials from environment variables.
        
        Args:
            host: Databricks host (default from DATABRICKS_HOST env var).
            token: Databricks token (default from DATABRICKS_TOKEN env var).
            http_path: HTTP path (default from DATABRICKS_HTTP_PATH env var).
            catalog: Databricks catalog (default "hive_metastore").
            schema: Databricks schema (default "default").
        
        Returns:
            DBConfig instance for live Databricks.
        """
        if not all([host, token, http_path]):
            raise ValueError("Host, token, and http_path must be provided via arguments or environment variables")
        conn_str = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
        return DBConfig(mode=DBMode.LIVE, connection_string=conn_str)

    @staticmethod
    def test_databricks(
        host: str = os.getenv("DATABRICKS_TEST_HOST"),
        token: str = os.getenv("DATABRICKS_TEST_TOKEN"),
        http_path: str = os.getenv("DATABRICKS_TEST_HTTP_PATH"),
        catalog: str = os.getenv("DATABRICKS_TEST_CATALOG", "hive_metastore"),
        schema: str = os.getenv("DATABRICKS_TEST_SCHEMA", "default")
    ) -> 'DBConfig':
        """
        Create a test Databricks configuration with credentials from environment variables.
        
        Args:
            Same as live_databricks, but with test-specific env vars (DATABRICKS_TEST_*).
        
        Returns:
            DBConfig instance for test Databricks.
        """
        if not all([host, token, http_path]):
            raise ValueError("Host, token, and http_path must be provided via arguments or environment variables")
        conn_str = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
        return DBConfig(mode=DBMode.TEST, connection_string=conn_str)

    @staticmethod
    def in_memory() -> 'DBConfig':
        """
        Create an in-memory SQLite configuration.
        
        Returns:
            DBConfig instance for in-memory SQLite.
        """
        return DBConfig(mode=DBMode.IN_MEMORY)