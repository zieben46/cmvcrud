from enum import Enum
from dataclasses import dataclass
import os
from typing import Optional, Callable
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
    connection_string: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    dbname: Optional[str] = None
    token: Optional[str] = None
    http_path: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None
    db_type: Optional[str] = None

    # Class-level default function for getting environment variables
    _getenv: Callable[[str, Optional[str]], str] = staticmethod(os.getenv)

    @classmethod
    def set_getenv(cls, custom_getenv: Callable[[str, Optional[str]], str]) -> None:
        """
        Set a custom function to retrieve environment variables, replacing os.getenv.
        
        Args:
            custom_getenv: A function that takes a key and optional default, returning a value.
        """
        cls._getenv = staticmethod(custom_getenv)

    @staticmethod
    def live_postgres(
        username: str = None,
        password: str = None,
        host: str = None,
        port: str = None,
        dbname: str = None
    ) -> 'DBConfig':
        """
        Create a live PostgreSQL configuration with credentials from environment variables.
        
        Args:
            username: Postgres username (default from PG_USERNAME via _getenv).
            password: Postgres password (default from PG_PASSWORD via _getenv).
            host: Postgres host (default from PG_HOST via _getenv, fallback to "localhost").
            port: Postgres port (default from PG_PORT via _getenv, fallback to "5432").
            dbname: Postgres database name (default from PG_DBNAME via _getenv).
        
        Returns:
            DBConfig instance for live PostgreSQL.
        """
        # Use the class-level _getenv function
        username = username or DBConfig._getenv("PG_USERNAME")
        password = password or DBConfig._getenv("PG_PASSWORD")
        host = host or DBConfig._getenv("PG_HOST", "localhost")
        port = port or DBConfig._getenv("PG_PORT", "5432")
        dbname = dbname or DBConfig._getenv("PG_DBNAME")

        if not all([username, password, dbname]):
            raise ValueError("Username, password, and dbname must be provided via arguments or environment variables")
        conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        return DBConfig(
            mode=DBMode.LIVE,
            connection_string=conn_str,
            host=host,
            port=port,
            username=username,
            password=password,
            dbname=dbname
            db_type="postgres"
        )

    @staticmethod
    def test_postgres(
        username: str = None,
        password: str = None,
        host: str = None,
        port: str = None,
        dbname: str = None
    ) -> 'DBConfig':
        """
        Create a test PostgreSQL configuration with credentials from environment variables or defaults.
        
        Args:
            username: Postgres username (default from PG_TEST_USERNAME via _getenv, fallback "admin").
            password: Postgres password (default from PG_TEST_PASSWORD via _getenv, fallback "password").
            host: Postgres host (default from PG_TEST_HOST via _getenv, fallback "localhost").
            port: Postgres port (default from PG_TEST_PORT via _getenv, fallback "5432").
            dbname: Postgres database name (default from PG_TEST_DBNAME via _getenv, fallback "dbadminkit_test").
        
        Returns:
            DBConfig instance for test PostgreSQL.
        """
        username = username or DBConfig._getenv("PG_TEST_USERNAME", "admin")
        password = password or DBConfig._getenv("PG_TEST_PASSWORD", "password")
        host = host or DBConfig._getenv("PG_TEST_HOST", "localhost")
        port = port or DBConfig._getenv("PG_TEST_PORT", "5432")
        dbname = dbname or DBConfig._getenv("PG_TEST_DBNAME", "dbadminkit_test")

        conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        return DBConfig(
            mode=DBMode.TEST,
            connection_string=conn_str,
            host=host,
            port=port,
            username=username,
            password=password,
            dbname=dbname
            db_type="postgres"
        )

    @staticmethod
    def live_databricks(
        host: str = None,
        token: str = None,
        http_path: str = None,
        catalog: str = None,
        schema: str = None
    ) -> 'DBConfig':
        """
        Create a live Databricks configuration with credentials from environment variables.
        
        Args:
            host: Databricks host (default from DATABRICKS_HOST via _getenv).
            token: Databricks token (default from DATABRICKS_TOKEN via _getenv).
            http_path: HTTP path (default from DATABRICKS_HTTP_PATH via _getenv).
            catalog: Databricks catalog (default from DATABRICKS_CATALOG via _getenv, fallback "hive_metastore").
            schema: Databricks schema (default from DATABRICKS_SCHEMA via _getenv, fallback "default").
        
        Returns:
            DBConfig instance for live Databricks.
        """
        host = host or DBConfig._getenv("DATABRICKS_HOST")
        token = token or DBConfig._getenv("DATABRICKS_TOKEN")
        http_path = http_path or DBConfig._getenv("DATABRICKS_HTTP_PATH")
        catalog = catalog or DBConfig._getenv("DATABRICKS_CATALOG", "hive_metastore")
        schema = schema or DBConfig._getenv("DATABRICKS_SCHEMA", "default")

        if not all([host, token, http_path]):
            raise ValueError("Host, token, and http_path must be provided via arguments or environment variables")
        conn_str = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
        return DBConfig(
            mode=DBMode.LIVE,
            host=host,
            token=token,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            connection_string=conn_str
            db_type="databricks"
        )

    @staticmethod
    def test_databricks(
        host: str = None,
        token: str = None,
        http_path: str = None,
        catalog: str = None,
        schema: str = None
    ) -> 'DBConfig':
        """
        Create a test Databricks configuration with credentials from environment variables or defaults.
        
        Args:
            host: Databricks host (default from DATABRICKS_TEST_HOST via _getenv, fallback "localhost").
            token: Databricks token (default from DATABRICKS_TEST_TOKEN via _getenv, fallback "test_token").
            http_path: HTTP path (default from DATABRICKS_TEST_HTTP_PATH via _getenv, fallback "/test").
            catalog: Databricks catalog (default from DATABRICKS_TEST_CATALOG via _getenv, fallback "hive_metastore").
            schema: Databricks schema (default from DATABRICKS_TEST_SCHEMA via _getenv, fallback "default").
        
        Returns:
            DBConfig instance for test Databricks.
        """
        host = host or DBConfig._getenv("DATABRICKS_TEST_HOST", "localhost")
        token = token or DBConfig._getenv("DATABRICKS_TEST_TOKEN", "test_token")
        http_path = http_path or DBConfig._getenv("DATABRICKS_TEST_HTTP_PATH", "/test")
        catalog = catalog or DBConfig._getenv("DATABRICKS_TEST_CATALOG", "hive_metastore")
        schema = schema or DBConfig._getenv("DATABRICKS_TEST_SCHEMA", "default")

        conn_str = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
        return DBConfig(
            mode=DBMode.TEST,
            host=host,
            token=token,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            connection_string=conn_str
            db_type="databricks"
        )

    @staticmethod
    def in_memory() -> 'DBConfig':
        """
        Create an in-memory SQLite configuration.
        
        Returns:
            DBConfig instance for in-memory SQLite.
        """
        return DBConfig(mode=DBMode.IN_MEMORY, connection_string="sqlite:///:memory:", db_type="sqlite")
    




from dbadminkit.core.config import DBConfig

# Uses os.getenv by default
config = DBConfig.live_databricks()
print(config.host)  # Value from DATABRICKS_HOST env var or raises ValueError if missing


# Set custom_getenv to a lambda that always returns "TEST"
DBConfig.set_getenv(lambda key, default=None: "TEST")

# Create a config instance
config = DBConfig.live_databricks()
print(config.host)        # "TEST"
print(config.token)       # "TEST"
print(config.http_path)   # "TEST"
print(config.catalog)     # "TEST"
print(config.schema)      # "TEST"
print(config.connection_string)  # "databricks://token:TEST@TEST?http_path=TEST&catalog=TEST&schema=TEST"