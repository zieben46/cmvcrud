import logging
from dataclasses import dataclass
from typing import Optional
from dotenv import dotenv_values

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class DatabaseProfile:
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

    @staticmethod
    def postgres(env_file: str) -> 'DatabaseProfile':
        """Create a PostgreSQL configuration by loading variables from a specified .env file.

        Args:
            env_file: Path to the .env file (e.g., '.env.live1') containing configuration.

        Returns:
            DatabaseProfile instance for PostgreSQL.

        Raises:
            ValueError: If required variables (PG_USERNAME, PG_PASSWORD, PG_DBNAME) are missing.
        """
        env_vars = dotenv_values(env_file)
        
        username = env_vars.get("PG_USERNAME")
        password = env_vars.get("PG_PASSWORD")
        host = env_vars.get("PG_HOST", "localhost")
        port = env_vars.get("PG_PORT", "5432")
        dbname = env_vars.get("PG_DBNAME")

        if not all([username, password, dbname]):
            raise ValueError(f"PG_USERNAME, PG_PASSWORD, and PG_DBNAME must be provided in {env_file}")

        conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        return DatabaseProfile(
            connection_string=conn_str,
            host=host,
            port=port,
            username=username,
            password=password,
            dbname=dbname,
            db_type="postgres"
        )

    @staticmethod
    def databricks(env_file: str) -> 'DatabaseProfile':
        """Create a Databricks configuration by loading variables from a specified .env file.

        Args:
            env_file: Path to the .env file (e.g., '.env.live') containing configuration.

        Returns:
            DatabaseProfile instance for Databricks.

        Raises:
            ValueError: If required variables (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH) are missing.
        """
        env_vars = dotenv_values(env_file)
        host = env_vars.get("DATABRICKS_HOST")
        token = env_vars.get("DATABRICKS_TOKEN")
        http_path = env_vars.get("DATABRICKS_HTTP_PATH")
        catalog = env_vars.get("DATABRICKS_CATALOG", "hive_metastore")
        schema = env_vars.get("DATABRICKS_SCHEMA", "default")

        if not all([host, token, http_path]):
            raise ValueError(f"DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be provided in {env_file}")

        conn_str = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
        return DatabaseProfile(
            host=host,
            token=token,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            connection_string=conn_str,
            db_type="databricks"
        )

    @staticmethod
    def in_memory() -> 'DatabaseProfile':
        """Create an in-memory SQLite configuration."""
        return DatabaseProfile(connection_string="sqlite:///:memory:", db_type="sqlite")