# datastorekit/profile.py
from dataclasses import dataclass
from typing import Optional
from dotenv import dotenv_values
import logging

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
            schema=env_vars.get("SCHEMA", "safe_user"),
            db_type="postgres"
        )

    @staticmethod
    def databricks(env_file: str) -> 'DatabaseProfile':
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
            connection_string=conn_str,
            host=host,
            token=token,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            db_type="databricks"
        )

    @staticmethod
    def mongodb(env_file: str) -> 'DatabaseProfile':
        env_vars = dotenv_values(env_file)
        host = env_vars.get("MONGO_HOST", "localhost")
        port = env_vars.get("MONGO_PORT", "27017")
        username = env_vars.get("MONGO_USERNAME")
        password = env_vars.get("MONGO_PASSWORD")
        dbname = env_vars.get("MONGO_DBNAME")
        if not dbname:
            raise ValueError(f"MONGO_DBNAME must be provided in {env_file}")
        auth = f"{username}:{password}@" if username and password else ""
        conn_str = f"mongodb://{auth}{host}:{port}/{dbname}"
        return DatabaseProfile(
            connection_string=conn_str,
            host=host,
            port=port,
            username=username,
            password=password,
            dbname=dbname,
            schema="default",
            db_type="mongodb"
        )

    @staticmethod
    def sqlite() -> 'DatabaseProfile':
        return DatabaseProfile(
            connection_string="sqlite:///:memory:",
            dbname="test_db",
            schema="default",
            db_type="sqlite"
        )

    @staticmethod
    def csv(env_file: str) -> 'DatabaseProfile':
        env_vars = dotenv_values(env_file)
        base_dir = env_vars.get("CSV_BASE_DIR", "./data")
        dbname = env_vars.get("CSV_DBNAME", "csv_db")
        return DatabaseProfile(
            connection_string=base_dir,
            dbname=dbname,
            schema="default",
            db_type="csv"
        )

    @staticmethod
    def inmemory(env_file: str) -> 'DatabaseProfile':
        env_vars = dotenv_values(env_file)
        dbname = env_vars.get("INMEMORY_DBNAME", "inmemory_db")
        return DatabaseProfile(
            connection_string="inmemory://",
            dbname=dbname,
            schema="default",
            db_type="inmemory"
        )