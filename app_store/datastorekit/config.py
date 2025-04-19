# datastorekit/config.py
import os
from typing import Dict, List
from dotenv import dotenv_values
import logging
from pathlib import Path
from datastorekit.profile import DatabaseProfile
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Config:
    """Configuration utilities and constants for the datastorekit package."""

    # Supported datastore types
    SUPPORTED_DB_TYPES = ["postgres", "databricks", "mongodb", "sqlite"]

    # Default schema if not specified
    DEFAULT_SCHEMA = "default"

    # Default .env folder path relative to project root
    ENV_FOLDER = ".env"

    # Required fields for each datastore type
    REQUIRED_ENV_FIELDS = {
        "postgres": ["PG_USERNAME", "PG_PASSWORD", "PG_DBNAME"],
        "databricks": ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"],
        "mongodb": ["MONGO_DBNAME"],
        "sqlite": []
    }

    @staticmethod
    def validate_env_file(env_path: str, db_type: str) -> bool:
        """Validate that an .env file contains required fields for the specified datastore type."""
        if db_type not in Config.SUPPORTED_DB_TYPES:
            raise ValueError(f"Unsupported db_type: {db_type}")
        if db_type == "sqlite":
            return True
        env_vars = dotenv_values(env_path)
        required_fields = Config.REQUIRED_ENV_FIELDS[db_type]
        missing_fields = [field for field in required_fields if field not in env_vars or not env_vars[field]]
        if missing_fields:
            logger.error(f"Missing required fields in {env_path}: {missing_fields}")
            return False
        return True

    @staticmethod
    def list_env_files(env_folder: str = ENV_FOLDER) -> List[str]:
        """List all .env files in the specified folder."""
        env_folder_path = Path(env_folder)
        if not env_folder_path.exists():
            raise FileNotFoundError(f"Environment folder not found: {env_folder}")
        return [str(env_folder_path / f) for f in env_folder_path.glob("*.env")]

    @staticmethod
    def get_default_schema(db_type: str) -> str:
        """Get the default schema for a given datastore type."""
        if db_type not in Config.SUPPORTED_DB_TYPES:
            raise ValueError(f"Unsupported db_type: {db_type}")
        return "safe_user" if db_type == "postgres" else Config.DEFAULT_SCHEMA

    @staticmethod
    def configure_logging(level: str = "INFO"):
        """Configure global logging settings."""
        logging.getLogger().setLevel(getattr(logging, level.upper(), logging.INFO))
        logger.info(f"Logging level set to {level}")

    @staticmethod
    def create_test_profile() -> DatabaseProfile:
        """Create a test DatabaseProfile for SQLite in-memory database."""
        return DatabaseProfile.sqlite()

    @staticmethod
    def setup_test_table(table_name: str, engine):
        """Create a test table in the SQLite database."""
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("category", String),
            Column("amount", Float),
            Column("start_date", DateTime, nullable=True),
            Column("end_date", DateTime, nullable=True),
            Column("is_active", Boolean, nullable=True)
        )
        metadata.create_all(engine)
        return table