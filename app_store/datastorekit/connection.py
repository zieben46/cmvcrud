from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from pyspark.sql import SparkSession
from typing import Optional
from datastorekit.profile import DatabaseProfile
import logging

logger = logging.getLogger(__name__)

class DatastoreConnection:
    def __init__(self, profile: DatabaseProfile):
        """Initialize a connection to a datastore based on the profile."""
        self.profile = profile
        if self.profile.db_type == "spark":
            self.spark = self._create_spark_session()
            self.engine = None
            self.session_factory = None
        else:
            self.engine = self._create_sqlalchemy_engine()
            self.session_factory = self._create_session()
            self.spark = None

    def _create_sqlalchemy_engine(self):
        """Private: Create a SQLAlchemy engine based on the profile."""
        try:
            if self.profile.db_type == "databricks":
                return create_engine(
                    f"databricks+connector://{self.profile.connection_string.split('://')[1]}",
                    echo=False
                )
            elif self.profile.db_type in ("postgres", "sqlite"):
                return create_engine(self.profile.connection_string, echo=False)
            else:
                raise ValueError(f"Unsupported db_type for SQLAlchemy: {self.profile.db_type}")
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {e}")
            raise

    def _create_spark_session(self):
        """Private: Create a SparkSession for Databricks."""
        try:
            if "databricks" not in self.profile.connection_string.lower():
                raise ValueError("Spark connection requires a Databricks configuration")
            parts = self.profile.connection_string.split("://")[1].split("?")
            auth = parts[0].split("@")
            token = auth[0].split(":")[1]
            host = auth[1]
            http_path = parts[1].split("http_path=")[1].split("&")[0]
            return (
                SparkSession.builder
                .appName("datastorekit")
                .config("spark.databricks.service.address", f"https://{host}")
                .config("spark.databricks.service.token", token)
                .config("spark.databricks.service.cluster", http_path.split("/")[4])
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate()
            )
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {e}")
            raise

    def _create_session(self):
        """Private: Create a SQLAlchemy session factory."""
        try:
            return scoped_session(sessionmaker(bind=self.engine))
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            raise

    def get_engine(self) -> Optional[create_engine]:
        """Return the SQLAlchemy engine, if applicable."""
        return self.engine

    def get_spark(self) -> Optional[SparkSession]:
        """Return the SparkSession, if applicable."""
        return self.spark

    def get_session_factory(self) -> Optional[scoped_session]:
        """Return the SQLAlchemy session factory, if applicable."""
        return self.session_factory

    def stop(self):
        """Close the connection, stopping the engine or SparkSession."""
        if self.spark:
            self.spark.stop()
        if self.engine:
            self.engine.dispose()