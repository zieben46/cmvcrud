# datastorekit/engine.py
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from pyspark.sql import SparkSession
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)

class DBEngine:
    def __init__(self, profile: DatabaseProfile):
        self.profile = profile
        if self.profile.db_type == "spark":
            self.spark = self._create_spark_session()
            self.engine = None
            self.Session = None
        else:
            self.engine = self._create_sqlalchemy_engine()
            self.Session = self._create_session()
            self.spark = None

    def _create_sqlalchemy_engine(self):
        """Create a SQLAlchemy engine based on the profile."""
        try:
            if self.profile.db_type == "databricks":
                return create_engine(
                    f"databricks+connector://{self.profile.connection_string.split('://')[1]}",
                    echo=False
                )
            elif self.profile.db_type == "postgres":
                return create_engine(
                    self.profile.connection_string,
                    echo=False
                )
            elif self.profile.db_type == "sqlite":
                return create_engine(
                    self.profile.connection_string,
                    echo=False
                )
            else:
                raise ValueError(f"Unsupported db_type for SQLAlchemy: {self.profile.db_type}")
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {e}")
            raise

    def _create_spark_session(self):
        """Create a SparkSession for Databricks."""
        try:
            if "databricks" not in self.profile.connection_string.lower():
                raise ValueError("SparkEngine requires a Databricks configuration")
            parts = self.profile.connection_string.split("://")[1].split("?")
            auth = parts[0].split("@")
            token = auth[0].split(":")[1]
            host = auth[1]
            http_path = parts[1].split("http_path=")[1].split("&")[0]
            spark = (
                SparkSession.builder
                .appName("datastorekit")
                .config("spark.databricks.service.address", f"https://{host}")
                .config("spark.databricks.service.token", token)
                .config("spark.databricks.service.cluster", http_path.split("/")[4])
                .config("spark.sql.catalogImplementation", "hive")
                .getOrCreate()
            )
            return spark
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {e}")
            raise

    def _create_session(self):
        """Create a SQLAlchemy session factory."""
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

    def stop(self):
        """Stop the engine or SparkSession, if applicable."""
        if self.spark:
            self.spark.stop()
        if self.engine:
            self.engine.dispose()