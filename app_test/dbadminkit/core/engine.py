from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .config import DBConfig, DBMode

class DBEngine:
    def __init__(self, config: DBConfig):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        if self.config.mode == DBMode.IN_MEMORY:
            return create_engine("sqlite:///:memory:", echo=True)
        elif self.config.mode in (DBMode.LIVE, DBMode.TEST):
            if not self.config.connection_string:
                raise ValueError("Connection string required for live/test mode")
            return create_engine(self.config.connection_string, echo=self.config.mode == DBMode.TEST)
        else:
            raise ValueError(f"Unknown mode: {self.config.mode}")

    def get_session(self):
        return self.Session()
    
    
    
from pyspark.sql import SparkSession
from dbadminkit.core.config import DBConfig

class SparkEngine:
    def __init__(self, config: DBConfig):
        self.config = config
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        if "databricks" not in self.config.connection_string.lower():
            raise ValueError("SparkEngine requires a Databricks configuration")
        
        # Extract host, token, and http_path from connection string
        # e.g., "databricks://token:dapi123@host?http_path=/sql/1.0/endpoints/123"
        parts = self.config.connection_string.split("://")[1].split("?")
        auth = parts[0].split("@")
        token = auth[0].split(":")[1]
        host = auth[1]
        http_path = parts[1].split("http_path=")[1].split("&")[0]

        spark = (
            SparkSession.builder
            .appName("dbadminkit")
            .config("spark.databricks.service.address", f"https://{host}")
            .config("spark.databricks.service.token", token)
            .config("spark.databricks.service.cluster", http_path.split("/")[4])  # Extract cluster ID
            .config("spark.sql.catalogImplementation", "hive")
            .getOrCreate()
        )
        return spark

    def get_spark(self):
        return self.spark

    def stop(self):
        self.spark.stop()