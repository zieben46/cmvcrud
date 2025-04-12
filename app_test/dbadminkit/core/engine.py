from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .database_profile import DBConfig, DBMode

class DBEngine:
    def __init__(self, config: DBConfig):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        if self.config.database ==databricks
            return create_engine(databricks, echo=True)
        elif self.config.database =  postgres
            return create_engine(postgres, echo=True)
                raise ValueError("Connection string required for live/test mode")
            return create_engine(self.config.connection_string, echo=self.config.mode == DBMode.TEST)
        else:
            raise ValueError(f"Unknown mode: {self.config.mode}")

    def get_session(self):
        return self.Session()
    
    
    
from pyspark.sql import SparkSession
from app_test.dbadminkit.core.database_profile import DBConfig

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


#     from dbadminkit.core.engine import SparkEngine
# from dbadminkit.core.database_profile import DatabaseProfile

# # Configure with your Databricks token and cluster details
# config = DatabaseProfile(
#     mode=DBMode.TEST,
#     connection_string="databricks://token:<your-token>@<your-databricks-host>?http_path=/sql/protocolv1/o/<org-id>/<cluster-id>"
# )
# engine = SparkEngine(config)
# spark = engine.get_spark()

# # Try a simple select
# df = spark.table("my_table").select("col1")
# print(df.collect())

# engine.stop()