# datastorekit/spark_engine.py
from pyspark.sql import SparkSession

class SparkEngine:
    def __init__(self, config: DatabaseProfile):
        self.config = config
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        if "databricks" not in self.config.connection_string.lower():
            raise ValueError("SparkEngine requires a Databricks configuration")
        parts = self.config.connection_string.split("://")[1].split("?")
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

    def get_spark(self):
        return self.spark

    def stop(self):
        self.spark.stop()