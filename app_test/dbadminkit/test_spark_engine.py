from app_test.dbadminkit.core.database_profile import DBConfig
from dbadminkit.core.engine import SparkEngine

# Replace these with your actual Databricks details
DATABRICKS_HOST = "adb-1234567890.1.azuredatabricks.net"
DATABRICKS_TOKEN = "dapi1234567890abcdef1234567890abcdef"
DATABRICKS_HTTP_PATH = "/sql/1.0/endpoints/1234567890abcdef"
DATABASE = "default"

# Create a DBConfig for Databricks
config = DBConfig(
    mode="databricks",
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN,
    http_path=DATABRICKS_HTTP_PATH,
    database=DATABASE
)

# Try to create a SparkEngine and get a Spark session
try:
    spark_engine = SparkEngine(config)
    spark = spark_engine.get_spark()
    print("Successfully created SparkEngine and got Spark session!")

    # Test a simple query to confirm connectivity
    spark.sql(f"SHOW TABLES IN {DATABASE}").show()
    print(f"Connected to Databricks and listed tables in {DATABASE} database!")

except Exception as e:
    print(f"Failed to create SparkEngine: {str(e)}")

# pip install pyspark==3.4.0

# pip install -e .

from app_test.dbadminkit.core.database_profile import DBConfig
from dbadminkit.core.spark_engine import SparkEngine

config = DBConfig(
    mode="databricks",
    host="adb-1234567890.1.azuredatabricks.net",
    token="dapi1234567890abcdef1234567890abcdef",
    http_path="/sql/1.0/endpoints/1234567890abcdef"
)

spark_engine = SparkEngine(config)
print("SparkEngine created successfully!" if spark_engine else "Failed to create SparkEngine")