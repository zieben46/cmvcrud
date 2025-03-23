from pyspark.sql import SparkSession
import requests

# Initialize Spark session
spark = SparkSession.builder.appName("CDFBatch20M").getOrCreate()

# Source and target details
source_table = "your_catalog.your_schema.source_table"
target_db = "postgres"
target_table = "target_table"

# Get last synced version from FastAPI
url = "http://localhost:8000/get-last-sync-version"
headers = {"Authorization": "Basic YWRtaW46cGFzc3dvcmQ="}  # Base64 "admin:password"
params = {"db_type": target_db, "table_name": target_table}
response = requests.get(url, headers=headers, params=params)
last_version = response.json()["last_version"] if response.status_code == 200 else 0

# Read CDF (Change Data Feed) as a batch from last version
cdf_batch = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_version)
    .table(source_table)
)

# Collect changes and send to endpoint
changes = [row.asDict() for row in cdf_batch.collect()]
if changes:
    url = "http://localhost:8000/ingest-cdf"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46cGFzc3dvcmQ="
    }
    params = {"target": target_db, "table_name": target_table}
    response = requests.post(url, json=changes, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Failed to send batch: {response.text}")
    else:
        # Update version after successful batch
        latest_version = spark.sql(f"DESCRIBE HISTORY {source_table}").select("version").orderBy("version", ascending=False).first()[0]
        requests.post(
            "http://localhost:8000/update-sync-version",
            json={"db_type": target_db, "table_name": target_table, "version": latest_version},
            headers=headers
        )
        print(f"Sent batch with {len(changes)} changes, updated to version {latest_version}")

# Stop the Spark session
spark.stop()