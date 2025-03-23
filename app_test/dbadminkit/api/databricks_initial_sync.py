from pyspark.sql import SparkSession
import requests

# Initialize Spark session
spark = SparkSession.builder.appName("InitialTableSync").getOrCreate()

# Source table and version
source_table = "your_catalog.your_schema.source_table"
version_to_sync = 5  # Replace with the desired version

# Read table at specific version
df = spark.read.option("versionAsOf", version_to_sync).table(source_table)

# Batch size for manageable chunks (e.g., 1M rows)
batch_size = 1000000

# Collect and send in batches
total_rows = df.count()
print(f"Total rows to sync: {total_rows}")

for offset in range(0, total_rows, batch_size):
    batch_df = df.limit(batch_size).offset(offset)
    batch_data = [row.asDict() for row in batch_df.collect()]
    
    url = "http://localhost:8000/sync-table"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic YWRtaW46cGFzc3dvcmQ="  # Base64 "admin:password"
    }
    params = {"target": "postgres", "table_name": "target_table"}
    
    response = requests.post(url, json=batch_data, headers=headers, params=params)
    if response.status_code == 200:
        print(f"Synced {len(batch_data)} rows at offset {offset}")
    else:
        print(f"Failed at offset {offset}: {response.text}")
        break

# Store the synced version in sync_metadata
with spark.sql("DESCRIBE HISTORY " + source_table).limit(1).collect()[0] as latest:
    synced_version = version_to_sync
url = "http://localhost:8000/update-sync-version"
requests.post(
    url,
    json={"db_type": "postgres", "table_name": "target_table", "version": synced_version},
    headers=headers
)
print(f"Initial sync complete at version {synced_version}")