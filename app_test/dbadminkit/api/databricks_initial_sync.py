from pyspark.sql import SparkSession
import requests
import time

# Configurable parameters
SOURCE_TABLE = "your_catalog.your_schema.source_table"
VERSION_TO_SYNC = 5
TARGET_DB = "postgres"
TARGET_TABLE = "target_table"
API_URL = "http://localhost:8000"
BATCH_SIZE = 10000  # Matches MAX_ROWS in sync_table
AUTH_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Basic YWRtaW46cGFzc3dvcmQ="  # Base64 "admin:password"
}

# Initialize Spark session
spark = SparkSession.builder.appName("InitialTableSync").getOrCreate()

# Read table at specific version
df = spark.read.option("versionAsOf", VERSION_TO_SYNC).table(SOURCE_TABLE)

# Repartition for efficient batching
num_partitions = (df.count() + BATCH_SIZE - 1) // BATCH_SIZE
df_repartitioned = df.repartition(num_partitions)
batch_data = df_repartitioned.rdd.mapPartitions(lambda partition: [row.asDict() for row in partition]).collect()

# Sync in batches
for i in range(0, len(batch_data), BATCH_SIZE):
    batch = batch_data[i:i + BATCH_SIZE]
    if len(batch) > BATCH_SIZE:  # Shouldn’t happen, but safety check
        print(f"Skipping oversized batch at index {i}: {len(batch)} rows")
        continue
    
    url = f"{API_URL}/sync-table"
    params = {"target": TARGET_DB, "table_name": TARGET_TABLE}
    
    response = requests.post(url, json=batch, headers=AUTH_HEADERS, params=params)
    if response.status_code == 200:
        print(f"Synced {len(batch)} rows in batch {i // BATCH_SIZE + 1}")
        # Update sync version
        requests.post(
            f"{API_URL}/update-sync-version",
            json={"db_type": TARGET_DB, "table_name": TARGET_TABLE, "version": VERSION_TO_SYNC},
            headers=AUTH_HEADERS
        )
    else:
        print(f"Failed at batch {i // BATCH_SIZE + 1}: {response.status_code} - {response.text}")
        break
    
    time.sleep(1)  # Rate limiting to avoid overwhelming API

print(f"Initial sync complete at version {VERSION_TO_SYNC}")
spark.stop()