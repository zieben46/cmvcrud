# 1. Use the Databricks SQL Connector for Direct Read/Write

import pandas as pd
import databricks.sql

conn = databricks.sql.connect(
    server_hostname="...",
    http_path="...",
    access_token="..."
)

# Read
df = pd.read_sql("SELECT * FROM my_table", conn)

# Write
df.to_sql("my_table", conn, if_exists="append", index=False, method="multi")

# If .to_sql() doesn’t work due to connector limitations, write CSV → S3 → COPY INTO.

# 2. Use S3 + COPY INTO (for Pushing Data to Databricks)

# Upload to S3 using boto3
s3_client.upload_file("my_data.csv", "my-bucket", "staging/my_data.csv")

# Then run COPY INTO via Databricks SQL connector
cursor.execute("""
COPY INTO my_schema.my_table
FROM 's3://my-bucket/staging/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
""")


# 3.  For Pulling from Databricks → Postgres or MinIO

# Use Databricks SQL connector to extract as Pandas

# Then write to:

# Postgres via SQLAlchemy (CRUD API)

# MinIO via df.to_csv() + boto3.upload_file()




#3 more detailed

# import databricks.sql
# import psycopg2
# from psycopg2.extras import execute_values

# # Connect to Databricks
# dbx_conn = databricks.sql.connect(server_hostname="...", http_path="...", access_token="...")
# cursor = dbx_conn.cursor()
# cursor.execute("SELECT * FROM big_table")

# # Connect to Postgres
# pg_conn = psycopg2.connect("dbname=... user=... password=...")
# pg_cursor = pg_conn.cursor()

# batch_size = 10000
# rows = cursor.fetchmany(batch_size)

# while rows:
#     execute_values(pg_cursor, "INSERT INTO target_table VALUES %s", rows)
#     pg_conn.commit()
#     rows = cursor.fetchmany(batch_size)

# cursor.close()
# pg_conn.close()
