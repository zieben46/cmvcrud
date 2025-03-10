from pyspark.sql import SparkSession

# Path to your .jar file (adjust based on your project structure)
jar_path = "/path/to/dbadminkit/lib/DatabricksJDBC42.jar"

spark = SparkSession.builder \
    .appName("dbadminkit_jdbc") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

jdbc_url = "jdbc:databricks://<host>:443;httpPath=/sql/protocolv1/o/<org-id>/<cluster-id>;AuthMech=3;UID=token;PWD=<your-token>"
df = spark.read.jdbc(url=jdbc_url, table="my_table")
print(df.collect())
spark.stop()





import jpype
import jpype.imports

# Path to your .jar file
jar_path = "/path/to/dbadminkit/lib/DatabricksJDBC42.jar"

# Start JVM and add .jar to classpath
jpype.startJVM(classpath=[jar_path])

# Import Java classes
from java.sql import DriverManager

# JDBC connection
jdbc_url = "jdbc:databricks://<host>:443;httpPath=/sql/protocolv1/o/<org-id>/<cluster-id>;AuthMech=3;UID=token;PWD=<your-token>"
conn = DriverManager.getConnection(jdbc_url)
stmt = conn.createStatement()
rs = stmt.executeQuery("SELECT * FROM my_table")
while rs.next():
    print(rs.getString("col1"))
conn.close()
jpype.shutdownJVM()


# # In Databricks notebook
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("export_data").getOrCreate()

# # Query the table
# df = spark.sql("SELECT * FROM my_table")

# # Export to S3
df.write.parquet("s3://<your-bucket>/output/my_table_data/")

# # Or export to DBFS
df.write.parquet("/dbfs/FileStore/output/my_table_data/")


# fetching externally

import boto3

s3 = boto3.client("s3", aws_access_key_id="<key>", aws_secret_access_key="<secret>")
s3.download_file("<your-bucket>", "output/my_table_data/part-00000-*.parquet", "local_data.parquet")


# From DBFS

# databricks fs cp dbfs:/FileStore/output/my_table_data/ local_data/ --profile <your-profile>



# Internal Job with DBFS
# Push from Outside:
# Upload local_data.csv to dbfs:/FileStore/input/ using CLI/API.

# Notebook: spark.read.csv("dbfs:/FileStore/input/data.csv").write.saveAsTable("my_table").

# Pull from Inside:
# Notebook: spark.sql("SELECT * FROM my_table").write.parquet("/dbfs/FileStore/output/").

# Fetch: databricks fs cp dbfs:/FileStore/output/ local_data/.

# Key: Only a Databricks token—no AWS/Azure keys needed externally.

# Internal Job with S3/ADLS
# Push from Outside:
# S3 (AWS): Upload to s3://<your-bucket>/input/ with boto3 (AWS keys).

# ADLS (Azure): Upload to abfss://<container>@<account>.dfs.core.windows.net/input/ with Azure SDK (Azure credentials).

# Notebook: Read from storage and save to table.

# Pull from Inside:
# Notebook: Export to s3:// or abfss://.

# Fetch: Use boto3 (S3) or azure-storage-blob (ADLS).

# Key: 
# S3: AWS Access Key ID/Secret Access Key.

# ADLS: Azure credentials (e.g., service principal, SAS token).

# Your Setup
# DBFS: 
# Push: databricks fs cp local_data.csv dbfs:/FileStore/input/ (token).

# Pull: Notebook exports to DBFS, fetch with token.

# Azure vs. AWS: 
# Check your host: <something>.azuredatabricks.net = Azure (ADLS); otherwise AWS (S3).

# Admin can confirm storage backend.



# option 1
# python -m scripts.myscript


# option 2
# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from core.stuff import x


# option 4
# .vscode/settings.json


# {
#     "python.analysis.extraPaths": ["./new_project_name"]
# }

# Reload VS Code (Ctrl+Shift+P, “Developer: Reload Window”).