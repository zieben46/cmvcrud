from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, inspect
import requests
from pyspark.sql import SparkSession
import boto3

app = FastAPI()

# Config
DATABRICKS_TOKEN = "your_databricks_token"
DATABRICKS_INSTANCE = "https://<databricks-instance>"
S3_BUCKET = "s3://your-bucket"
S3_PREFIX = "table_data/"
DB_URL = "postgresql://user:password@host:port/dbname"

# SQLAlchemy engine
engine = create_engine(DB_URL)

# Custom DBManager (assumed)
class DBManager:
    def __init__(self, engine):
        self.engine = engine
    
    def execute_query(self, query):
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

db_manager = DBManager(engine)

def get_spark_session():
    """Get Spark session (Databricks context)."""
    return SparkSession.builder.getOrCreate()

def export_table_to_s3(spark, table_name, s3_path, full_load=True, previous_version=None):
    """Export full table or changes to S3 as CSV."""
    if full_load:
        df = spark.table(table_name)
    else:
        df = spark.sql(f"SELECT * FROM table_changes('{table_name}', {previous_version}, <current_version>)")
    df.coalesce(10).write.mode("overwrite").option("header", "true").csv(s3_path)

def get_s3_file_path():
    """Get the latest CSV from S3."""
    s3_client = boto3.client("s3")
    bucket_name = S3_BUCKET.split("s3://")[1]
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=S3_PREFIX)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not files:
        raise ValueError("No CSV files found in S3.")
    return f"{S3_BUCKET}/{sorted(files)[-1]}"

def get_databricks_schema(spark, table_name):
    """Get Databricks table schema."""
    df = spark.table(table_name)
    return {col.name: col.dataType.simpleString() for col in df.schema}

def get_postgres_schema(table_name):
    """Get PostgreSQL table schema."""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return {}
    columns = inspector.get_columns(table_name)
    return {col["name"]: str(col["type"]) for col in columns}

def map_types(databricks_type):
    """Map Databricks types to PostgreSQL types (simplified)."""
    type_map = {
        "string": "VARCHAR",
        "int": "INTEGER",
        "bigint": "BIGINT",
        "double": "DOUBLE PRECISION",
        "timestamp": "TIMESTAMP"
    }
    return type_map.get(databricks_type, "TEXT")  # Default to TEXT for unmapped types

def recreate_postgres_table(table_name, schema):
    """Drop and recreate PostgreSQL table with Databricks schema."""
    drop_query = f"DROP TABLE IF EXISTS {table_name};"
    create_query = f"CREATE TABLE {table_name} ({', '.join([f'{col} {map_types(type_)}' for col, type_ in schema.items()])});"
    db_manager.execute_query(drop_query)
    db_manager.execute_query(create_query)

@app.post("/load-or-rewrite-table")
async def load_or_rewrite_table(initial_load: bool = False, force_rewrite: bool = False):
    """
    Load initial 20M rows or rewrite table if schema changes.
    - initial_load: True for full 20M-row load.
    - force_rewrite: True to force table recreation (e.g., manual schema change).
    """
    try:
        # Spark session (Databricks)
        spark = get_spark_session()
        table_name = "your_table_name"
        s3_path = f"{S3_BUCKET}/{S3_PREFIX}"

        # Get schemas
        databricks_schema = get_databricks_schema(spark, table_name)
        postgres_schema = get_postgres_schema(table_name)

        # Check schema mismatch
        schema_changed = databricks_schema != postgres_schema and postgres_schema != {}
        needs_recreation = initial_load or schema_changed or force_rewrite or not postgres_schema

        # Export full table to S3
        export_table_to_s3(spark, table_name, s3_path, full_load=True)

        if needs_recreation:
            # Recreate table if initial load, schema changed, forced, or table doesn’t exist
            recreate_postgres_table(table_name, databricks_schema)

        # Load data from S3 into PostgreSQL
        s3_file = get_s3_file_path()
        copy_query = f"""
            COPY {table_name} FROM '{s3_file}' DELIMITER ',' CSV HEADER;
        """
        db_manager.execute_query(copy_query)

        return {"status": "load_complete", "schema_changed": schema_changed}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


        # POST /load-or-rewrite-table?initial_load=true
        # POST /load-or-rewrite-table?force_rewrite=true

    # Assumes PostgreSQL can read from S3 directly (e.g., via aws_s3 extension). If not, add a step to download the file locally:
        s3_client = boto3.client("s3")
        s3_client.download_file(S3_BUCKET.split("s3://")[1], get_s3_file_path().split(S3_BUCKET + "/")[1], "/tmp/table.csv")
        copy_query = f"COPY {table_name} FROM '/tmp/table.csv' DELIMITER ',' CSV HEADER;"


        # curl -X POST "http://your-api/load-or-rewrite-table?initial_load=true"
        # curl -X POST "http://your-api/load-or-rewrite-table?force_rewrite=true"