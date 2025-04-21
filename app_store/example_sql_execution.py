# example_sql_execution.py
import os
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
from typing import List, Dict, Any

# Define .env paths
env_paths = [
    os.path.join(".env", ".env.spark"),
    os.path.join(".env", ".env.postgres")
]

# Initialize orchestrator
try:
    orchestrator = DataStoreOrchestrator(env_paths)
    print("Adapters:", orchestrator.list_adapters())
except Exception as e:
    print(f"Error: {e}")
    exit(1)

# Define TableInfo for Spark and PostgreSQL tables
spark_table_info = TableInfo(
    table_name="spend_plan",
    keys="unique_id,secondary_key",
    scd_type="type2",
    datastore_key="spark_db:default",
    columns={
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
)
postgres_table_info = TableInfo(
    table_name="spend_plan",
    keys="unique_id,secondary_key",
    scd_type="type2",
    datastore_key="spend_plan_db:safe_user",
    columns={
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
)

# Get tables
try:
    spark_table = orchestrator.get_table("spark_db:default", spark_table_info)
    postgres_table = orchestrator.get_table("spend_plan_db:safe_user", postgres_table_info)
    print(f"Connected to Spark table: {spark_table.table_name}")
    print(f"Connected to PostgreSQL table: {postgres_table.table_name}")
except Exception as e:
    print(f"Error: {e}")
    exit(1)

# Execute SQL queries
print("\n=== Executing SQL Queries ===")
try:
    # Spark: Select with parameters
    spark_sql = "SELECT * FROM your_catalog.default.spend_plan WHERE unique_id = :id"
    spark_results = spark_table.execute_sql(spark_sql, {"id": 1})
    print("Spark SQL results:", spark_results)

    # Spark: CDF query
    cdf_sql = "SELECT * FROM table_changes('your_catalog.default.spend_plan', 0, 1)"
    cdf_results = spark_table.execute_sql(cdf_sql)
    print("Spark CDF results:", cdf_results)

    # PostgreSQL: Select with parameters
    postgres_sql = "SELECT * FROM safe_user.spend_plan WHERE unique_id = :id"
    postgres_results = postgres_table.execute_sql(postgres_sql, {"id": 1})
    print("PostgreSQL SQL results:", postgres_results)

    # PostgreSQL: Insert
    insert_sql = "INSERT INTO safe_user.spend_plan (unique_id, secondary_key, category, amount, is_active) VALUES (:id, :key, :cat, :amt, :active)"
    postgres_table.execute_sql(insert_sql, {
        "id": 3,
        "key": "C",
        "cat": "Books",
        "amt": 25.0,
        "active": True
    })
    print("Inserted record into PostgreSQL")
except Exception as e:
    print(f"SQL execution failed: {e}")

# Try CSV (should fail)
try:
    csv_table_info = TableInfo(
        table_name="spend_plan",
        keys="unique_id,secondary_key",
        scd_type="type2",
        datastore_key="csv_db:default",
        columns={
            "unique_id": "Integer",
            "secondary_key": "String",
            "category": "String",
            "amount": "Float",
            "start_date": "DateTime",
            "end_date": "DateTime",
            "is_active": "Boolean"
        }
    )
    csv_table = orchestrator.get_table("csv_db:default", csv_table_info)
    csv_table.execute_sql("SELECT * FROM spend_plan")
except NotImplementedError as e:
    print("CSV SQL execution failed (expected):", str(e))