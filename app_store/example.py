# example.py
import os
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
from sqlalchemy import Integer, String, Float
from typing import List, Dict, Any

# Define paths to .env files
env_paths = [
    os.path.join(".env", ".env.postgres"),
    os.path.join(".env", ".env.databricks"),
    os.path.join(".env", ".env.mongodb")
]

# Initialize orchestrator
try:
    orchestrator = DataStoreOrchestrator(env_paths)
    print("Adapters:", orchestrator.list_adapters())
except Exception as e:
    print(f"Error: {e}")
    exit(1)

# Define TableInfo for PostgreSQL spend_plan
pg_table_info = TableInfo(
    table_name="spend_plan",
    columns={
        "unique_id": Integer,
        "category": String,
        "amount": Float
    },
    key="unique_id",
    scd_type="type1"
)

# Get PostgreSQL table
try:
    pg_table = orchestrator.get_table("spend_plan_db:safe_user", pg_table_info)
    print(f"Connected to PostgreSQL table: {pg_table.table_name}")
except Exception as e:
    print(f"Error: {e}")
    exit(1)

# Perform CRUD operations
print("\n=== Performing CRUD Operations on PostgreSQL ===")
try:
    # Create
    pg_table.create([
        {"unique_id": 1, "category": "Food", "amount": 50.0},
        {"unique_id": 2, "category": "Travel", "amount": 100.0}
    ])
    print("Created records in PostgreSQL")

    # Read
    records = pg_table.read({"category": "Food"})
    print("PostgreSQL records:", records)

    # Update
    pg_table.update([{"amount": 75.0}], {"unique_id": 1})
    print("Updated record in PostgreSQL")

    # Delete
    pg_table.delete({"unique_id": 1})
    print("Deleted record in PostgreSQL")
except Exception as e:
    print(f"PostgreSQL operation failed: {e}")