# datastorekit/acceptance_tests/test_sync_tables.py
import os
import pytest
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from .dsl.sync_dsl import SyncDSL
from .drivers.databricks_driver import DatabricksDriver
from .drivers.postgres_driver import PostgresDriver
from .drivers.csv_driver import CSVDriver
from .drivers.inmemory_driver import InMemoryDriver
from .drivers.mongodb_driver import MongoDBDriver
from .drivers.spark_driver import SparkDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.exceptions import DatastoreOperationError
from datastorekit.config import Config
from pymongo import MongoClient
from datetime import datetime

# Common schema and test data
schema = {
    "unique_id": "Integer",
    "secondary_key": "String",
    "category": "String",
    "amount": "Float",
    "start_date": "DateTime",
    "end_date": "DateTime",
    "is_active": "Boolean"
}
test_data = [
    {"unique_id": 1, "secondary_key": "A", "category": "Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
    {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True},
    {"unique_id": 3, "secondary_key": "C", "category": "Books", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True}
]

def test_sync_tables_across_datastores():
    """Test sync_tables between different datastores using DSL."""
    # Setup: Load environment files and initialize orchestrator
    env_files = {
        "databricks": os.path.join(".env", ".env.databricks_dev"),
        "postgres": os.path.join(".env", ".env.postgres_dev"),
        "csv": os.path.join(".env", ".env.csv_dev"),
        "inmemory": os.path.join(".env", ".env.inmemory_dev"),
        "mongodb": os.path.join(".env", ".env.mongodb_dev"),
        "spark": os.path.join(".env", ".env.spark_dev")
    }
    for ds_type, env_path in env_files.items():
        assert Config.validate_env_file(env_path, ds_type), f"Invalid {ds_type} .env file: {env_path}"

    orchestrator = DataStoreOrchestrator(env_paths=list(env_files.values()))

    # Define drivers
    drivers = {
        "databricks": DatabricksDriver(orchestrator, "databricks_db:default"),
        "postgres": PostgresDriver(orchestrator, "spend_plan_db:safe_user"),
        "csv": CSVDriver(orchestrator, "csv_db:default"),
        "inmemory": InMemoryDriver(orchestrator, "inmemory_db:default"),
        "mongodb": MongoDBDriver(orchestrator, "mongodb_db:default"),
        "spark": SparkDriver(orchestrator, "spark_db:default")
    }

    # Test combinations: source -> target (expanded)
    test_combinations = [
        ("databricks", "postgres"),
        ("csv", "inmemory"),
        ("mongodb", "spark"),
        ("spark", "postgres"),
        ("inmemory", "csv"),
        ("postgres", "mongodb"),
        ("databricks", "mongodb"),
        ("postgres", "csv"),
        ("spark", "inmemory")
    ]

    for source_ds, target_ds in test_combinations:
        source_driver = drivers[source_ds]
        target_driver = drivers[target_ds]
        source_table_name = f"source_test_{source_ds}_{target_ds}"
        target_table_name = f"target_test_{source_ds}_{target_ds}"

        # Setup source table and data
        dsl = SyncDSL(source_driver, target_driver)
        dsl.create_table(source_table_name, schema, key=None) \
           .setup_data(test_data)

        # Execute sync_tables (full load)
        dsl.sync_to_target(target_table_name, sync_method="full_load")

        # Verify target table
        target_dsl = SyncDSL(target_driver, source_driver)
        target_dsl.select_table(target_table_name, key=None) \
                  .assert_tables_synced()

        # Test schema change
        updated_schema = schema.copy()
        updated_schema["new_column"] = "String"
        updated_data = [dict(record, new_column=f"Test{record['unique_id']}") for record in test_data]
        dsl.create_table(source_table_name, updated_schema, key=None) \
           .setup_data(updated_data)
        dsl.sync_to_target(target_table_name, sync_method="full_load")
        target_dsl.assert_tables_synced()

        # Cleanup
        cleanup_table(source_driver, source_table_name)
        cleanup_table(target_driver, target_table_name)
        logger.info(f"Successfully tested sync_tables from {source_ds} to {target_ds}")

def test_sync_tables_incremental_databricks():
    """Test incremental sync with Databricks CDF to PostgreSQL."""
    # Setup orchestrator and drivers
    orchestrator = DataStoreOrchestrator(env_paths=[
        os.path.join(".env", ".env.databricks_dev"),
        os.path.join(".env", ".env.postgres_dev")
    ])
    source_driver = DatabricksDriver(orchestrator, "databricks_db:default")
    target_driver = PostgresDriver(orchestrator, "spend_plan_db:safe_user")
    source_table_name = "source_test_inc_databricks"
    target_table_name = "target_test_inc_databricks"

    # Setup source table and data
    dsl = SyncDSL(source_driver, target_driver)
    dsl.create_table(source_table_name, schema, key=None) \
       .setup_data(test_data)

    # Initial full load
    dsl.sync_to_target(target_table_name, sync_method="full_load") \
       .assert_tables_synced()

    # Simulate CDF changes (2,000 changes, 4 chunks of 500)
    cdf_changes = []
    for i in range(1000):
        cdf_changes.append({
            "unique_id": 4 + i,
            "secondary_key": f"D{i}",
            "category": f"Item{i}",
            "amount": 30.0 + i,
            "start_date": "2023-01-04",
            "end_date": None,
            "is_active": True,
            "_change_type": "insert"
        })
        cdf_changes.append({
            "unique_id": 1,
            "secondary_key": "A",
            "category": f"Food{i}",
            "amount": 50.0 + i/10,
            "start_date": "2023-01-01",
            "end_date": None,
            "is_active": True,
            "_change_type": "update_postimage"
        })

    dsl.execute_crud("CREATE", cdf_changes[::2])
    dsl.execute_crud("UPDATE", cdf_changes[1::2], {"unique_id": 1, "secondary_key": "A"})

    # Incremental sync
    dsl.sync_to_target(target_table_name, sync_method="incremental")

    # Verify target table (check last update and some inserts)
    target_dsl = SyncDSL(target_driver, source_driver)
    target_dsl.select_table(target_table_name, key=None) \
              .assert_table_has([
                  {"unique_id": 1, "secondary_key": "A", "category": "Food999", "amount": 59.9, "start_date": "2023-01-01", "end_date": None, "is_active": True},
                  {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True},
                  {"unique_id": 3, "secondary_key": "C", "category": "Books", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True},
                  {"unique_id": 4, "secondary_key": "D0", "category": "Item0", "amount": 30.0, "start_date": "2023-01-04", "end_date": None, "is_active": True},
                  {"unique_id": 1003, "secondary_key": "D999", "category": "Item999", "amount": 1029.0, "start_date": "2023-01-04", "end_date": None, "is_active": True}
              ])

    # Cleanup
    cleanup_table(source_driver, source_table_name)
    cleanup_table(target_driver, target_table_name)
    logger.info("Incremental sync test passed for Databricks to PostgreSQL")

def test_sync_tables_duplicate_key_error():
    """Test sync_tables error handling for duplicate keys."""
    orchestrator = DataStoreOrchestrator(env_paths=[
        os.path.join(".env", ".env.databricks_dev"),
        os.path.join(".env", ".env.postgres_dev")
    ])
    source_driver = DatabricksDriver(orchestrator, "databricks_db:default")
    target_driver = PostgresDriver(orchestrator, "spend_plan_db:safe_user")

    schema = {
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
    test_data = [
        {"unique_id": 1, "secondary_key": "A", "category": "Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
        {"unique_id": 1, "secondary_key": "A", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True}  # Duplicate key
    ]

    dsl = SyncDSL(source_driver, target_driver)
    dsl.create_table("source_error_test", schema, key=None) \
       .setup_data(test_data)

    with pytest.raises(DatastoreOperationError):
        dsl.sync_to_target("target_error_test", sync_method="full_load")

    # Cleanup
    source_engine = create_engine(source_driver.engine.url)
    target_engine = create_engine(target_driver.engine.url)
    metadata = MetaData()
    source_table = Table("source_error_test", metadata, schema="default")
    target_table = Table("target_error_test", metadata, schema="safe_user")
    metadata.drop_all(source_engine, tables=[source_table])
    metadata.drop_all(target_engine, tables=[target_table])
    source_engine.dispose()
    target_engine.dispose()
    logger.info("Duplicate key error test passed for Databricks to PostgreSQL")

def test_sync_tables_null_value_error():
    """Test sync_tables error handling for null values in non-nullable columns."""
    orchestrator = DataStoreOrchestrator(env_paths=[
        os.path.join(".env", ".env.csv_dev"),
        os.path.join(".env", ".env.postgres_dev")
    ])
    source_driver = CSVDriver(orchestrator, "csv_db:default")
    target_driver = PostgresDriver(orchestrator, "spend_plan_db:safe_user")

    schema = {
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
    test_data = [
        {"unique_id": 1, "secondary_key": "A", "category": None, "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True}
    ]

    dsl = SyncDSL(source_driver, target_driver)
    dsl.create_table("source_null_test", schema, key=None) \
       .setup_data(test_data)

    with pytest.raises(DatastoreOperationError):
        dsl.sync_to_target("target_null_test", sync_method="full_load")

    # Cleanup
    file_path = os.path.join(source_driver.base_dir, "source_null_test.csv")
    if os.path.exists(file_path):
        os.remove(file_path)
    target_engine = create_engine(target_driver.engine.url)
    metadata = MetaData()
    target_table = Table("target_null_test", metadata, schema="safe_user")
    metadata.drop_all(target_engine, tables=[target_table])
    target_engine.dispose()
    logger.info("Null value error test passed for CSV to PostgreSQL")

def cleanup_table(driver, table_name):
    """Cleanup table based on datastore type."""
    ds_type = driver.datastore_key.split(":")[0]
    if ds_type in ["databricks", "postgres", "spark"]:
        engine = create_engine(driver.engine.url)
        metadata = MetaData()
        schema = driver.orchestrator.adapters[driver.datastore_key].profile.schema or "default"
        table = Table(table_name, metadata, schema=schema)
        metadata.drop_all(engine, tables=[table])
        engine.dispose()
    elif ds_type == "csv":
        file_path = os.path.join(driver.base_dir, f"{table_name}.csv")
        if os.path.exists(file_path):
            os.remove(file_path)
    elif ds_type == "inmemory":
        driver.adapter.data.pop(table_name, None)
    elif ds_type == "mongodb":
        driver.db[table_name].drop()