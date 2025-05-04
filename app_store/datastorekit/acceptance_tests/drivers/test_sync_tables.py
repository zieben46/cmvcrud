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
from datastorekit.config import Config
from pymongo import MongoClient

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

    # Define schema and test data
    schema = {
        "unique_id": Integer,
        "secondary_key": String,
        "category": String,
        "amount": Float,
        "start_date": DateTime,
        "end_date": DateTime,
        "is_active": Boolean
    }
    test_data = [
        {"unique_id": 1, "secondary_key": "A", "category": "Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
        {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True},
        {"unique_id": 3, "secondary_key": "C", "category": "Books", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True}
    ]

    # Test combinations: source -> target
    test_combinations = [
        ("databricks", "postgres"),
        ("csv", "inmemory"),
        ("mongodb", "spark"),
        ("spark", "postgres"),
        ("inmemory", "csv"),
        ("postgres", "mongodb")
    ]

    for source_ds, target_ds in test_combinations:
        source_driver = drivers[source_ds]
        target_driver = drivers[target_ds]
        source_table_name = f"source_test_{source_ds}"
        target_table_name = f"target_test_{target_ds}"

        # Setup source table and data
        dsl = SyncDSL(source_driver, target_driver)
        dsl.create_table(source_table_name, schema, key=None) \
           .setup_data(test_data)

        # Execute sync_tables
        dsl.sync_to_target(target_table_name, sync_method="full_load")

        # Verify target table
        target_dsl = SyncDSL(target_driver, source_driver)
        target_dsl.select_table(target_table_name, key=None) \
                  .assert_tables_synced()

        # Test schema change
        updated_schema = schema.copy()
        updated_schema["new_column"] = String
        dsl.create_table(source_table_name, updated_schema, key=None) \
           .setup_data([dict(record, new_column="Test") for record in test_data])
        dsl.sync_to_target(target_table_name, sync_method="full_load")
        target_dsl.assert_tables_synced()

        # Cleanup
        if source_ds in ["databricks", "postgres", "spark"]:
            engine = create_engine(source_driver.engine.url)
            metadata = MetaData()
            table = Table(source_table_name, metadata, schema=source_driver.orchestrator.adapters[source_driver.datastore_key].profile.schema or "default")
            metadata.drop_all(engine, tables=[table])
            engine.dispose()
        elif source_ds == "csv":
            file_path = os.path.join(source_driver.base_dir, f"{source_table_name}.csv")
            if os.path.exists(file_path):
                os.remove(file_path)
        elif source_ds == "inmemory":
            source_driver.adapter.data.pop(source_table_name, None)
        elif source_ds == "mongodb":
            source_driver.db[source_table_name].drop()

        if target_ds in ["databricks", "postgres", "spark"]:
            engine = create_engine(target_driver.engine.url)
            metadata = MetaData()
            table = Table(target_table_name, metadata, schema=target_driver.orchestrator.adapters[target_driver.datastore_key].profile.schema or "default")
            metadata.drop_all(engine, tables=[table])
            engine.dispose()
        elif target_ds == "csv":
            file_path = os.path.join(target_driver.base_dir, f"{target_table_name}.csv")
            if os.path.exists(file_path):
                os.remove(file_path)
        elif target_ds == "inmemory":
            target_driver.adapter.data.pop(target_table_name, None)
        elif target_ds == "mongodb":
            target_driver.db[target_table_name].drop()

        logger.info(f"Successfully tested sync_tables from {source_ds} to {target_ds}")

def test_sync_tables_error_handling():
    """Test sync_tables error handling for duplicate keys."""
    orchestrator = DataStoreOrchestrator(env_paths=[
        os.path.join(".env", ".env.databricks_dev"),
        os.path.join(".env", ".env.postgres_dev")
    ])
    source_driver = DatabricksDriver(orchestrator, "databricks_db:default")
    target_driver = PostgresDriver(orchestrator, "spend_plan_db:safe_user")

    schema = {
        "unique_id": Integer,
        "category": String,
        "amount": Float
    }
    test_data = [
        {"unique_id": 1, "category": "Food", "amount": 50.0},
        {"unique_id": 1, "category": "Travel", "amount": 100.0}  # Duplicate key
    ]

    dsl = SyncDSL(source_driver, target_driver)
    dsl.create_table("source_error_test", schema, key="unique_id") \
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