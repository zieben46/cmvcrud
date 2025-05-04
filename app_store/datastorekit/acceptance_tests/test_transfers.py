# datastorekit/acceptance_tests/test_transfers.py
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from .dsl.sync_dsl import SyncDSL
from .drivers.databricks_driver import DatabricksDriver
from .drivers.postgres_driver import PostgresDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator
from datastorekit.config import Config
from datastorekit.models.table_info import TableInfo
from datetime import datetime
import pytest

schema = {
    "unique_id": "Integer",
    "secondary_key": "String",
    "category": "String",
    "amount": "Float",
    "start_date": "DateTime",
    "end_date": "DateTime",
    "is_active": "Boolean"
}
history_schema = {
    "version": "Integer",
    "timestamp": "DateTime",
    "operation": "String",
    "operation_parameters": "String",
    "num_affected_rows": "BigInteger"
}
test_data = [
    {"unique_id": 1, "secondary_key": "A", "category": "Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
    {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True}
]

def test_databricks_to_postgres_replication():
    """Test Databricks to PostgreSQL replication with history versioning and chunking."""
    # Setup: Load environment files and initialize orchestrator
    databricks_env = os.path.join(".env", ".env.databricks_dev")
    postgres_env = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(databricks_env, "databricks"), f"Invalid Databricks .env file: {databricks_env}"
    assert Config.validate_env_file(postgres_env, "postgres"), f"Invalid PostgreSQL .env file: {postgres_env}"
    
    orchestrator = DataStoreOrchestrator(env_paths=[databricks_env, postgres_env])
    databricks_driver = DatabricksDriver(orchestrator, "databricks_db:default")
    postgres_driver = PostgresDriver(orchestrator, "spend_plan_db:safe_user")
    replicator = DatabricksToPostgresReplicator(orchestrator, full_load_batch_size=1000, cdf_batch_size=500)

    # Create temporary source table in Databricks
    databricks_dsl = SyncDSL(databricks_driver, postgres_driver)
    databricks_dsl.create_table("source_test", schema, key=None) \
                  .setup_data(test_data)

    # Execute full load replication (no history table, expect full load)
    result = replicator.replicate("source_test", "target_test", "history_test", max_changes=5000)
    assert result, "Full load replication failed"

    # Verify target table
    postgres_dsl = SyncDSL(postgres_driver, databricks_driver)
    postgres_dsl.select_table("target_test", key=None) \
                .assert_table_has(test_data)

    # Create history table and seed initial version
    history_dsl = SyncDSL(postgres_driver, databricks_driver)
    history_dsl.create_table("history_test", history_schema, key="version") \
               .setup_data([
                   {"version": 1, "timestamp": datetime.now(), "operation": "FULL_LOAD", "operation_parameters": "", "num_affected_rows": 2}
               ])

    # Simulate 2,000 CDF changes to test chunking and time order
    cdf_changes = []
    for i in range(1000):
        cdf_changes.append({
            "unique_id": 3 + i,
            "secondary_key": f"C{i}",
            "category": f"Book{i}",
            "amount": 25.0 + i,
            "start_date": "2023-01-03",
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

    databricks_dsl.execute_crud("CREATE", cdf_changes[::2])
    databricks_dsl.execute_crud("UPDATE", cdf_changes[1::2], {"unique_id": 1, "secondary_key": "A"})

    # Execute incremental replication (2,000 changes, 4 chunks of 500)
    result = replicator.replicate("source_test", "target_test", "history_test", max_changes=5000)
    assert result, "Incremental replication failed"

    # Verify target table (check last update for unique_id=1 and some inserts)
    postgres_dsl.assert_table_has([
        {"unique_id": 1, "secondary_key": "A", "category": "Food999", "amount": 59.9, "start_date": "2023-01-01", "end_date": None, "is_active": True},
        {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True},
        {"unique_id": 3, "secondary_key": "C0", "category": "Book0", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True},
        {"unique_id": 1002, "secondary_key": "C999", "category": "Book999", "amount": 1024.0, "start_date": "2023-01-03", "end_date": None, "is_active": True}
    ])

    # Verify history table (incremental merge)
    history_dsl.assert_table_has([
        {"version": 1, "operation": "FULL_LOAD", "operation_parameters": "", "num_affected_rows": 2},
        {"version": 2, "operation": "INCREMENTAL_MERGE", "operation_parameters": "version_range=(1, 2)", "num_affected_rows": 2000}
    ])

    # Test full reload due to excessive changes
    result = replicator.replicate("source_test", "target_test", "history_test", max_changes=1000)
    assert result, "Full reload replication failed"
    postgres_dsl.assert_table_has([
        {"unique_id": 1, "secondary_key": "A", "category": "Food999", "amount": 59.9, "start_date": "2023-01-01", "end_date": None, "is_active": True},
        {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True},
        {"unique_id": 3, "secondary_key": "C0", "category": "Book0", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True},
        {"unique_id": 1002, "secondary_key": "C999", "category": "Book999", "amount": 1024.0, "start_date": "2023-01-03", "end_date": None, "is_active": True}
    ])

    # Verify history table (full reload)
    history_dsl.assert_table_has([
        {"version": 1, "operation": "FULL_LOAD", "operation_parameters": "", "num_affected_rows": 2},
        {"version": 2, "operation": "INCREMENTAL_MERGE", "operation_parameters": "version_range=(1, 2)", "num_affected_rows": 2000},
        {"version": 3, "operation": "FULL_LOAD", "operation_parameters": "", "num_affected_rows": 1000}
    ])

    # Teardown: Drop temporary tables
    databricks_engine = create_engine(databricks_driver.engine.url)
    postgres_engine = create_engine(postgres_driver.engine.url)
    metadata = MetaData()
    source_table = Table("source_test", metadata, schema="default")
    target_table = Table("target_test", metadata, schema="safe_user")
    history_table = Table("history_test", metadata, schema="safe_user")
    metadata.drop_all(databricks_engine, tables=[source_table])
    metadata.drop_all(postgres_engine, tables=[target_table, history_table])
    databricks_engine.dispose()
    postgres_engine.dispose()

def test_replication_error_handling():
    """Test replication error handling for duplicate keys."""
    env_files = [
        os.path.join(".env", ".env.databricks_dev"),
        os.path.join(".env", ".env.postgres_dev")
    ]
    for env_path in env_files:
        assert Config.validate_env_file(env_path, env_path.split(".")[-2]), f"Invalid .env file: {env_path}"
    
    orchestrator = DataStoreOrchestrator(env_paths=env_files)
    databricks_driver = DatabricksDriver(orchestrator, "databricks_db:default")
    postgres_driver = PostgresDriver(orchestrator, "spend_plan_db:safe_user")
    replicator = DatabricksToPostgresReplicator(orchestrator, full_load_batch_size=1000, cdf_batch_size=500)

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

    dsl = SyncDSL(databricks_driver, postgres_driver)
    dsl.create_table("source_error_test", schema, key=None) \
       .setup_data(test_data)

    result = replicator.replicate("source_error_test", "target_error_test", "history_error_test")
    assert not result, "Replication should fail on duplicate key"

    # Teardown
    databricks_engine = create_engine(databricks_driver.engine.url)
    postgres_engine = create_engine(postgres_driver.engine.url)
    metadata = MetaData()
    source_table = Table("source_error_test", metadata, schema="default")
    target_table = Table("target_error_test", metadata, schema="safe_user")
    history_table = Table("history_error_test", metadata, schema="safe_user")
    metadata.drop_all(databricks_engine, tables=[source_table])
    metadata.drop_all(postgres_engine, tables=[target_table, history_table])
    databricks_engine.dispose()
    postgres_engine.dispose()