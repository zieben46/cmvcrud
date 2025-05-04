# datastorekit/acceptance_tests/test_replication.py
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from .dsl.sync_dsl import SyncDSL
from .drivers.databricks_driver import DatabricksDriver
from .drivers.postgres_driver import PostgresDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator
from datastorekit.models.table_info import TableInfo
from datastorekit.config import Config
import pytest

def test_databricks_to_postgres_replication():
    """Test replication from Databricks to PostgreSQL using DSL."""
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
    source_schema = {
        "unique_id": Integer,
        "secondary_key": String,
        "category": String,
        "amount": Float,
        "start_date": DateTime,
        "end_date": DateTime,
        "is_active": Boolean
    }
    databricks_dsl = SyncDSL(databricks_driver, postgres_driver)
    databricks_dsl.create_table("source_test", source_schema, key="unique_id,secondary_key") \
                  .setup_data([
                      {"unique_id": 1, "secondary_key": "A", "category": "Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
                      {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True}
                  ])

    # Execute replication
    result = replicator.replicate("source_test", "target_test", "history_test")
    assert result, "Replication failed"

    # Verify target table in PostgreSQL
    postgres_dsl = SyncDSL(postgres_driver, databricks_driver)
    postgres_dsl.select_table("target_test", key="unique_id,secondary_key") \
                .assert_table_has([
                    {"unique_id": 1, "secondary_key": "A", "category": "Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
                    {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True}
                ])

    # Simulate CDF changes (incremental merge)
    databricks_dsl.execute_crud("UPDATE", [{"category": "Updated Food"}], {"unique_id": 1, "secondary_key": "A"}) \
                  .execute_crud("CREATE", [{"unique_id": 3, "secondary_key": "C", "category": "Books", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True}])

    # Execute incremental replication
    result = replicator.replicate("source_test", "target_test", "history_test")
    assert result, "Incremental replication failed"

    # Verify updated target table
    postgres_dsl.assert_table_has([
        {"unique_id": 1, "secondary_key": "A", "category": "Updated Food", "amount": 50.0, "start_date": "2023-01-01", "end_date": None, "is_active": True},
        {"unique_id": 2, "secondary_key": "B", "category": "Travel", "amount": 100.0, "start_date": "2023-01-02", "end_date": None, "is_active": True},
        {"unique_id": 3, "secondary_key": "C", "category": "Books", "amount": 25.0, "start_date": "2023-01-03", "end_date": None, "is_active": True}
    ])

    # Verify history table
    history_dsl = SyncDSL(postgres_driver, databricks_driver)
    history_dsl.select_table("history_test", key="version") \
               .assert_table_has([
                   {"version": 1, "operation": "FULL_LOAD", "num_affected_rows": 2},
                   {"version": 2, "operation": "INCREMENTAL_MERGE", "num_affected_rows": 2}
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