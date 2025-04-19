# datastorekit/acceptance_tests/test_transfers.py
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from datastorekit.dsl import SyncDSL
from datastorekit.drivers import PostgresDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.config import Config
from datastorekit.models.table_info import TableInfo

def test_replication_with_history():
    """Test replication with history versioning using DSL."""
    # Setup: Load .env.postgres_dev and initialize orchestrator
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres"), f"Invalid .env file: {env_path}"
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    source_driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")
    target_driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")

    # Create temporary tables
    schema = {
        "unique_id": Integer,
        "category": String,
        "amount": Float,
        "start_date": DateTime,
        "end_date": DateTime,
        "is_active": Boolean
    }
    history_schema = {
        "version": Integer,
        "timestamp": DateTime,
        "operation": String,
        "operation_parameters": String,
        "num_affected_rows": BigInteger
    }
    dsl = SyncDSL(source_driver, target_driver)
    dsl.create_table("spend_plan_test", schema, key="unique_id") \
       .setup_data([
           {"unique_id": 1, "category": "Food", "amount": 50.0},
           {"unique_id": 2, "category": "Travel", "amount": 100.0}
       ])
    dsl.create_table("spend_plan_copy_test", schema, key="unique_id")
    dsl.create_table("spend_plan_history_test", history_schema, key="version") \
       .setup_data([
           {"version": 1, "timestamp": DateTime.now(), "operation": "INSERT", "operation_parameters": "{}", "num_affected_rows": 2}
       ])

    # Test replication (should use history since version 1)
    dsl.sync_to_target("spend_plan_copy_test", sync_method="full_load") \
       .assert_tables_synced()

    # Teardown: Drop temporary tables
    engine = create_engine(source_driver.engine.url)
    metadata = MetaData()
    source_test_table = Table("spend_plan_test", metadata, schema="safe_user")
    target_test_table = Table("spend_plan_copy_test", metadata, schema="safe_user")
    history_test_table = Table("spend_plan_history_test", metadata, schema="safe_user")
    metadata.drop_all(engine, tables=[source_test_table, target_test_table, history_test_table])
    engine.dispose()

def test_replication_no_history_table():
    """Test replication when history table is missing, triggering full load."""
    # Setup: Load .env.postgres_dev and initialize orchestrator
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres"), f"Invalid .env file: {env_path}"
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    source_driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")
    target_driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")

    # Create temporary source table
    schema = {
        "unique_id": Integer,
        "category": String,
        "amount": Float,
        "start_date": DateTime,
        "end_date": DateTime,
        "is_active": Boolean
    }
    dsl = SyncDSL(source_driver, target_driver)
    dsl.create_table("spend_plan_test", schema, key="unique_id") \
       .setup_data([
           {"unique_id": 1, "category": "Food", "amount": 50.0},
           {"unique_id": 2, "category": "Travel", "amount": 100.0}
       ])

    # Create temporary target table (to test dropping)
    dsl.create_table("spend_plan_copy_test", schema, key="unique_id") \
       .setup_data([
           {"unique_id": 3, "category": "Old", "amount": 0.0}
       ])

    # Test replication (history table missing, should drop target and full load)
    dsl.sync_to_target("spend_plan_copy_test", sync_method="full_load") \
       .assert_tables_synced()

    # Teardown: Drop temporary tables
    engine = create_engine(source_driver.engine.url)
    metadata = MetaData()
    source_test_table = Table("spend_plan_test", metadata, schema="safe_user")
    target_test_table = Table("spend_plan_copy_test", metadata, schema="safe_user")
    history_test_table = Table("spend_plan_history_test", metadata, schema="safe_user")
    metadata.drop_all(engine, tables=[source_test_table, target_test_table, history_test_table])
    engine.dispose()