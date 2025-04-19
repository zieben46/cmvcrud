# datastorekit/acceptance_tests/test_transfers.py
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from datastorekit.dsl import SyncDSL
from datastorekit.drivers import PostgresDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.config import Config

def test_replication():
    """Test replication from source to target table using DSL."""
    # Setup: Load .env.postgres_dev and initialize orchestrator
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres"), f"Invalid .env file: {env_path}"
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    source_driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")
    target_driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")

    # Create temporary tables
    schema = {
        "id": Integer,
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
    dsl.create_table("spend_plan_test", schema, key="id") \
       .setup_data([
           {"id": 1, "category": "Food", "amount": 50.0},
           {"id": 2, "category": "Travel", "amount": 100.0}
       ])
    dsl.create_table("spend_plan_copy_test", schema, key="id")
    dsl.create_table("spend_plan_history_test", history_schema, key="version")

    # Test replication
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