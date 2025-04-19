# datastorekit/acceptance_tests/test_crud.py
import os
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float
from datastorekit.dsl import CrudDSL
from datastorekit.drivers import PostgresDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.config import Config
from datastorekit.models.table_info import TableInfo

def test_crud_operations():
    """Test CRUD operations on a temporary spend_plan table using DSL."""
    # Setup: Load .env.postgres_dev and initialize orchestrator
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres"), f"Invalid .env file: {env_path}"
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")

    # Create temporary table
    schema = {
        "unique_id": Integer,
        "category": String,
        "amount": Float
    }
    table_info = TableInfo(table_name="spend_plan_test", columns=schema, key="unique_id")
    dsl = CrudDSL(driver)
    dsl.create_table("spend_plan_test", schema, key="unique_id") \
       .setup_data([
           {"unique_id": 1, "category": "Food", "amount": 50.0},
           {"unique_id": 2, "category": "Travel", "amount": 100.0}
       ])

    # Test CRUD operations
    dsl.execute_crud("CREATE", [{"unique_id": 3, "category": "Books", "amount": 25.0}]) \
       .assert_table_has([
           {"unique_id": 1, "category": "Food", "amount": 50.0},
           {"unique_id": 2, "category": "Travel", "amount": 100.0},
           {"unique_id": 3, "category": "Books", "amount": 25.0}
       ])

    dsl.execute_crud("UPDATE", [{"amount": 75.0}], {"unique_id": 1}) \
       .assert_record_exists({"unique_id": 1}, {"category": "Food", "amount": 75.0})

    dsl.execute_crud("DELETE", [], {"unique_id": 1}) \
       .assert_table_has([
           {"unique_id": 2, "category": "Travel", "amount": 100.0},
           {"unique_id": 3, "category": "Books", "amount": 25.0}
       ])

    # Teardown: Drop temporary table
    engine = create_engine(driver.engine.url)
    metadata = MetaData()
    test_table = Table("spend_plan_test", metadata, schema="safe_user")
    metadata.drop_all(engine, tables=[test_table])
    engine.dispose()