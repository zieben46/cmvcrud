import os
import shutil
import pytest
from sqlalchemy import create_engine, Table, MetaData, Integer, String, Float
from pymongo import MongoClient
from .dsl.crud_dsl import CrudDSL
from .dsl.sync_dsl import SyncDSL
from .drivers.sqlalchemy_core_driver import SQLAlchemyCoreDriver
from .drivers.sqlalchemy_orm_driver import SQLAlchemyORMDriver
from .drivers.spark_driver import SparkDriver
from .drivers.mongodb_driver import MongoDBDriver
from .drivers.csv_driver import CSVDriver
from .drivers.inmemory_driver import InMemoryDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.config import Config
from datastorekit.models.table_info import TableInfo

# Schema for all tests
SCHEMA = {
    "unique_id": Integer,
    "category": String,
    "amount": Float
}

# Driver configurations
DRIVER_CONFIGS = [
    ("databricks", ".env.databricks_prod", SQLAlchemyCoreDriver),
    ("postgres", ".env.postgres_prod", SQLAlchemyORMDriver),
    ("spark", ".env.spark_prod", SparkDriver),
    ("mongodb", ".env.mongodb_prod", MongoDBDriver),
    ("csv", ".env.csv_prod", CSVDriver),
    ("inmemory", ".env.inmemory_prod", InMemoryDriver)
]

#bash command: pytest datastorekit/acceptance_tests/test_adapters.py -k "mongodb or csv" -v

def setup_orchestrator(env_file: str, db_type: str) -> DataStoreOrchestrator:
    """Setup orchestrator with a production-like .env file."""
    env_path = os.path.join(".env", env_file)
    assert Config.validate_env_file(env_path, db_type), f"Invalid .env file: {env_path}"
    return DataStoreOrchestrator(env_paths=[env_path])

def get_driver(db_type: str, orchestrator: DataStoreOrchestrator):
    """Get the driver and datastore key for the given db_type."""
    key = list(orchestrator.adapters.keys())[0]
    for config_db_type, _, driver_class in DRIVER_CONFIGS:
        if config_db_type == db_type:
            return driver_class(orchestrator, key), key
    raise ValueError(f"Unknown db_type: {db_type}")

def cleanup(driver, table_name: str):
    """Cleanup the test table/collection based on the driver type."""
    if isinstance(driver, (SQLAlchemyCoreDriver, SQLAlchemyORMDriver)):
        engine = create_engine(driver.engine.url)
        metadata = MetaData()
        schema_name = driver.orchestrator.adapters[driver.datastore_key].profile.schema
        test_table = Table(table_name, metadata, schema=schema_name)
        metadata.drop_all(engine, tables=[test_table])
        engine.dispose()
    elif isinstance(driver, SparkDriver):
        driver.spark.sql(f"DROP TABLE {driver.schema_name}.{table_name}")
    elif isinstance(driver, MongoDBDriver):
        driver.db[table_name].drop()
    elif isinstance(driver, CSVDriver):
        file_path = os.path.join(driver.base_dir, f"{table_name}.csv")
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(driver.base_dir):
            shutil.rmtree(driver.base_dir)
    elif isinstance(driver, InMemoryDriver):
        if table_name in driver.data:
            del driver.data[table_name]

@pytest.mark.parametrize("db_type,env_file,driver_class", DRIVER_CONFIGS)
def test_crud_operations(db_type, env_file, driver_class):
    """Test CRUD operations for all adapters using DSL."""
    orchestrator = setup_orchestrator(env_file, db_type)
    driver, key = get_driver(db_type, orchestrator)
    
    # Adjust key and data for MongoDB
    key_field = driver.key_field
    initial_data = [
        {key_field: 1, "category": "Food", "amount": 50.0},
        {key_field: 2, "category": "Travel", "amount": 100.0}
    ]
    expected_data = [
        {key_field: 1, "category": "Food", "amount": 50.0},
        {key_field: 2, "category": "Travel", "amount": 100.0},
        {key_field: 3, "category": "Books", "amount": 25.0}
    ]
    expected_after_delete = [
        {key_field: 2, "category": "Travel", "amount": 100.0},
        {key_field: 3, "category": "Books", "amount": 25.0}
    ]

    # Table info
    table_info = {
        "table_name": "test_table",
        "keys": key_field,
        "scd_type": "type1",
        "datastore_key": key,
        "columns": SCHEMA
    }

    # Run CRUD tests
    dsl = CrudDSL(driver)
    dsl.create_table("test_table", SCHEMA, key=key_field) \
       .setup_data(initial_data) \
       .execute_crud("CREATE", [{key_field: 3, "category": "Books", "amount": 25.0}]) \
       .assert_table_has(expected_data) \
       .execute_crud("UPDATE", [{"amount": 75.0}], {key_field: 1}) \
       .assert_record_exists({key_field: 1}, {"category": "Food", "amount": 75.0}) \
       .execute_crud("DELETE", [], {key_field: 1}) \
       .assert_table_has(expected_after_delete)

    # Cleanup
    cleanup(driver, "test_table")

def test_sync_mongodb_to_postgres():
    """Test synchronization from MongoDB to PostgreSQL using DSL."""
    mongo_orchestrator = setup_orchestrator(".env.mongodb_prod", "mongodb")
    postgres_orchestrator = setup_orchestrator(".env.postgres_prod", "postgres")
    mongo_driver, mongo_key = get_driver("mongodb", mongo_orchestrator)
    postgres_driver, postgres_key = get_driver("postgres", postgres_orchestrator)

    # MongoDB setup
    mongo_table_info = {
        "table_name": "test_table",
        "keys": mongo_driver.key_field,  # _id
        "scd_type": "type1",
        "datastore_key": mongo_key,
        "columns": SCHEMA
    }
    initial_data = [
        {mongo_driver.key_field: 1, "category": "Food", "amount": 50.0},
        {mongo_driver.key_field: 2, "category": "Travel", "amount": 100.0}
    ]

    # PostgreSQL setup
    postgres_table_info = {
        "table_name": "test_table_copy",
        "keys": postgres_driver.key_field,  # unique_id
        "scd_type": "type1",
        "datastore_key": postgres_key,
        "columns": SCHEMA
    }

    # Run synchronization test
    mongo_dsl = SyncDSL(mongo_driver, postgres_driver)
    postgres_dsl = CrudDSL(postgres_driver)
    mongo_dsl.create_table("test_table", SCHEMA, key=mongo_driver.key_field) \
             .setup_data(initial_data) \
             .select_table("test_table", key=mongo_driver.key_field)
    postgres_dsl.create_table("test_table_copy", SCHEMA, key=postgres_driver.key_field)
    mongo_dsl.sync_to_target("test_table_copy", sync_method="full_load") \
             .assert_tables_synced()

    # Cleanup
    cleanup(mongo_driver, "test_table")
    cleanup(postgres_driver, "test_table_copy")