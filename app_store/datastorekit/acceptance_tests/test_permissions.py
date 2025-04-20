# datastorekit/acceptance_tests/test_permissions.py
import os
from sqlalchemy import create_engine, MetaData
from .dsl.crud_dsl import CrudDSL
from .drivers.postgres_driver import PostgresDriver
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.config import Config
from datastorekit.models.table_info import TableInfo
from datastorekit.permissions.manager import PermissionsManager
from datastorekit.permissions.models import Users, UserAccess

def test_authentication():
    """Test user authentication using DSL."""
    # Setup: Load .env.postgres_dev and initialize orchestrator
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres"), f"Invalid .env file: {env_path}"
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")
    permissions_manager = PermissionsManager(orchestrator, "spend_plan_test_db:safe_user")

    # Create test users using DSL
    dsl = CrudDSL(driver)
    permissions_manager.add_user("testuser", "password123")
    permissions_manager.add_user("admin", "admin123", is_group_admin=True)

    # Test
    user = permissions_manager.authenticate_user("testuser", "password123")
    assert user is not None
    assert user["username"] == "testuser"
    assert user["is_group_admin"] is False
    assert permissions_manager.authenticate_user("testuser", "wrong") is None

    # Teardown: Drop users and user_access tables
    engine = create_engine(driver.engine.url)
    metadata = MetaData()
    users_table = Users.__table__
    users_table.schema = "safe_user"
    user_access_table = UserAccess.__table__
    user_access_table.schema = "safe_user"
    metadata.drop_all(engine, tables=[users_table, user_access_table])
    engine.dispose()

def test_access_control():
    """Test table access permissions using DSL."""
    # Setup: Load .env.postgres_dev and initialize orchestrator
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres"), f"Invalid .env file: {env_path}"
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    driver = PostgresDriver(orchestrator, "spend_plan_test_db:safe_user")
    permissions_manager = PermissionsManager(orchestrator, "spend_plan_test_db:safe_user")

    # Create test users and access using DSL
    dsl = CrudDSL(driver)
    permissions_manager.add_user("testuser", "password123")
    permissions_manager.add_user("admin", "admin123", is_group_admin=True)
    table_info = TableInfo(
        table_name="spend_plan",
        keys="unique_id",
        scd_type="type1",
        datastore_key="spend_plan_test_db:safe_user",
        columns={"unique_id": "Integer", "category": "String", "amount": "Float"},
        permissions_manager=permissions_manager
    )
    permissions_manager.add_user_access("testuser", table_info, "read")

    # Test
    assert permissions_manager.check_access("testuser", table_info, "read") is True
    assert permissions_manager.check_access("testuser", table_info, "write") is False
    assert permissions_manager.check_access("admin", table_info, "write") is True

    # Teardown: Drop users and user_access tables
    engine = create_engine(driver.engine.url)
    metadata = MetaData()
    users_table = Users.__table__
    users_table.schema = "safe_user"
    user_access_table = UserAccess.__table__
    user_access_table.schema = "safe_user"
    metadata.drop_all(engine, tables=[users_table, user_access_table])
    engine.dispose()