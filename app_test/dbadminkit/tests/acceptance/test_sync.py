import pytest
from dsl import SyncDSL, PostgresDriver, DatabricksDriver
from dbadminkit.core.database_profile import DatabaseProfile

class TestSync:
    def setUp(self, source_type, target_type):
        """Initialize source and target drivers based on types."""
        drivers = {
            "postgres": PostgresDriver(DatabaseProfile.in_memory()),
            "databricks": DatabricksDriver(DatabaseProfile.test_databricks())
        }
        self.source_driver = drivers[source_type]
        self.target_driver = drivers[target_type]
        self.dsl = SyncDSL(self.source_driver, self.target_driver)

    def test_sync_operations_postgres_to_databricks(self):
        self.setUp("postgres", "databricks")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .sync_to_target("employees_target", sync_method="jdbc") \
                .assert_tables_synced()

    def test_sync_operations_databricks_to_postgres(self):
        self.setUp("databricks", "postgres")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .sync_to_target("employees_target", sync_method="jdbc") \
                .assert_tables_synced()

    def test_sync_with_invalid_method_postgres_to_databricks(self):
        self.setUp("postgres", "databricks")
        with pytest.raises(ValueError):
            self.dsl.select_table("employees", "emp_id") \
                    .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                    .sync_to_target("employees_target", sync_method="invalid")

    def test_sync_with_invalid_method_databricks_to_postgres(self):
        self.setUp("databricks", "postgres")
        with pytest.raises(ValueError):
            self.dsl.select_table("employees", "emp_id") \
                    .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                    .sync_to_target("employees_target", sync_method="invalid")