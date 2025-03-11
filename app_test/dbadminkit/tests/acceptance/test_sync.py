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

    @pytest.mark.parametrize("source_type, target_type", [
        ("postgres", "databricks"),
        ("databricks", "postgres")
    ])
    def test_sync_operations(self, source_type, target_type):
        self.setUp(source_type, target_type)
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .sync_to_target("employees_target", sync_method="jdbc") \
                .assert_tables_synced()

    @pytest.mark.parametrize("source_type, target_type", [
        ("postgres", "databricks"),
        ("databricks", "postgres")
    ])
    def test_sync_with_invalid_method(self, source_type, target_type):
        self.setUp(source_type, target_type)
        with pytest.raises(ValueError):
            self.dsl.select_table("employees", "emp_id") \
                    .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                    .sync_to_target("employees_target", sync_method="invalid")