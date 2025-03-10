import pytest
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.tests.acceptance.dsl import SyncDSL

class TestSync:
    def setUp(self):
        self.dsl = SyncDSL(DatabaseProfile.in_memory(), DatabaseProfile.test_databricks())

    def test_sync_with_jdbc(self):
        self.dsl.select_table("employees", "emp_id") \
               .setup_data([{"emp_id": 1, "name": "Alice"}]) \
               .sync_to_target("employees_target", sync_method="jdbc") \
               .assert_tables_synced("employees_target")

    def test_sync_with_csv(self):
        self.dsl.select_table("employees", "emp_id") \
               .setup_data([{"emp_id": 1, "name": "Alice"}]) \
               .sync_to_target("employees_target", sync_method="csv") \
               .assert_tables_synced("employees_target")

    # Placeholder for bidirectional test (Postgres -> Databricks already works; reverse not implemented yet)
    # def test_sync_from_databricks_to_postgres(self):
    #     dsl = SyncDSL(DatabaseProfile.test_databricks(), DatabaseProfile.in_memory())
    #     dsl.select_table("employees_target", "emp_id") \
    #        .setup_data([{"emp_id": 1, "name": "Bob"}]) \
    #        .sync_to_target("employees", sync_method="jdbc") \
    #        .assert_tables_synced("employees")