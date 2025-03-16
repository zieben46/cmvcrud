import pytest
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.core.crud_types import CRUDOperation
from dsl import CrudDSL, PostgresDriver, DatabricksDriver

class TestCrud:
    def setUp(self, database_type):
        """Initialize the DSL and driver based on database type."""
        if database_type == "postgres":
            self.driver = PostgresDriver(DatabaseProfile.in_memory())
        elif database_type == "databricks":
            self.driver = DatabricksDriver(DatabaseProfile.test_databricks())
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        self.dsl = CrudDSL(self.driver)

    def test_crud_operations_postgres(self):
        self.setUp("postgres")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.CREATE, {"emp_id": 2, "name": "Bob"}) \
                .assert_table_has([{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}])

    def test_crud_operations_databricks(self):
        self.setUp("databricks")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.CREATE, {"emp_id": 2, "name": "Bob"}) \
                .assert_table_has([{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}])

    def test_update_postgres(self):
        self.setUp("postgres")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.UPDATE, {"name": "Updated"}, {"emp_id": 1}) \
                .assert_table_has([{"emp_id": 1, "name": "Updated"}])

    def test_update_databricks(self):
        self.setUp("databricks")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.UPDATE, {"name": "Updated"}, {"emp_id": 1}) \
                .assert_table_has([{"emp_id": 1, "name": "Updated"}])