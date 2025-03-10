import pytest
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.core.crud_types import CRUDOperation
from tests.dsl import CrudDSL

class TestCrud:
    def setUp(self):
        self.dsl = CrudDSL(DatabaseProfile.in_memory())

    def test_user_crud(self):
        self.dsl.select_table("employees", "emp_id") \
               .setup_data([{"emp_id": 1, "name": "Alice"}]) \
               .execute_crud(CRUDOperation.CREATE, {"emp_id": 2, "name": "Bob"}) \
               .assert_table_has([{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}])

    def test_update(self):  # No longer restricted to admin
        self.dsl.select_table("employees", "emp_id") \
               .setup_data([{"emp_id": 1, "name": "Alice"}]) \
               .execute_crud(CRUDOperation.UPDATE, {"emp_id": 1, "name": "Updated"}) \
               .assert_table_has([{"emp_id": 1, "name": "Updated"}])