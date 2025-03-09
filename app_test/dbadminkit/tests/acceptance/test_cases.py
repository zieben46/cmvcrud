import pytest
from dsl import DataOpsDSL

@pytest.fixture
def data_ops(docker_postgres):
    return DataOpsDSL(postgres_config=docker_postgres, databricks_config=None)  # Mock Databricks for test

@pytest.mark.acceptance
def test_should_sync_employees_table(data_ops):
    # Arrange: Setup test data in Postgres
    data_ops.setup_postgres_data(table_name="employees", record_count=100)
    
    # Act: Sync table from Postgres to Databricks
    data_ops.sync_table(source_table="employees", target_table="employees_target")
    
    # Assert: Verify data matches
    data_ops.assert_table_synced(source_table="employees", target_table="employees_target")