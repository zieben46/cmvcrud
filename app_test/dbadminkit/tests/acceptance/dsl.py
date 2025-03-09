from drivers import PostgresDriver, DatabricksDriver
import os

class DataOpsDSL:
    def __init__(self, postgres_config, databricks_config):
        self.pg_driver = PostgresDriver(postgres_config)
        self.db_driver = DatabricksDriver(databricks_config) if databricks_config else None
        self.env = "test"  # Fixed for test environment

    def setup_postgres_data(self, table_name, record_count):
        self.pg_driver.setup_test_data(table_name, record_count)

    def sync_table(self, source_table, target_table):
        # Use CLI command to sync
        os.system(f"dbadminkit sync --source postgres:{source_table} --target databricks:{target_table} --env {self.env}")

    def assert_table_synced(self, source_table, target_table):
        source_count = self.pg_driver.get_row_count(source_table)
        target_count = self.db_driver.get_row_count(target_table)
        assert source_count == target_count, f"Expected {source_count} rows, got {target_count}"