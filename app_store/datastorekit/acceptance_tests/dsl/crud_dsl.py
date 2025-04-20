# datastorekit/acceptance_tests/dsl/crud_dsl.py
import logging
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime, Boolean
from ..drivers.database_driver import DatabaseDriver

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DatabaseDSL:
    def __init__(self, driver: DatabaseDriver):
        self.driver = driver
        self.selected_table = None

    def create_table(self, table_name: str, schema: Dict[str, Any], key: str = "id"):
        """Create a table with the given schema."""
        if not schema:
            raise ValueError("Schema must be provided")
        self.selected_table = {"table_name": table_name, "key": key}
        self.driver.create_table(table_name, schema)
        logger.info(f"Created table {table_name} with schema {schema}")
        return self

    def select_table(self, table_name: str, key: str = "id"):
        """Select a table for operations."""
        self.selected_table = {"table_name": table_name, "key": key}
        return self

    def setup_data(self, records: List[Dict[str, Any]]):
        """Insert initial data into the selected table."""
        if not self.selected_table:
            raise ValueError("No table selected")
        self.driver.create(self.selected_table, records)
        logger.info(f"Inserted {len(records)} records into {self.selected_table['table_name']}")
        return self

    def assert_table_has(self, expected_records: List[Dict[str, Any]]):
        """Assert the table contains the expected records."""
        if not self.selected_table:
            raise ValueError("No table selected")
        actual_data = self.driver.read(self.selected_table, {})
        actual_df = pd.DataFrame(actual_data)
        expected_df = pd.DataFrame(expected_records)
        pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
        logger.info(f"Assertion passed for table {self.selected_table['table_name']}")
        return self

    def assert_record_exists(self, filters: Dict[str, Any], expected_data: Dict[str, Any]):
        """Assert a single record matches the expected data."""
        if not self.selected_table:
            raise ValueError("No table selected")
        data = self.driver.read(self.selected_table, filters)
        assert len(data) == 1, "Record not found"
        for key, value in expected_data.items():
            assert data[0][key] == value, f"Mismatch in {key}"
        return self

class CrudDSL(DatabaseDSL):
    def execute_crud(self, operation: str, data: List[Dict[str, Any]], filters: Dict[str, Any] = None):
        """Execute a CRUD operation on the selected table."""
        if not self.selected_table:
            raise ValueError("No table selected")
        if operation == "CREATE":
            self.driver.create(self.selected_table, data)
        elif operation == "READ":
            return self.driver.read(self.selected_table, filters or {})
        elif operation == "UPDATE":
            self.driver.update(self.selected_table, data, filters or {})
        elif operation == "DELETE":
            self.driver.delete(self.selected_table, filters or {})
        return self