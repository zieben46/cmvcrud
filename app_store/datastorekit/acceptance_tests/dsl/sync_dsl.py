# datastorekit/acceptance_tests/dsl/sync_dsl.py
import logging
from typing import Dict, Any, List
import pandas as pd
from ..drivers.database_driver import DatabaseDriver
from .crud_dsl import DatabaseDSL

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SyncDSL(DatabaseDSL):
    def __init__(self, source_driver: DatabaseDriver, target_driver: DatabaseDriver):
        super().__init__(source_driver)
        self.target_driver = target_driver
        self.target_table = None

    def sync_to_target(self, target_table: str, sync_method: str = "full_load"):
        """Sync data from source to target table."""
        if not self.selected_table:
            raise ValueError("No source table selected")
        self.target_table = {"table_name": target_table, "key": self.selected_table["key"]}
        self.driver.sync_to(self.selected_table, self.target_driver, target_table, sync_method)
        return self

    def assert_tables_synced(self):
        """Assert source and target tables have identical data."""
        if not self.selected_table or not self.target_table:
            raise ValueError("Tables not selected")
        source_data = self.driver.read(self.selected_table, {})
        target_data = self.target_driver.read(self.target_table, {})
        source_df = pd.DataFrame(source_data)
        target_df = pd.DataFrame(target_data)
        pd.testing.assert_frame_equal(source_df, target_df, check_like=True)
        return self