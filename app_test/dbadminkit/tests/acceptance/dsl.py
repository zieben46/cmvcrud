from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.database_manager import DBManager
from dbadminkit.core.crud_types import CRUDOperation
from typing import Dict, Any, List
import pandas as pd

class BaseDSL:
    """Shared base class for common DSL methods."""
    def __init__(self, config: DatabaseProfile):
        self.manager = DBManager(config)
        self.selected_table = None
        self.env = "test"

    def select_table(self, table_name: str, key: str = "id"):
        """Pick a table to operate on."""
        self.selected_table = {"table_name": table_name, "key": key}
        return self

    def setup_data(self, records: List[Dict[str, Any]]):
        """Insert initial test data into a table."""
        if not self.selected_table:
            raise ValueError("No table selected")
        with self.manager.engine.begin() as session:
            self.manager.get_table(self.selected_table).scd_handler.create(records, session)
        return self

    def assert_table_has(self, expected_records: List[Dict[str, Any]]):
        """Verify table contents match expected records."""
        if not self.selected_table:
            raise ValueError("No table selected")
        actual_data = self.manager.perform_crud(self.selected_table, CRUDOperation.READ, {})
        actual_df = pd.DataFrame(actual_data)
        expected_df = pd.DataFrame(expected_records)
        pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
        return self

class CrudDSL(BaseDSL):
    """DSL for CRUD operations."""
    def execute_crud(self, operation: CRUDOperation, data: Dict[str, Any]):
        """Execute a CRUD operation on the selected table."""
        if not self.selected_table:
            raise ValueError("No table selected")
        self.manager.perform_crud(self.selected_table, operation, data)
        return self

class SyncDSL(BaseDSL):
    """DSL for data mirroring/moving operations."""
    def __init__(self, source_config: DatabaseProfile, target_config: DatabaseProfile):
        super().__init__(source_config)
        self.target_manager = DBManager(target_config)

    def sync_to_target(self, target_table: str, sync_method: str = "jdbc"):
        """Sync the selected table to the target using the specified method."""
        if not self.selected_table:
            raise ValueError("No table selected")
        if sync_method == "jdbc":
            self.manager.transfer_with_jdbc(self.selected_table, self.target_manager.config)
        elif sync_method == "csv":
            self.manager.transfer_with_csv_copy(self.selected_table, self.target_manager)
        else:
            raise ValueError(f"Unsupported sync method: {sync_method}")
        return self

    def assert_tables_synced(self, target_table: str):
        """Verify source and target tables are synced."""
        if not self.selected_table:
            raise ValueError("No table selected")
        source_data = self.manager.perform_crud(self.selected_table, CRUDOperation.READ, {})
        target_data = self.target_manager.perform_crud({"table_name": target_table, "key": self.selected_table["key"]}, CRUDOperation.READ, {})
        source_df = pd.DataFrame(source_data)
        target_df = pd.DataFrame(target_data)
        pd.testing.assert_frame_equal(source_df, target_df, check_like=True)
        return self