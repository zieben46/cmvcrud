import streamlit as st
import pandas as pd
from abc import ABC, abstractmethod
from dbadminkit.core.database_manager import DBManager
from dbadminkit.core.databaseprofile import DatabaseProfile
from dbadminkit.core.crud_types import CRUDOperation
from dbadminkit.streamlit.lock import acquire_lock, release_lock, is_locked

class StreamlitDBApp(ABC):
    def __init__(self, config: DatabaseProfile, app_name: str):
        self.manager = DBManager(config)
        self.app_name = app_name
        self.lock_key = f"dbadminkit:{app_name}:lock"

    def run(self):
        """Run the app with locking."""
        if is_locked(self.lock_key):
            st.error("This app is currently locked by another user. Please try again later.")
            return

        if not acquire_lock(self.lock_key):
            st.error("Failed to acquire lock. Please try again.")
            return

        try:
            st.title(f"{self.app_name} Management")
            self._render_content()
        finally:
            release_lock(self.lock_key)

    def _render_content(self):
        """Render the DataFrame and submit button."""
        table_info = self.get_table_info()
        original_df = self._fetch_data(table_info)
        edited_df = st.data_editor(original_df, num_rows="dynamic")
        
        if st.button("Submit Changes"):
            self._submit_changes(table_info, original_df, edited_df)
            st.success("Changes saved!")

    def _fetch_data(self, table_info: dict) -> pd.DataFrame:
        data = self.manager.perform_crud(table_info, CRUDOperation.READ, {})
        return pd.DataFrame(data)

    def _submit_changes(self, table_info: dict, original_df: pd.DataFrame, edited_df: pd.DataFrame):
        self.manager.process_dataframe_edits(table_info, original_df, edited_df)

    @abstractmethod
    def get_table_info(self) -> dict:
        """Return table info specific to this app."""
        pass