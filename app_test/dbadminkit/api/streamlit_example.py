import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd  # Still used for display, but not for sync

# API base URL
API_URL = "http://localhost:8000"
MAX_ROWS = 10000  # Match sync_table limit

# Initialize session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.auth = None
    st.session_state.table_locked = False
    st.session_state.db_type = None
    st.session_state.table_name = None
    st.session_state.authorized_tables = []

# Login form
if not st.session_state.logged_in:
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        auth = HTTPBasicAuth(username, password)
        response = requests.get(
            f"{API_URL}/get-authorized-tables?db_type=postgres",
            auth=auth
        )
        if response.status_code == 200:
            st.session_state.logged_in = True
            st.session_state.auth = auth
            st.session_state.authorized_tables = response.json()["tables"]
            st.success("Logged in successfully!")
        else:
            st.error("Invalid credentials")
else:
    # Main app interface
    st.title("Table Editor")
    auth = st.session_state.auth

    # Database type selection
    db_type = st.selectbox("Database Type", ["postgres", "databricks"], key="db_type")
    
    # Fetch authorized tables if db_type changes
    if db_type != st.session_state.db_type:
        response = requests.get(
            f"{API_URL}/get-authorized-tables?db_type={db_type}",
            auth=auth
        )
        if response.status_code == 200:
            st.session_state.authorized_tables = response.json()["tables"]
            st.session_state.db_type = db_type
            st.session_state.table_name = None  # Reset table selection
            st.session_state.table_locked = False  # Reset lock status
        else:
            st.error("Failed to fetch authorized tables")
            st.session_state.authorized_tables = []

    # Table selection (only authorized tables)
    if st.session_state.authorized_tables:
        table_name = st.selectbox(
            "Table Name",
            options=st.session_state.authorized_tables,
            key="table_name_select"
        )
    else:
        st.warning("No tables available for this user in the selected database.")
        table_name = None

    # Lock table button (only if a table is selected and not locked)
    if table_name and not st.session_state.table_locked:
        if st.button("Lock Table"):
            response = requests.post(
                f"{API_URL}/lock-table?db_type={db_type}&table_name={table_name}",
                auth=auth
            )
            if response.status_code == 200:
                st.session_state.table_locked = True
                st.session_state.table_name = table_name
                st.success(f"Table '{table_name}' locked successfully!")
            else:
                st.error(response.json().get("detail", "Failed to lock table"))

    # Data editing and syncing (only if table is locked)
    if st.session_state.table_locked:
        # Fetch current data
        response = requests.post(
            f"{API_URL}/execute-crud?operation=read&target={db_type}&table_name={st.session_state.table_name}",
            auth=auth
        )
        if response.status_code == 200:
            data = response.json()["data"]  # List of dicts
            if len(data) > MAX_ROWS:
                st.warning(f"Data exceeds {MAX_ROWS} rows; only first {MAX_ROWS} rows shown.")
                data = data[:MAX_ROWS]
            
            # Convert to DataFrame for display only
            df = pd.DataFrame(data)
            st.write("Current Data:")
            edited_df = st.data_editor(df, num_rows="dynamic")
            edited_data = edited_df.to_dict('records')  # Convert back to list of dicts for API
        else:
            st.error("Failed to fetch data")
            edited_data = []

        # Submit changes
        if st.button("Submit Changes"):
            if len(edited_data) > MAX_ROWS:
                st.error(f"Cannot sync: Edited data exceeds {MAX_ROWS} rows.")
            else:
                response = requests.post(
                    f"{API_URL}/sync-table?target={db_type}&table_name={st.session_state.table_name}",
                    json=edited_data,
                    auth=auth
                )
                if response.status_code == 200:
                    st.success(f"Table synced successfully! Rows affected: {response.json()['rows_affected']}")
                else:
                    st.error(response.json().get("detail", "Failed to sync table"))  # Show detailed error

        # Unlock table
        if st.button("Unlock Table"):
            response = requests.post(
                f"{API_URL}/unlock-table?db_type={db_type}&table_name={st.session_state.table_name}",
                auth=auth
            )
            if response.status_code == 200:
                st.session_state.table_locked = False
                st.success("Table unlocked!")
            else:
                st.error(response.json().get("detail", "Failed to unlock table"))