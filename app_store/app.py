# app.py
import streamlit as st
import pandas as pd
import os
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.permissions.manager import PermissionsManager
from typing import Dict, List, Any

# Define paths to .env files
env_paths = [
    os.path.join(".env", ".env.postgres"),
    os.path.join(".env", ".env.databricks"),
    os.path.join(".env", ".env.mongodb")
]

# Initialize orchestrator
try:
    orchestrator = DataStoreOrchestrator(env_paths)
except Exception as e:
    st.error(f"Failed to initialize datastores: {e}")
    st.stop()

# Initialize permissions manager
permissions_manager = PermissionsManager(orchestrator, "spend_plan_db:safe_user")

# Streamlit app configuration
st.set_page_config(page_title="DataStoreKit Dashboard", layout="wide")

# Session state for user authentication
if "user" not in st.session_state:
    st.session_state.user = None

# Login UI
if not st.session_state.user:
    st.title("🔐 Login to DataStoreKit Dashboard")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Login"):
            user = permissions_manager.authenticate_user(username, password)
            if user:
                st.session_state.user = user
                st.success(f"Welcome, {username}!")
                st.experimental_rerun()
            else:
                st.error("Invalid username or password")
else:
    user = st.session_state.user
    st.title(f"🌟 DataStoreKit Dashboard - Welcome, {user['username']}")
    st.markdown("Manage and replicate data across datastores with ease!")

    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.user = None
        st.experimental_rerun()

    # Group admin: Manage users
    if user["is_group_admin"]:
        with st.sidebar.expander("Admin: Manage Users"):
            with st.form("add_user_form"):
                new_username = st.text_input("New Username")
                new_password = st.text_input("New Password", type="password")
                is_group_admin = st.checkbox("Group Admin")
                if st.form_submit_button("Add User"):
                    if permissions_manager.add_user(new_username, new_password, is_group_admin):
                        st.success(f"Added user: {new_username}")
                    else:
                        st.error("Failed to add user")

            with st.form("add_access_form"):
                target_username = st.text_input("Target Username")
                datastore_key = st.selectbox("Datastore", orchestrator.list_adapters(), key="admin_datastore")
                table_name = st.selectbox("Table", orchestrator.list_tables(*datastore_key.split(":")), key="admin_table")
                access_level = st.selectbox("Access Level", ["read", "write"])
                if st.form_submit_button("Add Access"):
                    if permissions_manager.add_user_access(target_username, datastore_key, table_name, access_level):
                        st.success(f"Added {access_level} access for {target_username} to {table_name}")
                    else:
                        st.error("Failed to add access")

    # Sidebar for datastore and table selection
    st.sidebar.header("Datastore Selection")
    user_access = permissions_manager.get_user_access(user["username"])
    accessible_tables = [(access["datastore_key"], access["table_name"], access["access_level"]) for access in user_access]
    if user["is_group_admin"]:
        accessible_tables = [(key, table, "write") for key in orchestrator.list_adapters() for table in orchestrator.list_tables(*key.split(":"))]
    
    if accessible_tables:
        selected_access = st.sidebar.selectbox(
            "Select Table",
            [(f"{key}:{table} ({access})", key, table, access) for key, table, access in accessible_tables],
            format_func=lambda x: x[0]
        )
        if selected_access:
            _, datastore_key, selected_table, access_level = selected_access
            source_db, source_schema = datastore_key.split(":")

            # Fetch table
            table_info = {"table_name": selected_table, "scd_type": "type1"}
            table = orchestrator.get_table(datastore_key, table_info)

            # Tabs for operations
            tabs = st.tabs(["🔍 Read", "✏️ Edit"] if access_level == "write" else ["🔍 Read"])

            # Read Tab
            with tabs[0]:
                st.header("Read Records")
                with st.expander("Filter Options"):
                    filter_category = st.text_input("Filter by Category")
                    filters = {"category": filter_category} if filter_category else {}
                try:
                    records = table.read(filters)
                    if records:
                        df = pd.DataFrame(records)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("No records found.")
                except Exception as e:
                    st.error(f"Failed to read: {e}")

            # Edit Tab (write access only)
            if access_level == "write":
                with tabs[1]:
                    st.header("Edit Records")
                    try:
                        records = table.read({})
                        df = pd.DataFrame(records)
                        if not df.empty:
                            edited_df = st.data_editor(df, num_rows="dynamic")
                            if st.button("Save Changes"):
                                table.update(edited_df.to_dict('records'), {})  # Pass all records as list
                                st.success("Changes saved!")
                        else:
                            st.info("No records to edit.")
                    except Exception as e:
                        st.error(f"Failed to edit: {e}")
    else:
        st.info("No accessible tables. Contact a group admin to grant access.")