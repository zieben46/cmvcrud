# app.py
import streamlit as st
import pandas as pd
import os
import json
from sqlalchemy.orm import Session
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.db_table import DBTable
from datastorekit.models.table_info import TableInfo
from datastorekit.permissions.manager import PermissionsManager
from typing import Dict, List, Any
from sqlalchemy import Integer, String, Float, DateTime, Boolean

# Define paths to .env files
env_paths = [
    os.path.join(".env", ".env.postgres"),
    os.path.join(".env", ".env.databricks"),
    os.path.join(".env", ".env.mongodb"),
    os.path.join(".env", ".env.csv"),
    os.path.join(".env", ".env.inmemory")
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

    # Group admin: Manage users and TableInfo
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
                table_name = st.text_input("Table Name")
                access_level = st.selectbox("Access Level", ["read", "write"])
                if st.form_submit_button("Add Access"):
                    table_info = TableInfo(
                        table_name=table_name,
                        keys="unique_id",
                        scd_type="type1",
                        datastore_key="spend_plan_db:safe_user",
                        columns={"unique_id": "Integer", "category": "String", "amount": "Float"},
                        permissions_manager=permissions_manager
                    )
                    if permissions_manager.add_user_access(target_username, table_info, access_level):
                        st.success(f"Added {access_level} access for {target_username} to {table_name}")
                    else:
                        st.error("Failed to add access")

        with st.sidebar.expander("Admin: Manage Tables"):
            with st.form("add_table_info_form"):
                table_name = st.text_input("Table Name")
                keys = st.text_input("Keys (comma-separated)", "unique_id")
                scd_type = st.selectbox("SCD Type", ["type0", "type1", "type2"])
                schedule_frequency = st.selectbox("Schedule Frequency", ["hourly", "daily", "weekly"])
                enabled = st.checkbox("Enabled", value=True)
                columns = st.text_area("Columns (JSON, optional)", '{"unique_id": "Integer", "category": "String", "amount": "Float"}')
                if st.form_submit_button("Add Table"):
                    try:
                        columns_dict = json.loads(columns) if columns.strip() else None
                        with orchestrator.adapters["spend_plan_db:safe_user"].session_factory() as session:
                            table_info = TableInfo(
                                table_name=table_name,
                                keys=keys,
                                scd_type=scd_type,
                                datastore_key="spend_plan_db:safe_user",
                                schedule_frequency=schedule_frequency,
                                enabled=enabled,
                                columns=columns_dict
                            )
                            session.add(table_info)
                            session.commit()
                            st.success(f"Added table: {table_name}")
                    except Exception as e:
                        st.error(f"Failed to add table: {e}")

            # Display TableInfo records
            with orchestrator.adapters["spend_plan_db:safe_user"].session_factory() as session:
                table_infos = session.query(TableInfo).all()
                if table_infos:
                    df = pd.DataFrame([ti.to_dict() for ti in table_infos])
                    st.subheader("Registered Tables")
                    st.dataframe(df, use_container_width=True)

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

            # Fetch table with TableInfo
            table_info = TableInfo(
                table_name=selected_table,
                keys="unique_id",
                scd_type="type1",
                datastore_key=datastore_key,
                columns={"unique_id": "Integer", "category": "String", "amount": "Float"},
                permissions_manager=permissions_manager
            )
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
                    if table_info.has_access(user["username"], "read"):
                        records = table.read(filters)
                        if records:
                            df = pd.DataFrame(records)
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.info("No records found.")
                    else:
                        st.error("No read access to this table.")
                except Exception as e:
                    st.error(f"Failed to read: {e}")

            # Edit Tab (write access only)
            if access_level == "write":
                with tabs[1]:
                    st.header("Edit Records")
                    try:
                        if table_info.has_access(user["username"], "write"):
                            records = table.read({})
                            df = pd.DataFrame(records)
                            if not df.empty:
                                edited_df = st.data_editor(df, num_rows="dynamic")
                                if st.button("Save Changes"):
                                    table.update(edited_df.to_dict('records'), {})
                                    st.success("Changes saved!")
                            else:
                                st.info("No records to edit.")
                        else:
                            st.error("No write access to this table.")
                    except Exception as e:
                        st.error(f"Failed to edit: {e}")
    else:
        st.info("No accessible tables. Contact a group admin to grant access.")