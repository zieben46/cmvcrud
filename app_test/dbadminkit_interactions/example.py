from dbadminkit.core.config import DBConfig
from dbadminkit.core.crud_operations import CRUDOperation
from dbadminkit.admin_operations import AdminDBOps

# Databricks config for SparkEngine
db_config = DBConfig.live_databricks(
    host="adb-1234567890.1.azuredatabricks.net",
    token="dapi1234567890abcdef1234567890abcdef",
    http_path="/sql/1.0/endpoints/1234567890abcdef"
)
db_ops = AdminDBOps(db_config)

# Postgres config for DBEngine
pg_config = DBConfig.live_postgres()  # Uses PG_* env vars by default
pg_ops = AdminDBOps(pg_config)

# Table info (same for both Databricks and Postgres)
table_info = {"table_name": "employees_target", "key": "emp_id", "scd_type": "type1"}

# Step 1: Query Databricks using SparkEngine (PySpark)
result = db_ops.perform_crud(table_info, CRUDOperation.READ, {"emp_id": 1})
print("Databricks read result:", result)

# Step 2: Save the result to Postgres using DBEngine (SQLAlchemy)
if result:  # Ensure there's data to save
    pg_ops.perform_crud(table_info, CRUDOperation.CREATE, result[0])  # Assuming single record for simplicity
    print("Saved to Postgres:", result[0])

# Optional: Verify the save by reading from Postgres
pg_result = pg_ops.perform_crud(table_info, CRUDOperation.READ, {"emp_id": 1})
print("Postgres read result:", pg_result)





from dbadminkit.core.config import DBConfig
from dbadminkit.admin_operations import AdminDBOps

# Databricks config for SparkEngine
db_config = DBConfig.live_databricks(
    host="adb-1234567890.1.azuredatabricks.net",
    token="dapi1234567890abcdef1234567890abcdef",
    http_path="/sql/1.0/endpoints/1234567890abcdef"
)
db_ops = AdminDBOps(db_config)

# Postgres config for DBEngine
pg_config = DBConfig.live_postgres()  # Uses PG_* env vars by default
pg_ops = AdminDBOps(pg_config)

# Table info for Postgres
target_table_info = {"table_name": "employees_target", "key": "emp_id", "scd_type": "type1"}

# S3 path to CSV
s3_path = "s3://my-bucket/path/to/employees.csv"

# Import CSV from S3 to Postgres
row_count = db_ops.import_s3_csv_to_postgres(s3_path, target_table_info, pg_config)
print(f"Successfully imported {row_count} records from S3 to Postgres")

# Verify a sample from Postgres
result = pg_ops.perform_crud(target_table_info, CRUDOperation.READ, {"emp_id": 1})
print("Postgres read result:", result)







from dbadminkit.core.config import DBConfig
from dbadminkit.admin_operations import AdminDBOps

# Configs
pg_config = DBConfig.live_postgres()
db_config = DBConfig.live_postgres()  # Hypothetical

pg_ops = AdminDBOps(pg_config)  # Source: Postgres
db_ops = AdminDBOps(db_config)  # Target: Databricks

# Table info
source_table_info = {
    "table_name": "employees_source",
    "key": "emp_id",
    "scd_type": "type2"
}
target_table_info = {
    "table_name": "employees_target",
    "key": "emp_id",
    "scd_type": "type1"  # Target can be any type
}

# Sync versions 1 to 3 from Postgres to Databricks
applied_count = db_ops.sync_scd2_versions(pg_ops, source_table_info, target_table_info, min_version=1, max_version=3)
print(f"Applied {applied_count} changes from Postgres to Databricks")





from dbadminkit.etl_trigger import ETLTrigger

# ETL Trigger
etl = ETLTrigger("http://etl-server:8080/jobs")
etl_params = {"source_table": "employees_target", "target_table": "data_warehouse"}

# Trigger ETL job
response = etl.trigger_job("load_warehouse", etl_params)
print("ETL job response:", response)









import streamlit as st
import pandas as pd
from passlib.hash import pbkdf2_sha256
from dbadminkit.core.config import DBConfig
from dbadminkit.core.crud_operations import CRUDOperation
from dbadminkit.admin_operations import AdminDBOps

# Database config
pg_config = DBConfig.live_postgres()
db_ops = AdminDBOps(pg_config)
table_info = {"table_name": "employees", "key": "emp_id", "scd_type": "type2"}
users_table_info = {"table_name": "users", "key": "username", "scd_type": "type1"}
locks_table_info = {"table_name": "table_locks", "key": "table_name", "scd_type": "type1"}

# Session state
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
    st.session_state["username"] = None
if "table_locked" not in st.session_state:
    st.session_state["table_locked"] = False
    st.session_state["locked_by"] = None

# Authentication
def check_password(username, password):
    user_data = db_ops.perform_crud(users_table_info, CRUDOperation.READ, {"username": username})
    if user_data and len(user_data) > 0:
        stored_hash = user_data[0]["password_hash"]
        return pbkdf2_sha256.verify(password, stored_hash)
    return False

# Lock status
def get_lock_status(table_name):
    lock_data = db_ops.perform_crud(locks_table_info, CRUDOperation.READ, {"table_name": table_name})
    if lock_data and len(lock_data) > 0:
        return lock_data[0]["locked_by"]
    return None

# Login UI
if not st.session_state["authenticated"]:
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if check_password(username, password):
            st.session_state["authenticated"] = True
            st.session_state["username"] = username
            st.success(f"Welcome, {username}!")
            locked_by = get_lock_status(table_info["table_name"])
            if locked_by:
                st.session_state["table_locked"] = True
                st.session_state["locked_by"] = locked_by
            else:
                st.session_state["table_locked"] = False
                st.session_state["locked_by"] = None
        else:
            st.error("Invalid username or password")
else:
    st.title(f"Employee Manager (Logged in as {st.session_state['username']})")
    
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.session_state["username"] = None
        st.session_state["table_locked"] = False
        st.session_state["locked_by"] = None
        st.rerun()

    # Lock/Unlock table
    locked_by = get_lock_status(table_info["table_name"])
    if locked_by:
        st.session_state["table_locked"] = True
        st.session_state["locked_by"] = locked_by
        st.warning(f"Table is locked by {locked_by}")
        if locked_by == st.session_state["username"]:
            if st.button("Unlock Table"):
                db_ops.perform_crud(locks_table_info, CRUDOperation.DELETE, {"table_name": table_info["table_name"]})
                st.session_state["table_locked"] = False
                st.session_state["locked_by"] = None
                st.success("Table unlocked!")
                st.rerun()
    else:
        st.session_state["table_locked"] = False
        st.session_state["locked_by"] = None
        if st.button("Lock Table"):
            db_ops.perform_crud(locks_table_info, CRUDOperation.CREATE, {
                "table_name": table_info["table_name"],
                "locked_by": st.session_state["username"]
            })
            st.session_state["table_locked"] = True
            st.session_state["locked_by"] = st.session_state["username"]
            st.success("Table locked by you!")
            st.rerun()

    # Fetch and edit data
    data = db_ops.perform_crud(table_info, CRUDOperation.READ, {})
    df = pd.DataFrame(data)

    if st.session_state["table_locked"] and st.session_state["locked_by"] != st.session_state["username"]:
        st.write("Table is locked by another user. You can only view the data.")
        st.dataframe(df)
    else:
        st.write("Edit Employees:")
        edited_df = st.data_editor(df, num_rows="dynamic")

        # Save changes
        if st.button("Save Changes"):
            if not st.session_state["table_locked"]:
                st.error("Please lock the table before saving changes!")
            elif st.session_state["locked_by"] == st.session_state["username"]:
                applied_count = db_ops.process_dataframe_edits(table_info, pd.DataFrame(data), edited_df)
                st.success(f"Saved {applied_count} changes successfully!")
                st.rerun()

    # Sync to Databricks
    if st.button("Sync to Databricks"):
        db_config = DBConfig.live_databricks(
            host="adb-1234567890.1.azuredatabricks.net",
            token="dapi1234567890abcdef1234567890abcdef",
            http_path="/sql/1.0/endpoints/1234567890abcdef"
        )
        db_target = AdminDBOps(db_config)
        target_table_info = {"table_name": "employees_target", "key": "emp_id", "scd_type": "type1"}
        applied_count = db_target.sync_scd2_versions(db_ops, table_info, target_table_info, min_version=1, max_version=3)
        st.success(f"Synced {applied_count} changes to Databricks")