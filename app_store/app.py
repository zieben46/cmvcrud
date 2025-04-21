# app.py
import streamlit as st
import json
import pandas as pd
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Streamlit app title
st.title("DatastoreKit Explorer")

# Initialize orchestrator
try:
    env_paths = [
        os.path.join(".env", ".env.databricks"),
        os.path.join(".env", ".env.postgres"),
        os.path.join(".env", ".env.spark"),
        os.path.join(".env", ".env.csv"),
        os.path.join(".env", ".env.mongodb"),
        os.path.join(".env", ".env.inmemory")
    ]
    # Filter out non-existent .env files
    env_paths = [path for path in env_paths if os.path.exists(path)]
    if not env_paths:
        st.error("No valid .env files found. Please configure datastores.")
        st.stop()
    orchestrator = DataStoreOrchestrator(env_paths)
    st.success("Initialized orchestrator with adapters: " + ", ".join(orchestrator.list_adapters()))
except Exception as e:
    st.error(f"Failed to initialize orchestrator: {e}")
    st.stop()

# Select adapter
adapter_key = st.selectbox("Select Datastore", orchestrator.list_adapters())

# Select table
try:
    db_name, schema = adapter_key.split(":")
    tables = orchestrator.list_tables(db_name, schema)
    if not tables:
        st.warning(f"No tables found in {adapter_key}")
        st.stop()
    table_name = st.selectbox("Select Table", tables)
except Exception as e:
    st.error(f"Failed to list tables: {e}")
    st.stop()

# Define TableInfo
table_info = TableInfo(
    table_name=table_name,
    keys="unique_id,secondary_key",
    scd_type="type2",
    datastore_key=adapter_key,
    columns={
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
)

# Get table
try:
    table = orchestrator.get_table(adapter_key, table_info)
    st.success(f"Connected to table: {table_name}")
except Exception as e:
    st.error(f"Failed to connect to table: {e}")
    st.stop()

# Tabs for different operations
tab1, tab2, tab3 = st.tabs(["Execute SQL Query", "Read Records", "Table Info"])

with tab1:
    st.subheader("Execute SQL Query")
    default_query = (
        f"SELECT category, COUNT(*) as record_count, SUM(amount) as total_amount "
        f"FROM {db_name}.{schema}.{table_name} WHERE is_active = TRUE GROUP BY category"
    )
    sql_query = st.text_area("Enter SQL Query", default_query, height=150)
    params = st.text_input("Parameters (JSON, e.g., {\"is_active\": true})", "")
    if st.button("Execute Query"):
        try:
            parameters = json.loads(params) if params else None
            logger.info("Executing query", extra={"query": sql_query, "parameters": parameters, "table": table_name})
            results = table.execute_sql(sql_query, parameters)
            if results:
                st.write("Query Results:")
                df = pd.DataFrame(results)
                st.dataframe(df)
                st.json(results)
                # Add download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download Results as CSV",
                    data=csv,
                    file_name=f"{table_name}_results.csv",
                    mime="text/csv"
                )
            else:
                st.info("Query executed successfully, but no results returned.")
        except NotImplementedError:
            st.error("SQL execution is not supported for this adapter.")
        except Exception as e:
            st.error(f"Query failed: {e}")
            logger.error(f"Query failed: {e}", extra={"query": sql_query, "parameters": parameters})

with tab2:
    st.subheader("Read Records")
    filters = st.text_input("Filters (JSON, e.g., {\"category\": \"Food\"})", "")
    chunk_size = st.number_input("Chunk Size (for large datasets)", min_value=100, value=100000, step=100)
    if st.button("Read Records"):
        try:
            filters_dict = json.loads(filters) if filters else {}
            logger.info("Reading records", extra={"filters": filters_dict, "table": table_name, "chunk_size": chunk_size})
            results = []
            for chunk in table.read_chunks(filters_dict, chunk_size=chunk_size):
                results.extend(chunk)
                st.write(f"Read chunk of {len(chunk)} records")
            if results:
                st.write("Records:")
                df = pd.DataFrame(results)
                st.dataframe(df)
                st.json(results)
                # Add download button for records
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download Records as CSV",
                    data=csv,
                    file_name=f"{table_name}_records.csv",
                    mime="text/csv"
                )
            else:
                st.info("No records found.")
        except Exception as e:
            st.error(f"Read failed: {e}")
            logger.error(f"Read failed: {e}", extra={"filters": filters_dict})

with tab3:
    st.subheader("Table Information")
    st.write("Table Name:", table_name)
    st.write("Keys:", ", ".join(table.keys))
    st.write("SCD Type:", table.scd_type)
    st.write("Columns:", table_info.columns)
    if st.button("List All Tables"):
        st.write("Tables in Datastore:", tables)

# Sidebar for additional actions
with st.sidebar:
    st.header("Additional Actions")
    if st.button("Clear Cache"):
        try:
            table.cache()  # Assuming cache method exists
            st.success("Cache cleared.")
        except Exception as e:
            st.error(f"Failed to clear cache: {e}")
    st.write("DatastoreKit Version: 1.0.0")  # Update with actual version