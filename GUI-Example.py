import pandas as pd

def calculate_deltas(df_original: pd.DataFrame, df_modified: pd.DataFrame, primary_keys: list):
    """Compares original and modified DataFrames to detect inserts, updates, and deletes."""

    # Ensure primary keys exist
    if not set(primary_keys).issubset(df_original.columns) or not set(primary_keys).issubset(df_modified.columns):
        raise ValueError("❌ Primary keys must be present in both DataFrames.")

    # 🔹 Step 1: Detect Inserts (Rows in `df_modified` but not in `df_original`)
    df_merged = df_modified.merge(df_original, on=primary_keys, how='left', indicator=True)
    inserts = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    # 🔹 Step 2: Detect Deletes (Rows in `df_original` but not in `df_modified`)
    df_merged = df_original.merge(df_modified, on=primary_keys, how='left', indicator=True)
    deletes = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    # 🔹 Step 3: Detect Updates (Rows where values changed but primary keys match)
    common_keys = df_original.merge(df_modified, on=primary_keys, how='inner')
    updated_rows = common_keys[df_original.set_index(primary_keys).loc[common_keys.index].ne(
        df_modified.set_index(primary_keys).loc[common_keys.index]
    ).any(axis=1)]

    # return inserts.to_dict(orient="records"), updated_rows.to_dict(orient="records"), deletes.to_dict(orient="records")
    return [], [], []
    




import streamlit as st
import requests
import pandas as pd

st.title("Table Editor")

# 🔹 Step 1: Authenticate User and Get Token
auth_url = "http://127.0.0.1:8000/token"
username = "admin"   # Change as needed
password = "admin123" # Change as needed

# 🔹 Send Authentication Request
auth_response = requests.post(auth_url, data={"username": username, "password": password})

if auth_response.status_code == 200:
    token = auth_response.json().get("access_token")
    headers = {"Authorization": f"Bearer {token}"}  # ✅ Attach token to headers
else:
    st.error("❌ Authentication failed! Check username & password.")
    st.stop()  # 🚨 Stop execution if authentication fails

# 🔹 Step 2: Fetch Data from API with Authentication
table_name = "user1"
response = requests.get(f"http://127.0.0.1:8000/{table_name}/read", headers=headers)

if response.status_code == 200:
    json_data = response.json()

    # Handle possible empty or incorrect responses
    if isinstance(json_data, dict):  
        json_data = [json_data]  # Convert single dict to list
    elif not json_data:
        json_data = []  # Ensure it's a list if empty

    # 🔹 Convert to Pandas DataFrame
    df_original = pd.DataFrame(json_data)

    # # Handle empty DataFrame case
    # if df_original.empty:
    #     df_original = pd.DataFrame(columns=["id", "name", "age"])  # Change to your table columns

# else:
#     st.error(f"❌ Failed to fetch data: {response.text}")
#     df_original = pd.DataFrame(columns=["id", "name", "age"])

# 🔹 Step 3: Display Data Editor
edited_df = st.data_editor(df_original, num_rows="dynamic")

# 🔹 Step 3: Compute Deltas on Submit
if st.button("Submit Changes"):
    inserts, updates, deletes = calculate_deltas(df_original, edited_df, primary_keys=["id"])

    # 🔹 Step 4: Send Delta to FastAPI Backend
    if inserts:
        requests.post(f"http://127.0.0.1:8000/{table_name}/create", json=inserts)
    if updates:
        requests.put(f"http://127.0.0.1:8000/{table_name}/update", json=updates)
    if deletes:
        requests.delete(f"http://127.0.0.1:8000/{table_name}/delete", json=deletes)

    st.success("✅ Changes successfully submitted!")