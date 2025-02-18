

# 🚀 How to Start the FastAPI Application

Follow these steps to run your FastAPI application successfully.

---

## ✅ 1️⃣ Ensure Dependencies Are Installed

First, install all required dependencies using:

```sh
pip install fastapi uvicorn sqlalchemy pandas python-jose
```

If you have a `requirements.txt` file:

```sh
pip install -r requirements.txt
```

📌 **Ensure your virtual environment is activated if using one:**

```sh
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

---

## ✅ 2️⃣ Run the FastAPI Application

FastAPI applications are run using **Uvicorn**, the ASGI server.

```sh
uvicorn main:app --reload
```

📌 **Explanation:**
- `main` → The Python file where `FastAPI()` is instantiated.
- `app` → The FastAPI instance in that file.
- `--reload` → Enables **auto-reload** (for development).

---

## ✅ 3️⃣ Open the API in Your Browser

Once the server is running, you can access your API:

- **Swagger UI (Interactive API Docs)** → [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc Documentation** → [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

🚀 **You can test API calls directly from the Swagger UI!**

---

## ✅ 4️⃣ Test the API Using `curl`

Try retrieving data from your API:

```sh
curl -X GET "http://127.0.0.1:8000/person/read" -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## ✅ 5️⃣ Running in Production

For production, run Uvicorn with more workers:

```sh
uvicorn api_controller:app --host 0.0.0.0 --port 8000 --workers 4
```

or use **Gunicorn** with Uvicorn workers:

```sh
gunicorn -w 4 -k uvicorn.workers.UvicornWorker api_controller:app
```

---

## ✅ 6️⃣ (Optional) Create a `Makefile` for Easy Startup

To simplify starting the app, create a **`Makefile`**:

```makefile
.PHONY: start

start:
	uvicorn api_controller:app --reload
```

Then run:

```sh
make start
```

---

## 🎯 Now Your API is Running!

🚀 You can interact with it using Swagger UI or HTTP requests.


🔹 1️⃣ Obtain Authentication Token
=============================

## Request Token (Login)
```bash
curl -X POST "http://127.0.0.1:8000/token" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "username=admin&password=admin123"
```

## Response (JWT Token)
```json
{
    "access_token": "your_token_here",
    "token_type": "bearer"
}
```


🔹 2️⃣ Lock a Table (Only One User Can Lock at a Time)
=============================

# Admin Locks `person` Table
```bash
curl -X POST "http://127.0.0.1:8000/person/lock" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```
# Response
{
    "message": "🔒 Table `person` is now locked by `admin`"
}


🔹 3️⃣ Attempting to Update While Table is Locked
=============================

# Another User Tries to Update (`Bob` Attempts Update)
```bash
curl -X PUT "http://127.0.0.1:8000/person/update/1" \
     -H "Authorization: Bearer OTHER_USER_ACCESS_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"name": "Bob Updated"}'
```
# Response (Blocked)
{
    "detail": "❌ Table `person` is locked by `admin`"
}


🔹 4️⃣ Unlock the Table (Only the Locking User Can Unlock)
=============================

# Admin Unlocks the Table
```bash
curl -X POST "http://127.0.0.1:8000/person/unlock" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```
# Response
{
    "message": "🔓 Table `person` is now unlocked."
}


🔹 5️⃣ Create a New Record
=============================

# Add a New Entry
```bash
curl -X POST "http://127.0.0.1:8000/person/create" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"name": "Alice", "age": 30}'
```
# Response
{
    "message": "✅ Entry added to `person` by `admin`"
}


🔹 6️⃣ Read All Entries
=============================

# Fetch All Data
```bash
curl -X GET "http://127.0.0.1:8000/person/read" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```
# Response
[
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
]


🔹 7️⃣ Update an Entry (After Unlocking)
=============================
```bash
# Update Alice’s Age
curl -X PUT "http://127.0.0.1:8000/person/update/1" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"age": 31}'
```
# Response
{
    "message": "🔄 Entry `1` updated in `person` by `admin`"
}


🔹 8️⃣ Delete an Entry (Only Admins Can Delete)
=============================
```bash
# Delete Bob’s Record
curl -X DELETE "http://127.0.0.1:8000/person/delete/2" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```
# Response
{
    "message": "🗑️ Entry `2` deleted from `person` by `admin`"
}


🔹 9️⃣ Download Data as a CSV
=============================
```bash
# Download CSV
curl -X GET "http://127.0.0.1:8000/person/download" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```


🔹 1️⃣0️⃣ Full Example Using Python
=============================

import requests

BASE_URL = "http://127.0.0.1:8000"
TOKEN = "YOUR_ACCESS_TOKEN"

headers = {"Authorization": f"Bearer {TOKEN}"}

# 🔒 Lock Table
resp = requests.post(f"{BASE_URL}/person/lock", headers=headers)
print(resp.json())

# 🚀 Try to Update While Locked
update_payload = {"name": "Alice Updated"}
resp = requests.put(f"{BASE_URL}/person/update/1", json=update_payload, headers=headers)
print(resp.json())  # Should work because we locked it

# 🔓 Unlock Table
resp = requests.post(f"{BASE_URL}/person/unlock", headers=headers)
print(resp.json())

=============================
=============================
=============================
Front-End interaction
✅ Table Locking 
=============================
=============================
=============================
```python


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

    return inserts.to_dict(orient="records"), updated_rows.to_dict(orient="records"), deletes.to_dict(orient="records")


import streamlit as st
import requests

st.title("Table Editor")

# 🔹 Step 1: Load Data from API
table_name = "your_table"
response = requests.get(f"http://127.0.0.1:8000/{table_name}/read")
df_original = pd.DataFrame(response.json())  # Save the original state
df_modified = df_original.copy()

# 🔹 Step 2: Allow User to Edit Data
edited_df = st.data_editor(df_modified)

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


```