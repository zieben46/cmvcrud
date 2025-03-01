

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


# Acceptance Testing with Docker for FastAPI CRUD App

This guide outlines how to set up acceptance testing for a FastAPI CRUD application within a CI/CD pipeline using Docker. The approach uses Docker to launch both the FastAPI app (with your `ReflectedTableModel` CRUD logic) and a PostgreSQL database, then performs HTTP requests to verify end-to-end behavior. This setup mimics a production-like DEV environment and ensures the full stack works as expected.

## Should Docker Mimic a Database Only, or Launch the API Too?

### Option 1: Docker Mimics Database Only
- **Setup**: Run just the database (e.g., PostgreSQL) in a Docker container; the app and tests run on the CI/CD runner.
- **Pros**: Simpler Docker config, easier debugging on the runner, mimics production database setup.
- **Cons**: Requires Python/dependencies on the runner, less portable.

### Option 2: Docker Launches API + Database (Full DEV Launch)
- **Setup**: Run both the FastAPI app and database in Docker containers, then test via HTTP requests.
- **Pros**: Fully containerized, portable, mimics production, simplifies CI/CD runner setup (just needs Docker).
- **Cons**: More complex Docker config, slightly slower startup.

### Recommendation
**Option 2** is preferred for acceptance testing in a CI/CD pipeline because it validates the full stack (API + database) in a reproducible, production-like way. The runner only needs Docker, and the setup doubles as a local DEV environment. We'll proceed with this approach using PostgreSQL.

## Implementation Steps

### Project Structure



your_project/
├── app/
│   ├── init.py
│   ├── main.py         # FastAPI app with ReflectedTableModel
│   └── models.py      # ReflectedTableModel definition
├── tests/
│   └── test_api.py    # Acceptance tests
├── Dockerfile         # Docker config for the app
├── docker-compose.yml # Defines app + database services
├── requirements.txt   # Python dependencies
└── .github/
    └── workflows/
        └── ci.yml     # CI/CD pipeline





### Step 1: FastAPI App (`app/main.py`)
A minimal FastAPI app using `ReflectedTableModel` with PostgreSQL.

```python
# app/main.py
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from app.models import ReflectedTableModel

app = FastAPI()

# Database setup with PostgreSQL
Base = declarative_base()
class MasterTable(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50))

# Connect to PostgreSQL (provided by docker-compose)
engine = create_engine("postgresql://user:password@db/testdb")
Base.metadata.create_all(engine)

def get_session():
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()

@app.post("/users/")
def create_user(data: dict, session: Session = Depends(get_session)):
    model = ReflectedTableModel(session, "users", MasterTable)
    return model.create(data)

@app.get("/users/{id}")
def read_user(id: int, session: Session = Depends(get_session)):
    model = ReflectedTableModel(session, "users", MasterTable)
    user = model.read(id)
    return user if user else {"error": "Not found"}

# Add other endpoints (update, delete, list) as needed...


# Dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]




fastapi==0.95.0
uvicorn==0.21.1
sqlalchemy==2.0.0
psycopg2-binary==2.9.5




version: "3.8"
services:
  app:
    build: .
    ports:
      - "8000:8000"
    depends_on:
      - db
  db:
    image: postgres:13
    environment:
      POSTGRES_DB: testdb
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"  # Optional, for local debugging




    # tests/test_api.py
import requests
import time

def wait_for_api():
    """Wait for the API to be ready."""
    for _ in range(10):
        try:
            response = requests.get("http://localhost:8000/users/1")
            if response.status_code in (200, 404):
                return True
        except requests.ConnectionError:
            time.sleep(1)
    raise Exception("API not ready")

def test_create_and_read_user():
    wait_for_api()
    payload = {"id": 1, "username": "alice"}
    response = requests.post("http://localhost:8000/users/", json=payload)
    assert response.status_code == 200
    assert response.json()["username"] == "alice"
    response = requests.get("http://localhost:8000/users/1")
    assert response.status_code == 200
    assert response.json()["username"] == "alice"

def test_read_nonexistent_user():
    wait_for_api()
    response = requests.get("http://localhost:8000/users/999")
    assert response.status_code == 200  # FastAPI returns 200 with error
    assert "error" in response.json()


name: CI Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  acceptance-tests:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v2

      - name: Build and start containers
        run: |
          docker-compose up -d --build

      - name: Wait for API to be ready
        run: |
          for i in {1..10}; do
            if curl -s http://localhost:8000/users/1; then
              break
            fi
            sleep 1
          done

      - name: Run acceptance tests
        run: |
          docker run --network host python:3.9-slim bash -c "pip install requests pytest && pytest tests/test_api.py -v"

      - name: Tear down containers
        if: always()
        run: |
          docker-compose down