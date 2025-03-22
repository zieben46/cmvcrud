from fastapi import FastAPI, Query, Depends, HTTPException
from sqlalchemy import Table, Column, String, JSON, ForeignKey, DateTime, MetaData, select, update, insert
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.sql import PrimaryKeyConstraint
import datetime
from typing import List, Dict, Any
import pandas as pd
from enum import Enum


# Mock database engines (replace with your actual engines)
db_managers = {
    "postgres": DBManager(Engine),  # Replace with actual PostgreSQL engine
    "databricks": DBManager(Engine)  # Replace with actual Databricks engine
}

postgres_engine = db_managers["postgres"].engine

# SQLAlchemy metadata and table definitions
metadata = MetaData()



# Helper functions for locking and unlocking
def is_table_locked(db_type: str, table_name: str, user: dict = None) -> bool:
    """Check if a table is locked by someone other than the current user."""
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(master_table.c.locked_by)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).fetchone()
        if result:
            locked_by = result[0]
            if locked_by is None:
                return False
            elif user and locked_by == user["username"]:
                return False
            else:
                return True
        return False

def lock_table(db_type: str, table_name: str, user: dict):
    """Lock a table if the user is authorized and the table is not already locked."""
    with postgres_engine.connect() as conn:
        # Check authorization
        allowed = conn.execute(
            select(table_user_groups.c.username)
            .where(table_user_groups.c.db_type == db_type)
            .where(table_user_groups.c.table_name == table_name)
            .where(table_user_groups.c.username == user["username"])
        ).fetchone()
        if not allowed:
            raise HTTPException(status_code=403, detail="You are not authorized to lock this table")

        # Check lock status
        locked_by = conn.execute(
            select(master_table.c.locked_by)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).scalar()
        if locked_by is not None:
            raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {db_type} is already locked by {locked_by}")

        # Lock the table
        conn.execute(
            update(master_table)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
            .values(locked_by=user["username"], locked_at=datetime.datetime.utcnow())
        )
        conn.commit()

def unlock_table(db_type: str, table_name: str, user: dict):
    """Unlock a table if the current user holds the lock."""
    with postgres_engine.connect() as conn:
        locked_by = conn.execute(
            select(master_table.c.locked_by)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).scalar()
        if locked_by != user["username"]:
            raise HTTPException(status_code=403, detail="You do not have the lock on this table")
        conn.execute(
            update(master_table)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
            .values(locked_by=None, locked_at=None)
        )
        conn.commit()

def get_table_info(db_type: str, table_name: str) -> dict:
    """Fetch the table_info JSON object from master_table."""
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(master_table.c.table_info)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).fetchone()
        if result:
            return result[0]
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in {db_type}")

# FastAPI app
app = FastAPI(title="Database Table Management API")


def verify_user(credentials: HTTPBasicCredentials = Depends(security)):
    with postgres_engine.connect() as conn:
        # Query the stored hashed password for the given username
        result = conn.execute(
            select(users.c.password)
            .where(users.c.username == credentials.username)
        ).fetchone()
        
        if result:
            stored_hash = result[0].encode('utf-8')  # Convert stored hash to bytes
            # Verify the provided password against the stored hash
            if bcrypt.checkpw(credentials.password.encode('utf-8'), stored_hash):
                return {"username": credentials.username}
        # Raise an error if the username doesn’t exist or password doesn’t match
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"}
        )

@app.post("/lock-table")
async def lock_table_endpoint(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to lock"),
    user: dict = Depends(verify_user)
):
    lock_table(db_type, table_name, user)
    return {"status": "locked", "db_type": db_type, "table": table_name, "locked_by": user["username"]}

@app.post("/unlock-table")
async def unlock_table_endpoint(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to unlock"),
    user: dict = Depends(verify_user)
):
    unlock_table(db_type, table_name, user)
    return {"status": "unlocked", "db_type": db_type, "table": table_name}

@app.post("/execute-crud")
async def execute_crud(
    operation: str = Query(..., description="CRUD operation: 'read', 'create', etc."),
    target: str = Query(..., description="Target database: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table name"),
    params: dict = {},  # Adjust based on your actual CRUD params
    user: dict = Depends(verify_user)
):
    if is_table_locked(target, table_name, user):
        raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {target} is locked by another user")
    
    table_info = get_table_info(target, table_name)
    table_info_full = {"table_name": table_name, **table_info}
    db_table = db_managers[target].get_table(table_info_full)
    
    data = db_table.perform_crud(CRUDOperation(operation.upper()), params)
    return {"status": "success", "data": data}

@app.post("/sync-table")
async def sync_table(
    target: str = Query(..., description="Target database: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to sync"),
    new_data: List[Dict[str, Any]],  # Data to sync
    user: dict = Depends(verify_user)
):
    if is_table_locked(target, table_name, user):
        raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {target} is locked by another user")
    
    table_info = get_table_info(target, table_name)
    table_info_full = {"table_name": table_name, **table_info}
    db_table = db_managers[target].get_table(table_info_full)
    
    current_data = db_table.perform_crud(CRUDOperation.READ, {})
    current_df = pd.DataFrame(current_data)
    new_df = pd.DataFrame(new_data)
    rows_affected = db_table.process_dataframe_edits(current_df, new_df)
    
    return {"status": "synced", "rows_affected": rows_affected}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# New endpoint to fetch authorized tables
@app.get("/get-authorized-tables")
async def get_authorized_tables(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    user: dict = Depends(verify_user)
):
    with postgres_engine.connect() as conn:
        if user["role"] == "admin":
            # Admins see all tables for the given db_type
            tables = conn.execute(
                select(master_table.c.table_name)
                .where(master_table.c.db_type == db_type)
            ).fetchall()
            table_list = [row[0] for row in tables]
        else:
            # Non-admins see only their assigned tables
            tables = conn.execute(
                select(table_user_groups.c.table_name)
                .where(table_user_groups.c.db_type == db_type)
                .where(table_user_groups.c.username == user["username"])
            ).fetchall()
            table_list = [row[0] for row in tables]
        
        if not table_list:
            return {"tables": [], "message": "No tables assigned to this user"}
        return {"tables": table_list}