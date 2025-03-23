from fastapi import FastAPI, Query, Depends, HTTPException, status, BackgroundTasks, Body
from fastapi.security import OAuth2PasswordRequestForm
from typing import List, Dict, Any, Literal
import pandas as pd
from utils import (
    get_current_user, require_role, process_changes, get_table_info, is_table_locked,
    lock_table, unlock_table, get_last_sync_version, create_jwt_token, verify_user_password,
    get_db_managers, get_postgres_engine, postgres_engine
)
from dbadminkit.core.crud_types import CRUDOperation

app = FastAPI(title="Database Table Management API")

# Endpoints
@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = verify_user_password(form_data.username, form_data.password, postgres_engine)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_jwt_token({"sub": user["username"], "role": user["role"]})
    return {"access_token": token, "token_type": "bearer"}

@app.get("/get-authorized-tables")
async def get_authorized_tables(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    user: dict = Depends(get_current_user),
    postgres_engine: Engine = Depends(get_postgres_engine)
):
    with postgres_engine.connect() as conn:
        if user["role"] == "admin":
            tables = conn.execute(
                select(master_table.c.table_name)
                .where(master_table.c.db_type == db_type)
            ).fetchall()
            table_list = [row[0] for row in tables]
        else:
            tables = conn.execute(
                select(table_user_groups.c.table_name)
                .where(table_user_groups.c.db_type == db_type)
                .where(table_user_groups.c.username == user["username"])
            ).fetchall()
            table_list = [row[0] for row in tables]
        return {"tables": table_list if table_list else []}

@app.post("/lock-table")
async def lock_table_endpoint(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to lock"),
    user: dict = Depends(get_current_user)
):
    lock_table(db_type, table_name, user)
    return {"status": "locked", "db_type": db_type, "table": table_name, "locked_by": user["username"]}

@app.post("/unlock-table")
async def unlock_table_endpoint(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to unlock"),
    user: dict = Depends(get_current_user)
):
    unlock_table(db_type, table_name, user)
    return {"status": "unlocked", "db_type": db_type, "table": table_name}

@app.post("/ingest-cdf")
async def ingest_cdf(
    changes: List[Dict[str, Any]],
    target: str = Query(..., description="Target DB: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to apply changes to"),
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(require_role("editor")),
    db_managers: Dict[str, DBManager] = Depends(get_db_managers)
):
    if is_table_locked(target, table_name, user):
        raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {target} is locked")
    background_tasks.add_task(process_changes, changes, target, table_name, db_managers)
    return {"status": "processing", "records": len(changes), "target": target, "table": table_name}

@app.post("/sync-table")
async def sync_table(
    new_data: List[Dict[str, Any]],
    target: str = Query(..., description="Target DB: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to sync"),
    user: dict = Depends(require_role("editor")),
    db_managers: Dict[str, DBManager] = Depends(get_db_managers)
):
    # Limit input size
    MAX_ROWS = 10000  # Adjust as needed
    if len(new_data) > MAX_ROWS:
        raise HTTPException(status_code=413, detail=f"Payload too large, max {MAX_ROWS} rows allowed")
    
    # Check if table is locked
    if is_table_locked(target, table_name, user):
        raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {target} is locked")
    
    # Fetch table info and manager
    table_info = get_table_info(target, table_name)
    table_info_full = {"table_name": table_name, **table_info}
    db_table = db_managers[target].get_table(table_info_full)
    
    # Fetch current data with limit
    current_data = db_table.perform_crud(CRUDOperation.READ, {}, limit=MAX_ROWS)
    
    # Process edits and return result
    try:
        rows_affected = db_table.process_list_edits(current_data, new_data)
        return {"status": "synced", "rows_affected": rows_affected}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")

@app.get("/get-last-sync-version")
async def get_last_sync_version_endpoint(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table name"),
    user: dict = Depends(get_current_user)
):
    version = get_last_sync_version(db_type, table_name)
    return {"db_type": db_type, "table_name": table_name, "last_version": version}

@app.post("/update-sync-version")
async def update_sync_version(
    db_type: str = Query(..., description="Database type: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table name"),
    version: int = Body(..., embed=True),
    user: dict = Depends(require_role("editor")),
    postgres_engine: Engine = Depends(get_postgres_engine)
):
    with postgres_engine.connect() as conn:
        conn.execute(
            update(sync_metadata)
            .where(sync_metadata.c.db_type == db_type)
            .where(sync_metadata.c.table_name == table_name)
            .values(last_version=version)
            .on_conflict_do_update(
                index_elements=["db_type", "table_name"],
                set_={"last_version": version}
            )
        )
        conn.commit()
    return {"status": "version_updated", "db_type": db_type, "table": table_name, "version": version}

@app.post("/execute-crud")
async def execute_crud(
    operation: Literal["insert", "read", "update", "delete"],
    target: str = Query(..., description="Target DB: 'postgres' or 'databricks'"),
    table_name: str = Query(..., description="Table to operate on"),
    data: Dict[str, Any] = None,
    user: dict = Depends(get_current_user),
    db_managers: Dict[str, DBManager] = Depends(get_db_managers)
):
    if operation != "read" and is_table_locked(target, table_name, user):
        raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {target} is locked by another user")
    table_info = get_table_info(target, table_name)
    table_info_full = {"table_name": table_name, **table_info}
    db_table = db_managers[target].get_table(table_info_full)
    crud_type = CRUDOperation(operation.upper())
    if operation == "read":
        result = db_table.perform_crud(crud_type, data or {})
        return {"status": "read", "data": result}
    if not data:
        raise HTTPException(status_code=400, detail="Data required for this operation")
    result = db_table.perform_crud(crud_type, data)
    return {"status": f"{operation}d", "data": result}

# New Background Task for Syncing Between Databases
def sync_tables_in_background(
    table_name: str, source: str, target: str, user: dict, db_managers: Dict[str, "DBManager"]
):
    BATCH_SIZE = 10000  # Adjust based on memory and DB capacity
    source_db_table = db_managers[source].get_table({"table_name": table_name, **get_table_info(source, table_name)})
    target_db_table = db_managers[target].get_table({"table_name": table_name, **get_table_info(target, table_name)})

    # Fetch target data once (small, 1 record initially)
    target_data = target_db_table.perform_crud(CRUDOperation.READ, {})
    target_keys = {row["id"] for row in target_data}  # Assuming 'id' is the key

    # Process source data in batches
    offset = 0
    rows_affected = 0
    try:
        while True:
            batch = source_db_table.perform_crud(CRUDOperation.READ, {}, limit=BATCH_SIZE, offset=offset)
            if not batch:  # End of data
                break
            
            # Process batch (inserts primarily, given target is small initially)
            insert_data = [row for row in batch if row["id"] not in target_keys]
            if insert_data:
                rows_affected += target_db_table.process_list_edits(target_data, insert_data)
            
            offset += BATCH_SIZE
            target_data = insert_data  # Update target_data for next batch comparison
    finally:
        # Ensure tables are unlocked even if an error occurs
        unlock_table(source, table_name, user)
        unlock_table(target, table_name, user)

    # Log result (could be stored in a DB or file in a real app)
    print(f"Synced {rows_affected} rows from {source} to {target} for table {table_name}")

@app.post("/sync-between-dbs")
async def sync_between_dbs(
    background_tasks: BackgroundTasks,
    table_name: str = Query(..., description="Table name to sync"),
    source: str = Query(..., description="Source DB: 'postgres' or 'databricks'"),
    target: str = Query(..., description="Target DB: 'postgres' or 'databricks'"),
    user: dict = Depends(require_role("editor")),
    db_managers: Dict[str, DBManager] = Depends(get_db_managers)
):
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must be different")
    if is_table_locked(source, table_name, user):
        raise HTTPException(status_code=423, detail=f"Source table '{table_name}' in {source} is locked")
    if is_table_locked(target, table_name, user):
        raise HTTPException(status_code=423, detail=f"Target table '{table_name}' in {target} is locked")
    
    # Lock tables
    lock_table(source, table_name, user)
    lock_table(target, table_name, user)
    
    try:
        # Offload sync to background task
        background_tasks.add_task(sync_tables_in_background, table_name, source, target, user, db_managers)
        return {
            "status": "sync_started",
            "source": source,
            "target": target,
            "table": table_name,
            "message": "Sync is running in the background"
        }
    except Exception as e:
        # Unlock tables if task setup fails
        unlock_table(source, table_name, user)
        unlock_table(target, table_name, user)
        raise HTTPException(status_code=500, detail=f"Failed to start sync: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)