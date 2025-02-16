from fastapi import FastAPI, Depends, HTTPException, Query
from model.db_model import DatabaseModel  # Your DB handler
from model.base_model import CrudType, SCDType
from fastapi.responses import StreamingResponse
import io
import os
import pandas as pd
from auth.auth import get_current_user
import logging

from model.base_model import DatabaseType


logging.basicConfig(level=logging.INFO)


app = FastAPI()


# 🔒 Dictionary to track table locks (user → table)
table_locks = {}  # { "table_name": "username" }

def get_db_model(table_name: str):
    """Fetch the database model and check if the table is locked."""
    try:
        return DatabaseModel(DatabaseType.POSTGRES, table_name)
    except ValueError as e:
        logging.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

class APIController:
    """Handles API routing for CRUD operations with authentication and table locking."""

    # ✅ CREATE (Requires Authentication)
    @staticmethod
    @app.post("/{table_name}/create")
    def create_entry(table_name: str, data: dict, db: DatabaseModel = Depends(get_db_model), user: dict = Depends(get_current_user)):
        """Creates a new record in the table (Requires authentication)."""
        try:
            db.execute(CrudType.CREATE, **data)
            return {"message": f"✅ Entry added to `{table_name}` by `{user['username']}`"}
        except Exception as e:
            logging.error(f"Error creating entry in `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    # ✅ READ (Requires Authentication)
    @staticmethod
    @app.get("/{table_name}/read")
    def read_entries(table_name: str, db: DatabaseModel = Depends(get_db_model), user: dict = Depends(get_current_user)):
        """Reads all records from the table (Requires authentication)."""
        try:
            df = db.execute(CrudType.READ)
            return df.to_dict(orient="records")
        except Exception as e:
            logging.error(f"Error reading `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    # ✅ UPDATE (Requires Authentication & Table Lock Check)
    @staticmethod
    @app.put("/{table_name}/update/{entry_id}")
    def update_entry(table_name: str, entry_id: int, data: dict, db: DatabaseModel = Depends(get_db_model), user: dict = Depends(get_current_user)):
        """Updates an entry (Requires authentication and table lock)."""
        if table_locks.get(table_name) and table_locks[table_name] != user["username"]:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is locked by `{table_locks[table_name]}`")

        try:
            db.execute(CrudType.UPDATE, id=entry_id, **data)
            return {"message": f"🔄 Entry `{entry_id}` updated in `{table_name}` by `{user['username']}`"}
        except Exception as e:
            logging.error(f"Error updating `{table_name}` entry `{entry_id}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    # ✅ DELETE (Requires Authentication & Table Lock Check)
    @staticmethod
    @app.delete("/{table_name}/delete/{entry_id}")
    def delete_entry(table_name: str, entry_id: int, db: DatabaseModel = Depends(get_db_model), user: dict = Depends(get_current_user)):
        """Deletes an entry (Requires authentication and table lock)."""
        if table_locks.get(table_name) and table_locks[table_name] != user["username"]:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is locked by `{table_locks[table_name]}`")

        try:
            db.execute(CrudType.DELETE, id=entry_id)
            return {"message": f"🗑️ Entry `{entry_id}` deleted from `{table_name}` by `{user['username']}`"}
        except Exception as e:
            logging.error(f"Error deleting `{table_name}` entry `{entry_id}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))
        
        
    # ✅ LOCK TABLE (New Feature)
    @staticmethod
    @app.post("/{table_name}/lock")
    def lock_table(table_name: str, user: dict = Depends(get_current_user)):
        """Locks a table so only the current user can edit it."""
        if table_name in table_locks:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is already locked by `{table_locks[table_name]}`")
        
        table_locks[table_name] = user["username"]
        return {"message": f"🔒 Table `{table_name}` is now locked by `{user['username']}`"}

    # ✅ UNLOCK TABLE (New Feature)
    @staticmethod
    @app.post("/{table_name}/unlock")
    def unlock_table(table_name: str, user: dict = Depends(get_current_user)):
        """Unlocks a table, allowing other users to edit it."""
        if table_locks.get(table_name) != user["username"]:
            raise HTTPException(status_code=403, detail="❌ You can only unlock tables you locked.")

        del table_locks[table_name]
        return {"message": f"🔓 Table `{table_name}` is now unlocked."}