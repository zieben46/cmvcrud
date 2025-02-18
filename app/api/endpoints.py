from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Dict
from sqlalchemy.orm import Session
from app.database.db_model import DatabaseModel
from app.config.enums import CrudType
from app.api.auth import get_current_user
from app.database.connection import get_db
import logging
import json

from app.api.auth import get_current_user
from app.api.auth import create_access_token
from datetime import timedelta

from fastapi import Form


# In-memory dictionary to track table locks
table_locks = {}

class TableAPI:
    """Handles table-based CRUD operations with table locking support."""

    def __init__(self):
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self):
        """Registers all API endpoints."""
        self.router.post("/{table_name}/create")(self.create_entries)
        self.router.get("/{table_name}/read")(self.read_entries)
        self.router.put("/{table_name}/update")(self.update_entries)
        self.router.delete("/{table_name}/delete")(self.delete_entries)
        self.router.post("/{table_name}/lock")(self.lock_table)
        self.router.post("/{table_name}/unlock")(self.unlock_table)
        
        self.router.post("/token")(self.login)
        self.router.get("/protected-route")(self.protected_route)
        # self.router.get("/{table_name}/read")(self.stream_entries)

    def create_entries(
        self, 
        table_name: str, 
        data: List[Dict],  
        db: Session = Depends(get_db),
        user: dict = Depends(get_current_user)
    ):
        """Creates multiple new records in the table (Requires authentication)."""
        try:
            model = DatabaseModel(db, table_name)
            model.execute(CrudType.CREATE, data)
            db.commit()  # ✅ Commit transaction
            return {"message": f"✅ {len(data)} entries added to `{table_name}` by `{user['username']}`"}
        except Exception as e:
            db.rollback()  # ✅ Rollback on failure
            logging.error(f"Error creating entries in `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def read_entries(
        self, 
        table_name: str,  
        db: Session = Depends(get_db),
        user: dict = Depends(get_current_user)
    ):
        """Reads all records from the table (Requires authentication)."""
        try:
            model = DatabaseModel(db, table_name) 
            return model.execute(CrudType.READ, [{}])
        except Exception as e:
            logging.error(f"Error reading `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def update_entries(
        self, 
        table_name: str,
        data: List[Dict],  
        db: Session = Depends(get_db),
        user: dict = Depends(get_current_user)
    ):
        """Updates multiple entries (Requires authentication and table lock)."""
        if table_locks.get(table_name) and table_locks[table_name] != user["username"]:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is locked by `{table_locks[table_name]}`")

        try:
            model = DatabaseModel(db, table_name)
            model.execute(CrudType.UPDATE, data)
            db.commit()  # ✅ Commit transaction
            return {"message": f"🔄 Updated `{len(data)}` entries in `{table_name}` by `{user['username']}`"}
        except Exception as e:
            db.rollback()  # ✅ Rollback on failure
            logging.error(f"Error updating `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def delete_entries(
        self, 
        table_name: str, 
        data: List[Dict],  
        db: Session = Depends(get_db),
        user: dict = Depends(get_current_user)
    ):
        """Deletes multiple entries (Requires authentication and table lock)."""
        if table_locks.get(table_name) and table_locks[table_name] != user["username"]:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is locked by `{table_locks[table_name]}`")
        try:
            model = DatabaseModel(db, table_name)
            model.execute(CrudType.DELETE, data)
            db.commit()  # ✅ Commit transaction
            return {"message": f"🗑️ Deleted `{len(data)}` entries from `{table_name}` by `{user['username']}`"}
        except Exception as e:
            db.rollback()  # ✅ Rollback on failure
            logging.error(f"Error deleting `{table_name}` entries: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def lock_table(
        self, 
        table_name: str, 
        user: dict = Depends(get_current_user)
    ):
        """Locks a table so only the current user can edit it."""
        if table_name in table_locks:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is already locked by `{table_locks[table_name]}`")
        table_locks[table_name] = user["username"]
        return {"message": f"🔒 Table `{table_name}` is now locked by `{user['username']}`"}

    def unlock_table(
        self, 
        table_name: str,
        user: dict = Depends(get_current_user)
    ):
        """Unlocks a table, allowing other users to edit it."""
        if table_locks.get(table_name) != user["username"]:
            raise HTTPException(status_code=403, detail="❌ You can only unlock tables you locked.")

        del table_locks[table_name]
        return {"message": f"🔓 Table `{table_name}` is now unlocked."}

    def login(
            self,
            username: str = Form(...),
            password: str = Form(...)
    ):
        """Authenticate user and return JWT token."""
        fake_users_db = {
            "admin": {"username": "admin", "password": "admin123", "role": "admin"},
            "user": {"username": "user", "password": "user123", "role": "editor"},
        }
        user = fake_users_db.get(username)
        if not user or user["password"] != password:
            raise HTTPException(status_code=401, detail="❌ Invalid credentials")
        access_token = create_access_token({"sub": username, "role": user["role"]}, expires_delta=timedelta(minutes=30))
        return {"access_token": access_token, "token_type": "bearer"}

    def protected_route(
        self, 
        user: dict = Depends(get_current_user)
    ):
        """Example of a protected route using authentication."""
        return {"message": f"🔒 Welcome, {user['username']}! You have `{user['role']}` permissions."}
    
    def stream_entries(
        self, table_name: str,  
        db: Session = Depends(get_db)
    ):
        """Streams records from a table to avoid large payloads and high memory usage."""

        def data_generator():
            """Generator function that yields database records one by one as JSON."""
            model = DatabaseModel(db, table_name)
            result = model.execute(CrudType.READ, [{}]) or []
            for row in result:
                yield json.dumps(row) + "\n"  # ✅ Convert row to JSON string and send it incrementally

        return StreamingResponse(data_generator(), media_type="application/json")