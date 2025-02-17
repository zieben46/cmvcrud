from fastapi import APIRouter, Depends, HTTPException
from app.models.db_model import DatabaseModel
from app.models.base_model import CrudType, DatabaseType
from app.auth.auth import get_current_user
import logging

# In-memory dictionary to track table locks
table_locks = {}

def get_db_model(table_name: str):
    """Fetch the database model and check if the table is locked."""
    try:
        return DatabaseModel(DatabaseType.POSTGRES, table_name)
    except ValueError as e:
        logging.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))

class APIController:
    """API Controller that registers all CRUD endpoints with table locking support."""

    def __init__(self):
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self):
        """Registers all routes to the APIRouter."""
        self.router.post("/{table_name}/create")(self.create_entry)
        self.router.get("/{table_name}/read")(self.read_entries)
        self.router.put("/{table_name}/update/{entry_id}")(self.update_entry)
        self.router.delete("/{table_name}/delete/{entry_id}")(self.delete_entry)
        self.router.post("/{table_name}/lock")(self.lock_table)
        self.router.post("/{table_name}/unlock")(self.unlock_table)

    def create_entry(
        self, table_name: str, data: dict, 
        db: DatabaseModel = Depends(get_db_model), 
        user: dict = Depends(get_current_user)
    ):
        """Creates a new record in the table (Requires authentication)."""
        try:
            db.execute(CrudType.CREATE, **data)
            return {"message": f"✅ Entry added to `{table_name}` by `{user['username']}`"}
        except Exception as e:
            logging.error(f"Error creating entry in `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def read_entries(
        self, table_name: str, 
        db: DatabaseModel = Depends(get_db_model), 
        user: dict = Depends(get_current_user)
    ):
        """Reads all records from the table (Requires authentication)."""
        try:
            df = db.execute(CrudType.READ)
            return df.to_dict(orient="records") # type: ignore
        except Exception as e:
            logging.error(f"Error reading `{table_name}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def update_entry(
        self, table_name: str, entry_id: int, data: dict, 
        db: DatabaseModel = Depends(get_db_model), 
        user: dict = Depends(get_current_user)
    ):
        """Updates an entry (Requires authentication and table lock)."""
        if table_locks.get(table_name) and table_locks[table_name] != user["username"]:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is locked by `{table_locks[table_name]}`")

        try:
            db.execute(CrudType.UPDATE, id=entry_id, **data)
            return {"message": f"🔄 Entry `{entry_id}` updated in `{table_name}` by `{user['username']}`"}
        except Exception as e:
            logging.error(f"Error updating `{table_name}` entry `{entry_id}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def delete_entry(
        self, table_name: str, entry_id: int, 
        db: DatabaseModel = Depends(get_db_model), 
        user: dict = Depends(get_current_user)
    ):

        print(f"user type: {type(user)}, value: {user}")  # 🔥 Debugging user
        """Deletes an entry (Requires authentication and table lock)."""
        if table_locks.get(table_name) and table_locks[table_name] != user["username"]:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is locked by `{table_locks[table_name]}`")

        try:
            db.execute(CrudType.DELETE, id=entry_id)
            return {"message": f"🗑️ Entry `{entry_id}` deleted from `{table_name}` by `{user['username']}`"}
        except Exception as e:
            logging.error(f"Error deleting `{table_name}` entry `{entry_id}`: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def lock_table(self, table_name: str, user: dict = Depends(get_current_user)):
        """Locks a table so only the current user can edit it."""
        if table_name in table_locks:
            raise HTTPException(status_code=403, detail=f"❌ Table `{table_name}` is already locked by `{table_locks[table_name]}`")

        table_locks[table_name] = user["username"]
        return {"message": f"🔒 Table `{table_name}` is now locked by `{user['username']}`"}

    def unlock_table(self, table_name: str, user: dict = Depends(get_current_user)):
        """Unlocks a table, allowing other users to edit it."""
        if table_locks.get(table_name) != user["username"]:
            raise HTTPException(status_code=403, detail="❌ You can only unlock tables you locked.")

        del table_locks[table_name]
        return {"message": f"🔓 Table `{table_name}` is now unlocked."}
