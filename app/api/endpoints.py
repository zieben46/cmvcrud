from fastapi import APIRouter, Depends, HTTPException, Form
from fastapi.responses import StreamingResponse
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from app.database.db_model import ReflectedTableModel
from app.database.models import AdminBase, MasterTableLock, User
from app.config.enums import CrudType
from app.api.auth import get_current_user, create_access_token
from functools import partial  # Import partial
from app.database.connection import get_db, engine
import logging
import json
from datetime import timedelta

class TableCrudAPI:
    """Handles CRUD operations for user tables with locking support."""

    def __init__(self, model_type=ReflectedTableModel):
        self.router = APIRouter()
        self.model_type = model_type
        self._register_routes()

    def _register_routes(self):
        self.router.post("/{table_name}/records")(self.create_records)
        self.router.get("/{table_name}/records")(self.read_records)
        self.router.put("/{table_name}/records")(self.update_records)
        self.router.delete("/{table_name}/records")(self.delete_records)
        self.router.post("/{table_name}/lock")(self.lock_table)
        self.router.post("/{table_name}/unlock")(self.unlock_table)
        self.router.post("/auth/token")(self.login)
        self.router.get("/auth/protected")(self.protected_route)
        self.router.get("/{table_name}/records/stream")(self.stream_records)
        self.router.get("/{table_name}/lock_status")(self.check_lock_status)



    def _check_lock(self, session: Session, table_name: str, username: str) -> None:
        lock = session.query(MasterTableLock).filter_by(table_name=table_name).first()
        if lock and lock.locked_by != username:
            raise HTTPException(status_code=403, detail=f"Table `{table_name}` is locked by `{lock.locked_by}`")

    def _get_table_specs(self, session: Session, table_name: str) -> Dict[str, Any]:
        master = session.query(AdminBase.classes.master_table).filter_by(table_name=table_name).first()
        if not master:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found in master_table"
            )
        lock = session.query(MasterTableLock).filter_by(table_name=table_name).first()
        return {
            'table_name': table_name,
            'is_locked': bool(lock),
            'locked_by': lock.locked_by if lock else None,
            'scd_type': master.scd_type if master else 0,
            'created_at': master.created_at if master else None,
            'updated_at': lock.locked_at if lock else (master.updated_at if master else None)
        }

    def create_records(
        self,
        table_name: str,
        data: List[Dict],
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)
            self._check_lock(session, table_name, user["username"])
            model = self.model_type(engine, table_specs)
            DynamicModel = model.generate_table_model()
            validated_data = [DynamicModel(**item).dict(exclude_unset=True) for item in data]
            result = model.execute(session, CrudType.CREATE, validated_data)
            return {"message": f"✅ {len(result)} entries added to `{table_specs['table_name']}` by `{user['username']}`"}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            logging.error(f"Server error creating entries in `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            logging.error(f"Unexpected error creating entries in `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Unexpected error occurred")

    def read_records(
        self,
        table_name: str,
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)
            model = self.model_type(engine, table_specs)
            DynamicModel = model.generate_table_model()
            records = model.execute(session, CrudType.READ)
            validated_records = [DynamicModel(**record).dict() for record in records]
            return validated_records
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            logging.error(f"Server error reading `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            logging.error(f"Unexpected error reading `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Unexpected error occurred")

    def update_records(
        self,
        table_name: str,
        data: List[Dict],
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)
            self._check_lock(session, table_name, user["username"])
            model = self.model_type(engine, table_specs)
            DynamicModel = model.generate_table_model()
            validated_data = [DynamicModel(**item).dict(exclude_unset=True) for item in data]
            updated_count = model.execute(session, CrudType.UPDATE, validated_data)
            return {"message": f"🔄 Updated `{updated_count}` entries in `{table_specs['table_name']}` by `{user['username']}`"}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            logging.error(f"Server error updating `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            logging.error(f"Unexpected error updating `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Unexpected error occurred")

    def delete_records(
        self,
        table_name: str,
        data: List[Dict],
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)
            self._check_lock(session, table_name, user["username"])
            model = self.model_type(engine, table_specs)
            deleted_count = model.execute(session, CrudType.DELETE, data)
            return {"message": f"🗑️ Deleted `{deleted_count}` entries from `{table_specs['table_name']}` by `{user['username']}`"}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            logging.error(f"Server error deleting `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            logging.error(f"Unexpected error deleting `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Unexpected error occurred")

    def lock_table(
        self,
        table_name: str,
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)  # Validate table exists
            lock = session.query(MasterTableLock).filter_by(table_name=table_name).first()
            if lock:
                raise HTTPException(status_code=403, detail=f"Table `{table_name}` is already locked by `{lock.locked_by}`")
            
            new_lock = MasterTableLock(table_name=table_name, locked_by=user["username"])
            session.add(new_lock)
            session.commit()
            return {"message": f"🔒 Table `{table_specs['table_name']}` is now locked by `{user['username']}`"}
        except Exception as e:
            session.rollback()
            logging.error(f"Error locking `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to lock table")

    def unlock_table(
        self,
        table_name: str,
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)  # Validate table exists
            lock = session.query(MasterTableLock).filter_by(table_name=table_name).first()
            if not lock:
                raise HTTPException(status_code=400, detail=f"Table `{table_name}` is not locked")
            if lock.locked_by != user["username"]:
                raise HTTPException(status_code=403, detail="You can only unlock tables you locked")
            
            session.delete(lock)
            session.commit()
            return {"message": f"🔓 Table `{table_specs['table_name']}` is now unlocked"}
        except Exception as e:
            session.rollback()
            logging.error(f"Error unlocking `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to unlock table")

    def login(
        self,
        username: str = Form(...),
        password: str = Form(...),
        session: Session = Depends(get_db)
    ):
        try:
            user = session.query(User).filter_by(username=username).first()
            if not user or user.password != password:
                raise HTTPException(status_code=401, detail="Invalid credentials")
            
            access_token = create_access_token(
                {"sub": user.username, "role": user.role},
                expires_delta=timedelta(minutes=30)
            )
            return {"access_token": access_token, "token_type": "bearer"}
        except Exception as e:
            logging.error(f"Error during login for `{username}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Login failed")

    def protected_route(
        self,
        user: dict = Depends(get_current_user)
    ):
        return {"message": f"🔒 Welcome, {user['username']}! You have `{user['role']}` permissions"}

    def check_lock_status(
        self,
        table_name: str,
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        try:
            table_specs = self._get_table_specs(session, table_name)  # Validate table exists
            lock = session.query(MasterTableLock).filter_by(table_name=table_name).first()
            if lock:
                return {
                    "is_locked": True,
                    "locked_by": lock.locked_by,
                    "locked_at": lock.locked_at.isoformat() if lock.locked_at else None
                }
            return {"is_locked": False, "locked_by": None, "locked_at": None}
        except Exception as e:
            logging.error(f"Error checking lock status for `{table_name}`: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to check lock status")

    def stream_records(
        self,
        table_name: str,
        user: dict = Depends(get_current_user),
        session: Session = Depends(get_db)
    ):
        def data_generator():
            try:
                table_specs = self._get_table_specs(session, table_name)
                model = self.model_type(engine, table_specs)
                DynamicModel = model.generate_table_model()
                result = model.execute(session, CrudType.READ) or []
                for row in result:
                    validated_row = DynamicModel(**row).dict()
                    yield json.dumps(validated_row) + "\n"
            except ValueError as e:
                yield json.dumps({"error": str(e)})
            except RuntimeError as e:
                logging.error(f"Server error streaming `{table_name}`: {str(e)}")
                yield json.dumps({"error": "Internal server error"})
            except Exception as e:
                logging.error(f"Unexpected error streaming `{table_name}`: {str(e)}")
                yield json.dumps({"error": "Unexpected error occurred"})
        
        return StreamingResponse(data_generator(), media_type="application/json")