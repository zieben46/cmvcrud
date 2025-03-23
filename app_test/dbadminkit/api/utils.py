from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import Table, Column, String, JSON, ForeignKey, DateTime, MetaData, select, update, Integer
from sqlalchemy.dialects.postgresql import JSONB
from jose import JWTError, jwt
from typing import Dict, Optional
from sqlalchemy.engine import Engine
import datetime
import bcrypt
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.core.db_manager import DBManager

# JWT Configuration
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# SQLAlchemy table definitions
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("username", String, primary_key=True),
    Column("password", String),
    Column("role", String)
)

master_table = Table(
    "master_table",
    metadata,
    Column("db_type", String),
    Column("table_name", String),
    Column("table_info", JSONB),
    Column("locked_by", String, ForeignKey("users.username"), nullable=True),
    Column("locked_at", DateTime, nullable=True),
    PrimaryKeyConstraint("db_type", "table_name")
)

table_user_groups = Table(
    "table_user_groups",
    metadata,
    Column("db_type", String),
    Column("table_name", String),
    Column("username", String, ForeignKey("users.username")),
    PrimaryKeyConstraint("db_type", "table_name", "username")
)

sync_metadata = Table(
    "sync_metadata",
    metadata,
    Column("db_type", String),
    Column("table_name", String),
    Column("last_version", Integer),
    PrimaryKeyConstraint("db_type", "table_name")
)

# Dependency-injectable database managers
def get_db_managers(
    postgres_profile: DatabaseProfile = Depends(lambda: DatabaseProfile.live_postgres()),
    databricks_profile: DatabaseProfile = Depends(lambda: DatabaseProfile.live_databricks())
) -> Dict[str, DBManager]:
    db_managers = {
        "postgres": DBManager(postgres_profile),
        "databricks": DBManager(databricks_profile)
    }
    return db_managers

# Helper to get postgres_engine from db_managers
def get_postgres_engine(db_managers: Dict[str, DBManager] = Depends(get_db_managers)) -> Engine:
    return db_managers["postgres"].engine.engine

# JWT Authentication Functions
def create_jwt_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_user_password(username: str, password: str, postgres_engine: Engine) -> Optional[Dict[str, str]]:
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(users.c.password, users.c.role)
            .where(users.c.username == username)
        ).fetchone()
        if result and bcrypt.checkpw(password.encode('utf-8'), result[0].encode('utf-8')):
            return {"username": username, "role": result[1]}
    return None

async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        role = payload.get("role")
        if username is None or role is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def require_role(required_role: str):
    async def role_checker(user: dict = Depends(get_current_user)):
        if user["role"] != required_role and user["role"] != "admin":
            raise HTTPException(status_code=403, detail=f"Role '{required_role}' or 'admin' required")
        return user
    return role_checker

# Table Locking Functions
def is_table_locked(db_type: str, table_name: str, user: dict = None, postgres_engine: Engine = Depends(get_postgres_engine)) -> bool:
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(master_table.c.locked_by)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).fetchone()
        if result:
            locked_by = result[0]
            return locked_by is not None and (not user or locked_by != user["username"])
        return False

def lock_table(db_type: str, table_name: str, user: dict, postgres_engine: Engine = Depends(get_postgres_engine)):
    with postgres_engine.connect() as conn:
        allowed = conn.execute(
            select(table_user_groups.c.username)
            .where(table_user_groups.c.db_type == db_type)
            .where(table_user_groups.c.table_name == table_name)
            .where(table_user_groups.c.username == user["username"])
        ).fetchone() or user["role"] == "admin"
        if not allowed:
            raise HTTPException(status_code=403, detail="You are not authorized to lock this table")
        locked_by = conn.execute(
            select(master_table.c.locked_by)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).scalar()
        if locked_by is not None:
            raise HTTPException(status_code=423, detail=f"Table '{table_name}' in {db_type} is locked by {locked_by}")
        conn.execute(
            update(master_table)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
            .values(locked_by=user["username"], locked_at=datetime.datetime.utcnow())
        )
        conn.commit()

def unlock_table(db_type: str, table_name: str, user: dict, postgres_engine: Engine = Depends(get_postgres_engine)):
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

def get_table_info(db_type: str, table_name: str, postgres_engine: Engine = Depends(get_postgres_engine)) -> dict:
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(master_table.c.table_info)
            .where(master_table.c.db_type == db_type)
            .where(master_table.c.table_name == table_name)
        ).fetchone()
        if result:
            return result[0]
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in {db_type}")

# Version Tracking
def get_last_sync_version(db_type: str, table_name: str, postgres_engine: Engine = Depends(get_postgres_engine)) -> int:
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(sync_metadata.c.last_version)
            .where(sync_metadata.c.db_type == db_type)
            .where(sync_metadata.c.table_name == table_name)
        ).scalar()
        return result if result is not None else 0

# CDF Processing (No schema validation)
async def process_changes(changes: List[Dict[str, Any]], target: str, table_name: str, db_managers: Dict[str, DBManager] = Depends(get_db_managers)):
    table_info = get_table_info(target, table_name)
    table_info_full = {"table_name": table_name, **table_info}
    db_table = db_managers[target].get_table(table_info_full)
    db_table.process_cdc_logs(changes)