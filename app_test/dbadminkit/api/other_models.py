from fastapi import FastAPI, Query, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import Table, Column, String, JSON, ForeignKey, DateTime, MetaData, select
from sqlalchemy.dialects.postgresql import JSONB
import bcrypt

app = FastAPI()
security = HTTPBasic()

# Mock database engine (replace with your actual engine)
postgres_engine = db_managers["postgres"].engine  # Assuming from your setup

# SQLAlchemy table definitions
metadata = MetaData()
users = Table(
    "users",
    metadata,
    Column("username", String, primary_key=True),
    Column("password", String),  # Hashed password
    Column("role", String)       # Added role column
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

# Authentication
def verify_user(credentials: HTTPBasicCredentials = Depends(security)):
    with postgres_engine.connect() as conn:
        result = conn.execute(
            select(users.c.password, users.c.role)
            .where(users.c.username == credentials.username)
        ).fetchone()
        if result and bcrypt.checkpw(credentials.password.encode('utf-8'), result[0].encode('utf-8')):
            return {"username": credentials.username, "role": result[1]}
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"}
        )