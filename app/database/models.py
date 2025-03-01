from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from datetime import datetime

# Admin-specific base for reflection
AdminBase = automap_base()

# Define admin tables explicitly if needed, but we'll reflect them
class MasterTable(AdminBase):
    __tablename__ = "master_table"  # Placeholder; adjust if real schema exists

class MasterTableLock(AdminBase):
    __tablename__ = "master_table_locks"

class User(AdminBase):
    __tablename__ = "users"

# Reflect only admin tables
def reflect_admin_tables(engine):
    AdminBase.prepare(
        engine,
        reflect=True,
        only=["master_table", "master_table_locks", "users"]
    )

# Example schema (for reference, adjust as needed)
"""
CREATE TABLE master_table (
    table_name VARCHAR(255) PRIMARY KEY,
    scd_type INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE master_table_locks (
    table_name VARCHAR(255) PRIMARY KEY,
    locked_by VARCHAR(255),
    locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users (
    username VARCHAR(255) PRIMARY KEY,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL
);
"""