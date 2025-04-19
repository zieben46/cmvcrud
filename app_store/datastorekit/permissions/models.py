# datastorekit/permissions/models.py
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Users(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_group_admin = Column(Boolean, default=False)

class UserAccess(Base):
    __tablename__ = "user_access"
    id = Column(Integer, primary_key=True)
    username = Column(String, ForeignKey("users.username"), nullable=False)
    datastore_key = Column(String, nullable=False)  # e.g., 'spend_plan_db:safe_user'
    table_name = Column(String, nullable=False)
    access_level = Column(String, nullable=False)  # e.g., 'read', 'write'