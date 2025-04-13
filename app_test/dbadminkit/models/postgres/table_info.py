from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Union, Dict, Optional
import json

# SQLAlchemy setup
Base = declarative_base()

class TableInfo(Base):
    __tablename__ = 'table_info'

    id = Column(Integer, primary_key=True)
    table_name = Column(String, nullable=False)
    security_type = Column(String, nullable=False)  # e.g., "public", "restricted"
    schema = Column(String, default="public")
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    metadata = Column(JSON, nullable=True)  # Optional JSONB for extra data

    def to_dict(self) -> Dict:
        """Serialize to dictionary for JSON compatibility."""
        return {
            'id': self.id,
            'table_name': self.table_name,
            'security_type': self.security_type,
            'schema': self.schema,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict, session=None) -> 'TableInfo':
        """Create TableInfo from dictionary, optionally persist to session."""
        # Convert ISO datetime string to datetime object if present
        if 'created_at' in data and data['created_at']:
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        # Create instance
        instance = cls(**{k: v for k, v in data.items() if k in cls.__table__.columns})
        if session:
            session.add(instance)
        return instance