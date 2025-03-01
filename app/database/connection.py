# app/database/connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config.db_configs import settings

# Create engine based on the settings
engine = create_engine(settings.database_url)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Dependency to get a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

        