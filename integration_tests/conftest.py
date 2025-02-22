import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import app  # Assuming your FastAPI app is in main.py
from app.database.connection import get_db
from app.config.db_configs import PostgresConfig
import os

# Override environment for tests
os.environ.update({
    "POSTGRES_USERNAME": "testuser",
    "POSTGRES_PASSWORD": "testpass",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DATABASE": "testdb",
})

@pytest.fixture(scope="session")
def db_engine():
    config = PostgresConfig(os.getenv)
    engine = create_engine(config.get_url())
    yield engine
    engine.dispose()

@pytest.fixture(scope="function")
def db_session(db_engine):
    connection = db_engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()
    yield session
    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture
def client(db_session):
    def override_get_db():
        yield db_session
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()

@pytest.fixture(scope="function")
def setup_tables(db_session):
    # Create a test table for SCD Type 1
    db_session.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            salary FLOAT
        )
    """)
    db_session.commit()
    yield
    db_session.execute("DROP TABLE employees")
    db_session.commit()