from typing import Callable

import pytest
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Table, MetaData
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from app.database.scd.db_scd_strategies import SCDType2HandlerStrategy  # Adjust import path


@pytest.fixture
def db_session():
    """Creates an in-memory SQLite database session for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    metadata = MetaData()

    # Define the test table schema (mimics a real SCD Type 2 table)
    employees = Table(
        "employees",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("salary", Float),
        Column("on_time", DateTime, nullable=False),
        Column("off_time", DateTime, nullable=True)
    )

    # Create tables
    metadata.create_all(engine)

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session, employees  # Yield the session and table for tests

    # Tear down
    session.close()
    engine.dispose()


def test_create_adds_on_time(db_session):
    session, employees = db_session
    strategy = SCDType2HandlerStrategy(employees)
    strategy.table = employees  # Assign the test table

    # Input test data
    test_data = [
        {"id": 1, "name": "Alice", "salary": 50000},
        {"id": 2, "name": "Bob", "salary": 60000}
    ]

    # Use a fixed timestamp for consistency
    fixed_now = datetime(2025, 1, 1, 12, 0, 0)
    pd.Timestamp.now: Callable[[], datetime] = lambda: fixed_now  # Override timestamp

    # Run create()
    strategy.create(session, test_data)

    # Verify inserted data
    result = session.execute(employees.select()).fetchall()
    assert len(result) == 2  # Two records inserted
    assert result[0].on_time == fixed_now  # Check if on_time was set
    assert result[1].on_time == fixed_now


def test_read_filters_off_time(db_session):
    session, employees = db_session
    strategy = SCDType2HandlerStrategy()
    strategy.table = employees

    # Insert test data (some with `off_time`)
    session.execute(employees.insert(), [
        {"id": 1, "name": "Alice", "salary": 50000, "on_time": "2025-01-01", "off_time": None},
        {"id": 2, "name": "Bob", "salary": 60000, "on_time": "2025-01-02", "off_time": "2025-02-01"},
    ])
    session.commit()

    # Run read()
    result = strategy.read(session)

    # Only Alice should be returned (Bob has `off_time` set)
    expected = [{"id": 1, "name": "Alice", "salary": 50000, "on_time": "2025-01-01", "off_time": None}]
    
    assert result == expected
