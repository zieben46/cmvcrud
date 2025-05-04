import pytest
from sqlalchemy import create_engine
from datastorekit.adapters.sqlalchemy_core_adapter import SQLAlchemyCoreAdapter

def test_sync_tables():
    # Setup in-memory SQLite
    engine = create_engine("sqlite:///:memory:")
    profile = DatabaseProfile(db_type="sqlite", dbname="test", keys="unique_id")
    adapter = SQLAlchemyCoreAdapter(profile)
    adapter.engine = engine

    # Create source table
    engine.execute("CREATE TABLE source_table (unique_id INTEGER PRIMARY KEY, category TEXT, amount FLOAT)")
    engine.execute("INSERT INTO source_table VALUES (1, 'A', 10.0), (2, 'B', 20.0)")

    # Initialize orchestrator
    orchestrator = DataStoreOrchestrator(profiles=[profile])
    orchestrator.adapters["test:default"] = adapter

    # Test: Non-existent target table
    orchestrator.sync_tables("test", "public", "source_table", "test", "public", "target_table")
    result = adapter.select("target_table", {})
    assert len(result) == 2
    assert any(r["unique_id"] == 2 and r["category"] == "B" for r in result)

    # Test: Schema changed
    engine.execute("ALTER TABLE source_table ADD COLUMN new_col TEXT")
    engine.execute("UPDATE source_table SET new_col = 'X'")
    orchestrator.sync_tables("test", "public", "source_table", "test", "public", "target_table")
    result = adapter.select("target_table", {})
    assert any(r["new_col"] == "X" for r in result)

    # Test: Schema unchanged with changes
    engine.execute("INSERT INTO source_table VALUES (3, 'C', 30.0, 'Y')")
    engine.execute("DELETE FROM target_table WHERE unique_id = 1")
    orchestrator.sync_tables("test", "public", "source_table", "test", "public", "target_table")
    result = adapter.select("target_table", {})
    assert len(result) == 2
    assert any(r["unique_id"] == 3 and r["category"] == "C" for r in result)

    # Test: Duplicate key error
    engine.execute("INSERT INTO target_table VALUES (3, 'D', 40.0, 'Z')")
    with pytest.raises(DatastoreOperationError):
        orchestrator.sync_tables("test", "public", "source_table", "test", "public", "target_table")