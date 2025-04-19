# datastorekit/acceptance_tests/fixtures.py
from sqlalchemy import create_engine
from datastorekit.config import Config
from datastorekit.models.db_table import DBTable

def create_test_db():
    """Create an in-memory SQLite database for testing.

    Returns:
        tuple: (engine, table_info)
    """
    profile = Config.create_test_profile()
    engine = create_engine(profile.connection_string)
    table_info = {"table_name": "spend_plan", "scd_type": "type1", "key": "id"}
    table = Config.setup_test_table("spend_plan", engine)
    return engine, table_info

def insert_test_data(table: DBTable):
    """Insert sample test data into the spend_plan table.

    Args:
        table: DBTable instance for the spend_plan table.
    """
    test_data = [
        {"id": 1, "category": "Food", "amount": 50.0},
        {"id": 2, "category": "Travel", "amount": 100.0}
    ]
    for data in test_data:
        table.create(data)