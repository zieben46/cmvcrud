import pytest
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

from model.db_model import DatabaseModel

def test_connection_should_throw_connection_error_for_bad_URL():
    bad_url = URL.create(
        drivername="sqlite", 
        database="invalid/path/to/db"
    )

    bad_engine = create_engine(bad_url, echo=False)
    db_model = DatabaseModel(str(bad_url), "any_table")
    with pytest.raises(ConnectionError, match="❌ Unable to connect to the database"):
        db_model._test_connection()


def test_read_scd_0():
    pass