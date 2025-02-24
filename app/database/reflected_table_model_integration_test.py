from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from your_module import ReflectedTableModel  # Replace with actual module name

Base = declarative_base()

class MasterTable(Base):
    """Master table definition with scdtype metadata."""
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50))
    email = Column(String(120))
    created_at = Column(DateTime)
    scdtype = "integration_test_scdtype"

class TestReflectedTableModel:
    def setup_method(self):
        """Setup run before each test method: create in-memory SQLite database."""
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def teardown_method(self):
        """Teardown run after each test method: clean up database."""
        self.session.close()
        Base.metadata.drop_all(self.engine)

    # Basic Functionality Tests
    def test_init_success(self):
        """Test that the model initializes correctly."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        assert model.table_class.__name__ == "users"  # Reflected class exists
        assert model.metadata["scdtype"] == "integration_test_scdtype"  # Metadata extracted

    def test_create_and_read(self):
        """Test creating a record and reading it back."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        data = {"id": 1, "username": "alice", "email": "alice@example.com"}
        model.create(data)
        record = model.read(1)
        assert record.username == "alice"
        assert record.email == "alice@example.com"

    def test_update(self):
        """Test updating an existing record."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        model.create({"id": 1, "username": "bob"})
        model.update(1, {"username": "bobby"})
        record = model.read(1)
        assert record.username == "bobby"

    def test_delete(self):
        """Test deleting a record."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        model.create({"id": 1, "username": "charlie"})
        assert model.delete(1) is True
        assert model.read(1) is None
        assert model.delete(999) is False  # Non-existent ID

    def test_list_with_pagination(self):
        """Test listing records with skip and limit."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        model.create({"id": 1, "username": "dave"})
        model.create({"id": 2, "username": "eve"})
        model.create({"id": 3, "username": "frank"})
        records = model.list(skip=1, limit=1)
        assert len(records) == 1
        assert records[0].username == "eve"

    # Edge Cases and Error Handling
    def test_invalid_table_name(self):
        """Test initialization with a non-existent table."""
        try:
            ReflectedTableModel(self.session, "bogus", MasterTable)
            assert False, "Expected ValueError for invalid table name"
        except ValueError as e:
            assert str(e) == "Table 'bogus' not found in the database"

    def test_create_duplicate_id(self):
        """Test creating a record with a duplicate ID."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        model.create({"id": 1, "username": "grace"})
        try:
            model.create({"id": 1, "username": "hank"})  # Duplicate ID
            assert False, "Expected an exception for duplicate ID"
        except Exception:  # SQLite raises IntegrityError
            record = model.read(1)
            assert record.username == "grace"  # Original record unchanged

    def test_update_nonexistent(self):
        """Test updating a record that doesn’t exist."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        result = model.update(999, {"username": "nope"})
        assert result is None  # Should return None for non-existent record

    def test_list_empty_table(self):
        """Test listing from an empty table."""
        model = ReflectedTableModel(self.session, "users", MasterTable)
        records = model.list()
        assert len(records) == 0

    # Bonus: Metadata Edge Case
    def test_metadata_no_scdtype(self):
        """Test metadata extraction when scdtype is missing."""
        class NoScdTypeMaster(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            username = Column(String(50))
            # No scdtype attribute

        # Recreate table with new definition
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        model = ReflectedTableModel(self.session, "users", NoScdTypeMaster)
        assert "scdtype" not in model.metadata  # Should handle missing metadata gracefully

# Optional: Run tests if this file is executed directly
if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])


# reisuablility:

# add this to a conftest.py and put into root of testing folder.

#     import pytest
# from sqlalchemy import create_engine, Column, Integer, String, DateTime
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import Session

# Base = declarative_base()

# class MasterTable(Base):
#     """Master table definition with scdtype metadata."""
#     __tablename__ = 'users'
#     id = Column(Integer, primary_key=True)
#     username = Column(String(50))
#     email = Column(String(120))
#     created_at = Column(DateTime)
#     scdtype = "integration_test_scdtype"

# @pytest.fixture(scope="function")
# def db_session():
#     """Reusable in-memory SQLite database and session."""
#     engine = create_engine("sqlite:///:memory:")
#     Base.metadata.create_all(engine)
#     session = Session(engine)
#     yield session
#     session.close()
#     Base.metadata.drop_all(engine)