class StubDatabaseModel:
    """Test-friendly version of DatabaseModel that logs operations instead of executing them."""
    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.operations_log = []  # Store operations for verification

    def execute(self, crud_type, data):
        """Log executed operations instead of modifying the database."""
        log_entry = f"{crud_type.name} EXECUTED ON TABLE `{self.table_name}` using DATA: {data}"
        self.operations_log.append(log_entry)  # ✅ Store instead of running SQL
        return log_entry  # ✅ Simulate return behavior


def test_get_db():
    """Provides a test database session."""
    session = SessionTesting()
    try:
        yield session
    finally:
        session.close()


test_api = TableAPI(model_class=StubDatabaseModel, get_db=test_get_db)




def test_delete_entries(client, db_session, override_database_model):
    test_client, session, employees = client

    # ✅ Replace DatabaseModel with the override
    model = override_database_model(session, "employees")

    test_data = [{"id": 1, "name": "Alice", "salary": 50000}]

    token = create_access_token({"sub": "admin", "role": "admin"})
    headers = {"Authorization": f"Bearer {token}"}

    test_client.post("/employees/lock", headers=headers)

    response = test_client.delete(
        "/employees/delete",
        json=test_data,
        headers=headers
    )

    assert response.status_code == 200
    assert "entries from `employees`" in response.json()["message"]

    # ✅ Confirm the DELETE operation was captured
    assert model.operations_log == [f"DELETE EXECUTED ON TABLE `employees` using DATA: {test_data}"]





# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# TEST_DATABASE_URL = "sqlite:///:memory:"
# engine = create_engine(TEST_DATABASE_URL)
# SessionTesting = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# def test_get_db():
#     """Provides a test database session."""
#     session = SessionTesting()
#     try:
#         yield session
#     finally:
#         session.close()



# import pytest
# from fastapi.testclient import TestClient

# # ✅ Use StubDatabaseModel and test DB
# test_api = TableAPI(model_class=StubDatabaseModel, get_db=test_get_db)

# # ✅ Create a FastAPI app with only test routes
# test_app = FastAPI()
# test_app.include_router(test_api.router)

# @pytest.fixture
# def client():
#     """Create a test client with the test API."""
#     return TestClient(test_app)



# def test_create_entry(client):
#     """Test creating an entry using the test database."""
#     response = client.post("/employees/create", json=[{"id": 1, "name": "Alice", "salary": 50000}])
#     assert response.status_code == 200
#     assert "entries added" in response.json()["message"]
