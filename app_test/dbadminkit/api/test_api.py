import pytest
from httpx import AsyncClient
from fastapi import FastAPI
from utils import get_db_managers, DatabaseProfile, oauth2_scheme, create_jwt_token
from fastapi_app import (
    login, get_authorized_tables, lock_table_endpoint, unlock_table_endpoint,
    ingest_cdf, sync_table, get_last_sync_version_endpoint, update_sync_version,
    execute_crud, sync_between_dbs
)

# Helper function to create a test app instance
def create_test_app():
    app = FastAPI()
    
    # Import endpoints directly into the test app
    app.post("/login")(login)
    app.get("/get-authorized-tables")(get_authorized_tables)
    app.post("/lock-table")(lock_table_endpoint)
    app.post("/unlock-table")(unlock_table_endpoint)
    app.post("/ingest-cdf")(ingest_cdf)
    app.post("/sync-table")(sync_table)
    app.get("/get-last-sync-version")(get_last_sync_version_endpoint)
    app.post("/update-sync-version")(update_sync_version)
    app.post("/execute-crud")(execute_crud)
    app.post("/sync-between-dbs")(sync_between_dbs)
    
    return app

# Helper function to create mock db_managers
def create_mock_db_managers():
    test_postgres_profile = DatabaseProfile.test_postgres()
    test_databricks_profile = DatabaseProfile.test_databricks()
    return get_db_managers(postgres_profile=test_postgres_profile, databricks_profile=test_databricks_profile)

# Helper function to create a mock JWT token
def create_mock_token():
    return create_jwt_token({"sub": "testuser", "role": "editor"})

# Test login
@pytest.mark.asyncio
async def test_login():
    app = create_test_app()
    mock_db_managers = create_mock_db_managers()
    app.dependency_overrides[get_db_managers] = lambda: mock_db_managers
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/login", data={"username": "testuser", "password": "testpass"})
        assert response.status_code == 200
        assert "access_token" in response.json()

# Test ingest-cdf
@pytest.mark.asyncio
async def test_ingest_cdf():
    app = create_test_app()
    mock_db_managers = create_mock_db_managers()
    app.dependency_overrides[get_db_managers] = lambda: mock_db_managers
    
    mock_token = create_mock_token()
    headers = {"Authorization": f"Bearer {mock_token}"}
    changes = [{"id": "1", "change_type": "insert", "timestamp": "2025-03-22T12:00:00"}]
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post(
            "/ingest-cdf?target=postgres&table_name=test_table",
            json=changes,
            headers=headers
        )
        assert response.status_code == 200
        assert response.json()["status"] == "processing"

# Test sync-table
@pytest.mark.asyncio
async def test_sync_table():
    app = create_test_app()
    mock_db_managers = create_mock_db_managers()
    app.dependency_overrides[get_db_managers] = lambda: mock_db_managers
    
    mock_token = create_mock_token()
    headers = {"Authorization": f"Bearer {mock_token}"}
    new_data = [{"id": "1", "name": "Test"}]
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post(
            "/sync-table?target=postgres&table_name=test_table",
            json=new_data,
            headers=headers
        )
        assert response.status_code == 200
        assert response.json()["status"] == "synced"

# Test get-last-sync-version
@pytest.mark.asyncio
async def test_get_last_sync_version():
    app = create_test_app()
    mock_db_managers = create_mock_db_managers()
    app.dependency_overrides[get_db_managers] = lambda: mock_db_managers
    
    mock_token = create_mock_token()
    headers = {"Authorization": f"Bearer {mock_token}"}
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(
            "/get-last-sync-version?db_type=postgres&table_name=test_table",
            headers=headers
        )
        assert response.status_code == 200
        assert "last_version" in response.json()

# Test update-sync-version
@pytest.mark.asyncio
async def test_update_sync_version():
    app = create_test_app()
    mock_db_managers = create_mock_db_managers()
    app.dependency_overrides[get_db_managers] = lambda: mock_db_managers
    
    mock_token = create_mock_token()
    headers = {"Authorization": f"Bearer {mock_token}"}
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post(
            "/update-sync-version?db_type=postgres&table_name=test_table",
            json={"version": 10},
            headers=headers
        )
        assert response.status_code == 200
        assert response.json()["status"] == "version_updated"

if __name__ == "__main__":
    pytest.main(["-v"])