import pytest
# from httpx import AsyncClient  # For async testing if needed
from app.api.auth import create_access_token

@pytest.mark.asyncio  # Remove if not using async endpoints
async def test_create_entries(client, setup_tables):
    # Get a JWT token
    token = create_access_token({"sub": "admin", "role": "admin"})
    headers = {"Authorization": f"Bearer {token}"}

    # Test CREATE
    response = client.post(
        "/employees/create",
        json=[{"id": 1, "name": "Alice", "salary": 50000}],
        headers=headers,
    )
    assert response.status_code == 200
    assert "1 entries added" in response.json()["message"]

    # Verify in DB
    result = client.get("/employees/read", headers=headers)
    assert result.status_code == 200
    assert result.json() == [{"id": 1, "name": "Alice", "salary": 50000}]

@pytest.mark.asyncio
async def test_table_locking(client, setup_tables):
    token_admin = create_access_token({"sub": "admin", "role": "admin"})
    token_user = create_access_token({"sub": "user", "role": "user"})
    headers_admin = {"Authorization": f"Bearer {token_admin}"}
    headers_user = {"Authorization": f"Bearer {token_user}"}

    # Lock table as admin
    response = client.post("/employees/lock", headers=headers_admin)
    assert response.status_code == 200
    assert "locked by `admin`" in response.json()["message"]

    # Try to update as user (should fail)
    response = client.put(
        "/employees/update",
        json=[{"id": 1, "name": "Bob"}],
        headers=headers_user,
    )
    assert response.status_code == 403
    assert "locked by `admin`" in response.text

@pytest.mark.asyncio
async def test_scd_type1_update(client, setup_tables):
    token = create_access_token({"sub": "admin", "role": "admin"})
    headers = {"Authorization": f"Bearer {token}"}

    # Insert initial data
    client.post("/employees/create", json=[{"id": 1, "name": "Alice", "salary": 50000}], headers=headers)

    # Update (SCD Type 1 overwrites)
    response = client.put(
        "/employees/update",
        json=[{"id": 1, "name": "Bob", "salary": 60000}],
        headers=headers,
    )
    assert response.status_code == 200
    assert "1 entries" in response.json()["message"]

    # Verify update
    result = client.get("/employees/read", headers=headers)
    assert result.json() == [{"id": 1, "name": "Bob", "salary": 60000}]