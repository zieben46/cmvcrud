import pytest
import docker
from dbadminkit.core.config import DBConfig

@pytest.fixture(scope="session")
def docker_client():
    return docker.from_client()

@pytest.fixture(scope="session")
def docker_postgres(docker_client):
    container = docker_client.containers.run(
        "postgres:14",
        environment={
            "POSTGRES_USER": "admin",
            "POSTGRES_PASSWORD": "password",
            "POSTGRES_DB": "dbadminkit_test"
        },
        ports={"5432/tcp": 5432},
        detach=True
    )
    yield DBConfig(
        mode="postgres",
        host="localhost",
        port=5432,
        database="dbadminkit_test",
        user="admin",
        password="password"
    )
    container.stop()
    container.remove()