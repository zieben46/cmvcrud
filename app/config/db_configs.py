# app/config/db_configs.py
from pydantic import BaseSettings
from typing import Literal

class Settings(BaseSettings):
    CONFIG_ENV: Literal["DEV", "LIVE", "TEST_PG1", "TEST_PG2"] = "DEV"  # Environment choices

    # Default PostgreSQL settings (overridden by .env file if provided)
    POSTGRES_USERNAME: str = "default_user"
    POSTGRES_PASSWORD: str = "default_pass"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DATABASE: str = "default_db"

    # Specific settings for each environment (can be overridden via env vars)
    # LIVE environment
    LIVE_POSTGRES_HOST: str = "live_host"
    LIVE_POSTGRES_PORT: int = 5432
    LIVE_POSTGRES_DATABASE: str = "live_db"
    LIVE_POSTGRES_USERNAME: str = "live_user"
    LIVE_POSTGRES_PASSWORD: str = "live_pass"

    # DEV environment
    DEV_POSTGRES_HOST: str = "dev_host"
    DEV_POSTGRES_PORT: int = 5432
    DEV_POSTGRES_DATABASE: str = "dev_db"
    DEV_POSTGRES_USERNAME: str = "dev_user"
    DEV_POSTGRES_PASSWORD: str = "dev_pass"

    # In-memory PostgreSQL (for testing, we'll simulate with local instances)
    TEST_PG1_POSTGRES_HOST: str = "localhost"  # First test instance
    TEST_PG1_POSTGRES_PORT: int = 5433  # Different port for test instance 1
    TEST_PG1_POSTGRES_DATABASE: str = "test_db1"
    TEST_PG1_POSTGRES_USERNAME: str = "test_user1"
    TEST_PG1_POSTGRES_PASSWORD: str = "test_pass1"

    TEST_PG2_POSTGRES_HOST: str = "localhost"  # Second test instance
    TEST_PG2_POSTGRES_PORT: int = 5434  # Different port for test instance 2
    TEST_PG2_POSTGRES_DATABASE: str = "test_db2"
    TEST_PG2_POSTGRES_USERNAME: str = "test_user2"
    TEST_PG2_POSTGRES_PASSWORD: str = "test_pass2"

    class Config:
        env_file = ".env"  # Load from .env file if present
        env_file_encoding = "utf-8"

    @property
    def database_url(self) -> str:
        """Return the appropriate database URL based on CONFIG_ENV."""
        drivername = "postgresql+psycopg2"  # Use psycopg2 for PostgreSQL
        if self.CONFIG_ENV == "LIVE":
            return (
                f"{drivername}://{self.LIVE_POSTGRES_USERNAME}:"
                f"{self.LIVE_POSTGRES_PASSWORD}@{self.LIVE_POSTGRES_HOST}:"
                f"{self.LIVE_POSTGRES_PORT}/{self.LIVE_POSTGRES_DATABASE}"
            )
        elif self.CONFIG_ENV == "DEV":
            return (
                f"{drivername}://{self.DEV_POSTGRES_USERNAME}:"
                f"{self.DEV_POSTGRES_PASSWORD}@{self.DEV_POSTGRES_HOST}:"
                f"{self.DEV_POSTGRES_PORT}/{self.DEV_POSTGRES_DATABASE}"
            )
        elif self.CONFIG_ENV == "TEST_PG1":
            return (
                f"{drivername}://{self.TEST_PG1_POSTGRES_USERNAME}:"
                f"{self.TEST_PG1_POSTGRES_PASSWORD}@{self.TEST_PG1_POSTGRES_HOST}:"
                f"{self.TEST_PG1_POSTGRES_PORT}/{self.TEST_PG1_POSTGRES_DATABASE}"
            )
        elif self.CONFIG_ENV == "TEST_PG2":
            return (
                f"{drivername}://{self.TEST_PG2_POSTGRES_USERNAME}:"
                f"{self.TEST_PG2_POSTGRES_PASSWORD}@{self.TEST_PG2_POSTGRES_HOST}:"
                f"{self.TEST_PG2_POSTGRES_PORT}/{self.TEST_PG2_POSTGRES_DATABASE}"
            )
        else:
            raise ValueError(f"Unknown CONFIG_ENV: {self.CONFIG_ENV}")

# Load settings
settings = Settings()