from sqlalchemy.engine.url import URL

import os

CREDENTIALS = [
    "POSTGRES_DRIVERNAME",
    "POSTGRES_USERNAME",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DATABASE"
]

class PostgresConfig():

    def __init__(self, getenv_func=os.getenv):
        self.getenv_func = getenv_func

    def _load_credentials(self):
        credentials = {key: self.getenv_func(key) for key in CREDENTIALS}
        missing_keys = [key for key, value in credentials.items() if value is None]
        if missing_keys:
            raise ValueError(f"Missing environment variables: {', '.join(missing_keys)}")
        return credentials
    
    def get_url(self):
        credentials = self._load_credentials()
        DATABASE_URL = URL.create(
            drivername=credentials["POSTGRES_DRIVERNAME"],
            username=credentials["POSTGRES_USERNAME"],
            password=credentials["POSTGRES_PASSWORD"],
            host=credentials["POSTGRES_HOST"],
            port=int(credentials["POSTGRES_PORT"]),
            database=credentials["POSTGRES_DATABASE"]
        )
        return DATABASE_URL
    
    import os
from pydantic import BaseSettings

class Settings(BaseSettings):
    CONFIG_ENV: str = os.getenv("CONFIG_ENV", "DEV")  # Default to DEV

    POSTGRES_USERNAME: str = os.getenv("POSTGRES_USERNAME", "default_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "default_pass")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_DATABASE: str = os.getenv("POSTGRES_DATABASE", "default_db")

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.POSTGRES_USERNAME}:{self.POSTGRES_PASSWORD}" \
               f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DATABASE}"

settings = Settings()