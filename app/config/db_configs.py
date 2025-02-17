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