# datastorekit/engine.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class DBEngine:
    def __init__(self, config: DatabaseProfile):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        if not self.config.connection_string:
            raise ValueError("Connection string required for DBEngine")
        if self.config.db_type not in ["postgres", "databricks"]:
            raise ValueError(f"DBEngine supports only 'postgres' or 'databricks', got {self.config.db_type}")
        return create_engine(self.config.connection_string, echo=True)

    def get_session(self):
        return self.Session()