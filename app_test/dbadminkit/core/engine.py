from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .config import DBConfig, DBMode

class DBEngine:
    def __init__(self, config: DBConfig):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        if self.config.mode == DBMode.IN_MEMORY:
            return create_engine("sqlite:///:memory:", echo=True)
        elif self.config.mode in (DBMode.LIVE, DBMode.TEST):
            if not self.config.connection_string:
                raise ValueError("Connection string required for live/test mode")
            return create_engine(self.config.connection_string, echo=self.config.mode == DBMode.TEST)
        else:
            raise ValueError(f"Unknown mode: {self.config.mode}")

    def get_session(self):
        return self.Session()