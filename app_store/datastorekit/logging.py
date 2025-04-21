# datastorekit/utils/logging.py
import logging
import os
from typing import Optional, Dict, Any
from datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.models.table_info import TableInfo
from datetime import datetime

class DatastoreLogHandler(logging.Handler):
    def __init__(self, orchestrator: DataStoreOrchestrator, datastore_key: str, table_name: str):
        super().__init__()
        self.orchestrator = orchestrator
        self.datastore_key = datastore_key
        self.table_name = table_name
        self.table = self._get_log_table()

    def _get_log_table(self):
        table_info = TableInfo(
            table_name=self.table_name,
            keys="id",
            scd_type="type0",
            datastore_key=self.datastore_key,
            columns={
                "id": "Integer",
                "timestamp": "DateTime",
                "level": "String",
                "message": "String",
                "query": "String",
                "parameters": "String",
                "table_name": "String",
                "adapter": "String",
                "source": "String"
            }
        )
        return self.orchestrator.get_table(self.datastore_key, table_info)

    def emit(self, record):
        try:
            log_entry = {
                "id": None,  # Auto-incremented by datastore
                "timestamp": datetime.utcnow(),
                "level": record.levelname,
                "message": record.msg,
                "query": getattr(record, "query", None),
                "parameters": json.dumps(getattr(record, "parameters", None)) if getattr(record, "parameters", None) else None,
                "table_name": getattr(record, "table", None),
                "adapter": getattr(record, "adapter", None),
                "source": record.pathname
            }
            self.table.insert([log_entry])
        except Exception as e:
            print(f"Failed to log to datastore: {e}")

def setup_logging(fallback_file: str, datastore_key: Optional[str] = None, table_name: Optional[str] = None, env_paths: Optional[list] = None):
    """Set up logging to a file and optionally a datastore."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(fallback_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(stream_handler)

    # Datastore handler
    if datastore_key and table_name and env_paths:
        try:
            orchestrator = DataStoreOrchestrator(env_paths)
            datastore_handler = DatastoreLogHandler(orchestrator, datastore_key, table_name)
            logger.addHandler(datastore_handler)
        except Exception as e:
            print(f"Failed to initialize datastore logging: {e}")

    return logger