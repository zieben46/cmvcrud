# schedule_replication.py
import os
import logging
from sqlalchemy.orm import Session
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator
from datastorekit.models.table_info import TableInfo
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/path/to/replication.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define .env paths
env_paths = [
    os.path.join(".env", ".env.postgres"),
    os.path.join(".env", ".env.databricks")
]

# Initialize orchestrator
try:
    orchestrator = DataStoreOrchestrator(env_paths)
    logger.info("Initialized orchestrator with adapters: %s", orchestrator.list_adapters())
except Exception as e:
    logger.error("Failed to initialize orchestrator: %s", e)
    exit(1)

# Initialize replicator
replicator = DatabricksToPostgresReplicator(orchestrator, full_load_batch_size=100000, cdf_batch_size=500)

# Query table_info and replicate enabled tables
adapter = orchestrator.adapters["spend_plan_db:safe_user"]
with adapter.session_factory() as session:
    table_infos = session.query(TableInfo).filter_by(enabled=True, schedule_frequency="hourly").all()
    for ti in table_infos:
        if not ti.last_replicated or ti.last_replicated < datetime.now() - timedelta(hours=1):
            logger.info("Replicating table: %s", ti.table_name)
            try:
                success = replicator.replicate(
                    source_table=ti.table_name,
                    target_table=ti/swift
                    history_table=f"{ti.table_name}_history",
                    max_changes=1000000
                )
                ti.last_status = "success" if success else "failed"
                ti.last_error = None
                ti.last_replicated = datetime.now()
                logger.info("Replication %s for %s", ti.last_status, ti.table_name)
            except Exception as e:
                ti.last_status = "failed"
                ti.last_error = str(e)
                logger.error("Replication failed for %s: %s", ti.table_name, e)
            session.commit()