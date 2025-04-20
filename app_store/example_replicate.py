# replicate_databricks_to_postgres.py
import os
import logging
from datastorekit.orchestrator import DataStoreOrchestrator
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator
from datastorekit.models.table_info import TableInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("replication.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define .env paths
env_paths = [
    os.path.join(".env", ".env.databricks"),
    os.path.join(".env", ".env.postgres")
]

# Initialize orchestrator
try:
    orchestrator = DataStoreOrchestrator(env_paths)
    logger.info("Initialized orchestrator with adapters: %s", orchestrator.list_adapters())
except Exception as e:
    logger.error("Failed to initialize orchestrator: %s", e)
    exit(1)

# Initialize replicator
replicator = DatabricksToPostgresReplicator(orchestrator, full_load_batch_size=100000, cdf_batch_size=1000)

# Define TableInfo for source (Databricks) and target (PostgreSQL)
source_table_info = TableInfo(
    table_name="spend_plan",
    keys="unique_id,secondary_key",
    scd_type="type2",
    datastore_key="databricks_db:default",
    columns={
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
)
target_table_info = TableInfo(
    table_name="spend_plan",
    keys="unique_id,secondary_key",
    scd_type="type2",
    datastore_key="spend_plan_db:safe_user",
    columns={
        "unique_id": "Integer",
        "secondary_key": "String",
        "category": "String",
        "amount": "Float",
        "start_date": "DateTime",
        "end_date": "DateTime",
        "is_active": "Boolean"
    }
)

# Replicate
try:
    success = replicator.replicate(
        source_table="spend_plan",
        target_table="spend_plan",
        history_table="spend_plan_history",
        max_changes=20000000
    )
    logger.info("Replication %s", "successful" if success else "failed")
except Exception as e:
    logger.error("Replication failed: %s", e)