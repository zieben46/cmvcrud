# replicate.py
import os
from CMVCrud.app_store.datastorekit.datastore_orchestrator import DataStoreOrchestrator
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator

# Define .env paths
env_paths = [
    os.path.join(".env", ".env.postgres"),
    os.path.join(".env", ".env.databricks")
]

# Initialize orchestrator
orchestrator = DataStoreOrchestrator(env_paths)
replicator = DatabricksToPostgresReplicator(orchestrator)

# Replicate
replicator.replicate(
    source_table="spend_plan",
    target_table="spend_plan",
    history_table="spend_plan_history",
    max_changes=20_000_000
)