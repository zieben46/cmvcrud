import pytest
from datastorekit.replication.databricks_to_postgres import DatabricksToPostgresReplicator

def test_replicator():
    # Setup mock orchestrator and adapters
    orchestrator = DataStoreOrchestrator(profiles=[
        DatabaseProfile(db_type="databricks", dbname="databricks_db", keys="unique_id,secondary_key"),
        DatabaseProfile(db_type="postgres", dbname="spend_plan_db", keys="unique_id,secondary_key")
    ])
    replicator = DatabricksToPostgresReplicator(orchestrator, full_load_batch_size=1000, cdf_batch_size=500)

    # Test full load
    assert replicator.replicate("source_table", "target_table", "history_table")

    # Test incremental merge
    # Simulate CDF changes and history
    assert replicator.replicate("source_table", "target_table", "history_table")