# dbadminkit/cli.py
import argparse
import os
from dbadminkit.core.admin_operations import AdminDBOps
from dbadminkit.core.config import DBConfig

def get_config(env, db_type):
    """Retrieve config based on environment (test/live) and database type."""
    if env == "test":
        if db_type == "postgres":
            return DBConfig(
                mode="postgres",
                host="localhost",  # Dockerized Postgres
                port=5432,
                database="dbadminkit_test",
                user="admin",
                password="password"
            )
        elif db_type == "databricks":
            # Mock or test Databricks config (adjust as needed)
            return DBConfig(
                mode="databricks",
                host="localhost",  # Simulated for test
                token="test_token",
                http_path="/sql/1.0/endpoints/test"
            )
    elif env == "live":
        # Use env vars for live configs (set by user)
        if db_type == "postgres":
            return DBConfig.live_postgres()
        elif db_type == "databricks":
            return DBConfig.live_databricks(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN"),
                http_path=os.getenv("DATABRICKS_HTTP_PATH")
            )
    raise ValueError(f"Unsupported env: {env} or db_type: {db_type}")

def main():
    parser = argparse.ArgumentParser(description="Database administration toolkit (dbadminkit)")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync a table between source and target")
    sync_parser.add_argument("--source", required=True, help="Source database and table (e.g., postgres:employees)")
    sync_parser.add_argument("--target", required=True, help="Target database and table (e.g., databricks:employees_target)")
    sync_parser.add_argument("--env", choices=["test", "live"], default="live", help="Environment: test or live")

    args = parser.parse_args()

    if args.command == "sync":
        # Parse source and target
        source_db, source_table = args.source.split(":", 1)
        target_db, target_table = args.target.split(":", 1)

        # Get configs based on environment
        source_config = get_config(args.env, source_db)
        target_config = get_config(args.env, target_db)

        # Instantiate AdminDBOps
        source_ops = AdminDBOps(source_config)
        target_ops = AdminDBOps(target_config)

        # Call the sync method
        source_info = {"table_name": source_table}
        target_info = {"table_name": target_table}
        if source_db == "postgres" and target_db == "databricks":
            source_ops.sync_scd2_versions(target_ops, source_info, target_info)
        elif source_db == "databricks" and target_db == "postgres":
            source_ops.transfer_with_jdbc(source_info, target_config)
        else:
            raise ValueError(f"Unsupported sync direction: {source_db} to {target_db}")

if __name__ == "__main__":
    main()