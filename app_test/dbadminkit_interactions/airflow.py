from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dbadminkit.core.config import DBConfig
from dbadminkit.admin_operations import AdminDBOps
from dbadminkit.etl_trigger import ETLTrigger

# Configs
pg_config = DBConfig.live_postgres("postgresql://user:pass@localhost:5432/dbname")
db_config = DBConfig.live_postgres("databricks+connector://user:pass@databricks_host/dbname")
etl_endpoint = "http://etl-server:8080/jobs"

# Task Functions
def sync_scd2_postgres_to_databricks():
    pg_ops = AdminDBOps(pg_config)
    db_ops = AdminDBOps(db_config)
    source_table_info = {"table_name": "employees_source", "key": "emp_id", "scd_type": "type2"}
    target_table_info = {"table_name": "employees_target", "key": "emp_id", "scd_type": "type1"}
    applied_count = db_ops.sync_scd2_versions(pg_ops, source_table_info, target_table_info, min_version=1, max_version=3)
    print(f"Applied {applied_count} changes from Postgres to Databricks")
    return applied_count

def trigger_etl_job():
    etl = ETLTrigger(etl_endpoint)
    etl_params = {"source_table": "employees_target", "target_table": "data_warehouse"}
    response = etl.trigger_job("load_warehouse", etl_params)
    print("ETL job response:", response)
    return response

# DAG Definition
with DAG(
    dag_id="scd2_sync_with_etl",
    start_date=datetime(2025, 3, 8),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    sync_task = PythonOperator(
        task_id="sync_scd2_postgres_to_databricks",
        python_callable=sync_scd2_postgres_to_databricks,
    )
    trigger_etl_task = PythonOperator(
        task_id="trigger_etl_job",
        python_callable=trigger_etl_job,
    )

    sync_task >> trigger_etl_task