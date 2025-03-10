from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from app_test.dbadminkit.core.database_profile import DBConfig
from app_test.dbadminkit.database_manager import DBManager
from dbadminkit.etl_trigger import ETLTrigger
from app_test.dbadminkit.core.crud_types import CRUDOperation

# Configs
pg_config = DBConfig.live_postgres("postgresql://user:pass@localhost:5432/dbname")
db_config = DBConfig.live_postgres("databricks+connector://user:pass@databricks_host/dbname")
etl_endpoint = "http://etl-server:8080/jobs"

# Task Functions
def read_cdc_from_postgres(ti):
    pg_ops = DBManager(pg_config)
    cdc_table_info = {"table_name": "cdc_log", "key": "emp_id", "scd_type": "type1"}
    last_ts = ti.xcom_pull(task_ids="sync_scd2_postgres_to_databricks", key="last_processed_ts") or "1970-01-01"
    cdc_records = pg_ops.perform_crud(cdc_table_info, CRUDOperation.READ, {"timestamp__gt": last_ts})
    ti.xcom_push(key="last_processed_ts", value=max(r["timestamp"] for r in cdc_records))
    return cdc_records

def sync_scd2_postgres_to_databricks(ti):
    pg_ops = DBManager(pg_config)
    db_ops = DBManager(db_config)
    source_table_info = {"table_name": "employees_source", "key": "emp_id", "scd_type": "type2"}
    target_table_info = {"table_name": "employees_target", "key": "emp_id", "scd_type": "type1"}
    applied_count = db_ops.sync_scd2_versions(pg_ops, source_table_info, target_table_info, min_version=1, max_version=3)
    cdc_records = ti.xcom_pull(task_ids="read_cdc_from_postgres")
    if cdc_records:
        db_ops.process_cdc_logs(target_table_info, cdc_records)
        applied_count += len(cdc_records)
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
    dag_id="full_sync_workflow",
    start_date=datetime(2025, 3, 8),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    read_cdc_task = PythonOperator(
        task_id="read_cdc_from_postgres",
        python_callable=read_cdc_from_postgres,
    )
    sync_task = PythonOperator(
        task_id="sync_scd2_postgres_to_databricks",
        python_callable=sync_scd2_postgres_to_databricks,
    )
    trigger_etl_task = PythonOperator(
        task_id="trigger_etl_job",
        python_callable=trigger_etl_job,
    )

    read_cdc_task >> sync_task >> trigger_etl_task