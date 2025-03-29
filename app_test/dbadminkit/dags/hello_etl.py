from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    print("✅ Hello from Airflow!")

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

with DAG(
    dag_id='hello_etl',
    default_args=default_args,
    description='The simplest DAG to test Airflow',
    schedule_interval='* * * * *',  # every minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )


# (Terminal)
# export AIRFLOW_HOME=~/airflow
# export AIRFLOW__CORE__DAGS_FOLDER=~/dbadminkit/dags



# (Initialize & start Airflow)
# Initialize Airflow DB (only first time)
airflow db init

# Start scheduler (separate terminal)
airflow scheduler

# Start webserver (separate terminal)
airflow webserver --port 8080


# go to http://localhost:8080