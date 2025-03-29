from celery import Celery

# 1:Create tasks.py:
# (tasks.py)

app = Celery('tasks', broker='sqla+sqlite:///celerydb.sqlite')  # SQLite as broker

@app.task
def etl_task():
    print("ETL running...")
    data = [1, 2, 3]  # Extract
    transformed = [x * 3 for x in data]  # Transform
    print(f"Loaded: {transformed}")  # Load

# 2:  Create beat_config.py to schedule it
    # beat_config.py
    from celery import Celery
from celery.schedules import crontab

app = Celery('tasks', broker='sqla+sqlite:///celerydb.sqlite')
app.conf.beat_schedule = {
    'run-etl-every-30-seconds': {
        'task': 'tasks.etl_task',
        'schedule': 30.0,  # Every 30 seconds
    },
}


# 3:Run Celery worker and beat

# celery -A tasks worker --loglevel=info
# celery -A beat_config beat --loglevel=info


# Pros
# Lightweight if using SQLite as the broker.
# Supports task queues and scheduling.
# Scalable with a proper broker (Redis/RabbitMQ) later if needed.

# Cons
# Requires running two processes (worker and beat).
# More complex than schedule or APScheduler.

