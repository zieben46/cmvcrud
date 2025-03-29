from apscheduler.schedulers.blocking import BlockingScheduler

def etl_job():
    print("ETL running...")
    data = range(5)  # Extract
    transformed = [x + 1 for x in data]  # Transform
    print(f"Loaded: {list(transformed)}")  # Load

scheduler = BlockingScheduler()
scheduler.add_job(etl_job, 'interval', seconds=5)  # Runs every 5 seconds

print("Starting scheduler...")
scheduler.start()

# Pros
# Lightweight and Python-only.

# Supports cron-like scheduling and background execution.

# Can integrate with databases (optional, not required).

# Cons
# No built-in UI (though you could add one with Flask).

# Less feature-rich than Airflow for complex workflows.

