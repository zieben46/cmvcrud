import schedule
import time

# Define ETL functions
def extract():
    print("Extracting data...")
    return [1, 2, 3, 4, 5]

def transform(data):
    print("Transforming data...")
    return [x * 2 for x in data]

def load(data):
    print("Loading data...")
    print(f"Loaded: {data}")

def etl_job():
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

# Schedule the ETL job
schedule.every(5).seconds.do(etl_job)  # Runs every 5 seconds

# Run the scheduler
print("Starting scheduler...")
while True:
    schedule.run_pending()
    time.sleep(1)  # Prevents busy-waiting

#     Pros
# Minimal dependencies (just the schedule package).
# No external services or database required.
# Runs in a single Python script.

# Cons
# No UI or advanced features like retries or task dependencies.

