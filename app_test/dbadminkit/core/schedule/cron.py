def etl():
    print("ETL running...")
    data = [1, 2, 3]  # Extract
    transformed = [x * 10 for x in data]  # Transform
    print(f"Loaded: {transformed}")  # Load

if __name__ == "__main__":
    etl()


    crontab -e



    # */5 * * * * /usr/bin/python3 /path/to/etl.py >> /path/to/logfile.log 2>&1

#     Pros
# No Python package dependencies.

# Built into Unix-like systems.

# Reliable for simple scheduling.

# Cons
# No Windows support without WSL or third-party tools (e.g., Task Scheduler).

# No task dependencies or advanced logic without scripting.

