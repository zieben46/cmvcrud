# dbadminkit/__main__.py
from .cli import main

if __name__ == "__main__":
    main()

#example bash (Uses Dockerized Postgres (localhost:5432) and a mock Databricks config)
    # dbadminkit sync --source postgres:employees --target databricks:employees_target --env test

#example bash (Uses live Postgres (via DBConfig.live_postgres()) and Databricks configs from env vars)
#     export DATABRICKS_HOST="adb-1234567890.1.azuredatabricks.net"
# export DATABRICKS_TOKEN="dapi1234567890abcdef1234567890abcdef"
# export DATABRICKS_HTTP_PATH="/sql/1.0/endpoints/1234567890abcdef"
# dbadminkit sync --source postgres:employees --target databricks:employees_target --env live





# Install Locally:

# python -m venv .venv
# source .venv/bin/activate
# pip install -e .


# Share with Others
# GitHub: Push to a repo (e.g., git@github.com:yourname/dbadminkit.git).
# git clone git@github.com:yourname/dbadminkit.git
# cd dbadminkit
# python -m venv .venv
# source .venv/bin/activate
# pip install -e .



