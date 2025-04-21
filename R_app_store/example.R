# Load required libraries
library(R6)
library(data.table)

# Source the codebase files (adjust paths as needed)
source("datastorekit/R/profile.R")
source("datastorekit/R/connection.R")
source("datastorekit/R/models/table_info.R")
source("datastorekit/R/models/db_table.R")
source("datastorekit/R/adapters/base.R")
source("datastorekit/R/adapters/postgres.R")
source("datastorekit/R/adapters/databricks.R")
source("datastorekit/R/adapters/csv.R")
source("datastorekit/R/orchestrator.R")

# Define schema for tables
SCHEMA <- list(
  unique_id = "integer",
  category = "character",
  amount = "numeric"
)

# Initialize orchestrator with .env files
env_paths <- c(
  file.path(".env", ".env.postgres_prod"),
  file.path(".env", ".env.csv_prod")
)
orchestrator <- DataStoreOrchestrator$new(env_paths = env_paths)

# List available adapters
cat("Available adapters:\n")
print(orchestrator$list_adapters())
# Output: e.g., "prod_db:public" "csv_db:default"

# Define table info for PostgreSQL
postgres_table_info <- TableInfo$new(
  table_name = "test_table",
  keys = "unique_id",
  scd_type = "type1",
  datastore_key = "prod_db:public",
  columns = SCHEMA
)

# Define table info for CSV
csv_table_info <- TableInfo$new(
  table_name = "test_table",
  keys = "unique_id",
  scd_type = "type1",
  datastore_key = "csv_db:default",
  columns = SCHEMA
)

# Create tables
postgres_table <- orchestrator$get_table("prod_db:public", postgres_table_info)
csv_table <- orchestrator$get_table("csv_db:default", csv_table_info)

# Insert data into PostgreSQL table
data <- list(
  list(unique_id = 1L, category = "Food", amount = 50.0),
  list(unique_id = 2L, category = "Travel", amount = 100.0)
)
postgres_table$create(data)
cat("Inserted data into PostgreSQL table\n")

# Read data from PostgreSQL table
read_data <- postgres_table$read(list())
cat("Data from PostgreSQL table:\n")
print(read_data)
# Output: list of records, e.g., list(list(unique_id = 1L, category = "Food", amount = 50.0), ...)

# Update data in PostgreSQL table
postgres_table$update(list(list(amount = 75.0)), list(unique_id = 1L))
cat("Updated data in PostgreSQL table\n")

# Read updated data
updated_data <- postgres_table$read(list(unique_id = 1L))
cat("Updated data from PostgreSQL table:\n")
print(updated_data)
# Output: list(list(unique_id = 1L, category = "Food", amount = 75.0))

# Replicate data from PostgreSQL to CSV
orchestrator$replicate(
  source_db = "prod_db",
  source_schema = "public",
  source_table = "test_table",
  target_db = "csv_db",
  target_schema = "default",
  target_table = "test_table"
)
cat("Replicated data from PostgreSQL to CSV\n")

# Read data from CSV table
csv_data <- csv_table$read(list())
cat("Data from CSV table:\n")
print(csv_data)
# Output: same as PostgreSQL data

# List tables in PostgreSQL
tables <- orchestrator$list_tables("prod_db", "public")
cat("Tables in PostgreSQL (public schema):\n")
print(tables)
# Output: e.g., c("test_table",