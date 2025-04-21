library(testthat)
library(data.table)

# Schema for all tests
SCHEMA <- list(
  unique_id = "integer",
  category = "character",
  amount = "numeric"
)

# Driver configurations
DRIVER_CONFIGS <- list(
  list(db_type = "postgres", env_file = ".env.postgres_prod", driver_class = "PostgresDriver"),
  list(db_type = "databricks", env_file = ".env.databricks_prod", driver_class = "DatabricksDriver"),
  list(db_type = "csv", env_file = ".env.csv_prod", driver_class = "CSVDriver")
)

setup_orchestrator <- function(env_file, db_type) {
  env_path <- file.path(".env", env_file)
  # Assume a simple validation function for .env files
  if (!file.exists(env_path)) {
    stop(sprintf("Invalid .env file: %s", env_path))
  }
  DataStoreOrchestrator$new(env_paths = env_path)
}

get_driver <- function(db_type, orchestrator) {
  key <- names(orchestrator$adapters)[1]
  for (config in DRIVER_CONFIGS) {
    if (config$db_type == db_type) {
      driver_class <- get(config$driver_class)
      return(list(driver = driver_class$new(orchestrator, key), key = key))
    }
  }
  stop(sprintf("Unknown db_type: %s", db_type))
}

cleanup <- function(driver, table_name) {
  if (inherits(driver, "PostgresDriver")) {
    query <- sprintf("DROP TABLE IF EXISTS %s.%s",
                     driver$orchestrator$adapters[[driver$datastore_key]]$profile$schema,
                     table_name)
    DBI::dbExecute(driver$conn, query)
  } else if (inherits(driver, "DatabricksDriver")) {
    spark_sql(driver$spark, sprintf("DROP TABLE IF EXISTS %s.%s", driver$schema_name, table_name))
  } else if (inherits(driver, "CSVDriver")) {
    file_path <- file.path(driver$base_dir, sprintf("%s.csv", table_name))
    if (file.exists(file_path)) {
      file.remove(file_path)
    }
    if (dir.exists(driver$base_dir) && length(dir(dir$base_dir)) == 0) {
      unlink(driver$base_dir, recursive = TRUE)
    }
  }
}

# Parameterized test for CRUD operations
for (config in DRIVER_CONFIGS) {
  test_that(sprintf("CRUD operations for %s", config$db_type), {
    orchestrator <- setup_orchestrator(config$env_file, config$db_type)
    driver_info <- get_driver(config$db_type, orchestrator)
    driver <- driver_info$driver
    key <- driver_info$key

    key_field <- driver$key_field
    initial_data <- list(
      list(unique_id = 1L, category = "Food", amount = 50.0),
      list(unique_id = 2L, category = "Travel", amount = 100.0)
    )
    expected_data <- list(
      list(unique_id = 1L, category = "Food", amount = 50.0),
      list(unique_id = 2L, category = "Travel", amount = 100.0),
      list(unique_id = 3L, category = "Books", amount = 25.0)
    )
    expected_after_delete <- list(
      list(unique_id = 2L, category = "Travel", amount = 100.0),
      list(unique_id = 3L, category = "Books", amount = 25.0)
    )

    table_info <- list(
      table_name = "test_table",
      keys = key_field,
      scd_type = "type1",
      datastore_key = key,
      columns = SCHEMA
    )

    dsl <- CrudDSL$new(driver)
    dsl$
      create_table("test_table", SCHEMA, key = key_field)$
      setup_data(initial_data)$
      execute_crud("CREATE", list(list(unique_id = 3L, category = "Books", amount = 25.0)))$
      assert_table_has(expected_data)$
      execute_crud("UPDATE", list(list(amount = 75.0)), list(unique_id = 1L))$
      assert_record_exists(list(unique_id = 1L), list(category = "Food", amount = 75.0))$
      execute_crud("DELETE", list(), list(unique_id = 1L))$
      assert_table_has(expected_after_delete)

    cleanup(driver, "test_table")
  })
}

test_that("Synchronization from CSV to PostgreSQL", {
  csv_orchestrator <- setup_orchestrator(".env.csv_prod", "csv")
  postgres_orchestrator <- setup_orchestrator(".env.postgres_prod", "postgres")
  csv_driver_info <- get_driver("csv", csv_orchestrator)
  postgres_driver_info <- get_driver("postgres", postgres_orchestrator)
  csv_driver <- csv_driver_info$driver
  csv_key <- csv_driver_info$key
  postgres_driver <- postgres_driver_info$driver
  postgres_key <- postgres_driver_info$key

  csv_table_info <- list(
    table_name = "test_table",
    keys = csv_driver$key_field,
    scd_type = "type1",
    datastore_key = csv_key,
    columns = SCHEMA
  )
  initial_data <- list(
    list(unique_id = 1L, category = "Food", amount = 50.0),
    list(unique_id = 2L, category = "Travel", amount = 100.0)
  )

  postgres_table_info <- list(
    table_name = "test_table_copy",
    keys = postgres_driver$key_field,
    scd_type = "type1",
    datastore_key = postgres_key,
    columns = SCHEMA
  )

  csv_dsl <- SyncDSL$new(csv_driver, postgres_driver)
  postgres_dsl <- CrudDSL$new(postgres_driver)
  csv_dsl$
    create_table("test_table", SCHEMA, key = csv_driver$key_field)$
    setup_data(initial_data)$
    select_table("test_table", key = csv_driver$key_field)
  postgres_dsl$create_table("test_table_copy", SCHEMA, key = postgres_driver$key_field)
  csv_dsl$
    sync_to_target("test_table_copy", sync_method = "full_load")$
    assert_tables_synced()

  cleanup(csv_driver, "test_table")
  cleanup(postgres_driver, "test_table_copy")
})