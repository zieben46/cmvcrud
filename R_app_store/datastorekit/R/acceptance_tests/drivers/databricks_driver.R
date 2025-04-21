library(R6)
library(sparklyr)

DatabricksDriver <- R6Class(
  "DatabricksDriver",
  inherit = DatabaseDriver,
  public = list(
    orchestrator = NULL,
    datastore_key = NULL,
    spark = NULL,
    schema_name = NULL,
    key_field = "unique_id",

    initialize = function(orchestrator, datastore_key) {
      self$orchestrator <- orchestrator
      self$datastore_key <- datastore_key
      self$spark <- orchestrator$adapters[[datastore_key]]$connection$spark
      self$schema_name <- orchestrator$adapters[[datastore_key]]$profile$schema
    },

    create_table = function(table_name, schema) {
      columns <- sapply(names(schema), function(col) {
        type <- switch(schema[[col]],
                       integer = "INTEGER",
                       character = "STRING",
                       numeric = "FLOAT",
                       stop(sprintf("Unsupported column type: %s", schema[[col]])))
        sprintf("%s %s", col, type)
      })
      query <- sprintf("CREATE TABLE %s.%s (%s) USING DELTA", self$schema_name, table_name, paste(columns, collapse = ", "))
      spark_sql(self$spark, query)
      message(sprintf("Created Delta table %s.%s", self$schema_name, table_name))
    },

    create = function(table_info, data) {
      table <- self$orchestrator$get_table(self$datastore_key, TableInfo$new(
        table_name = table_info$table_name,
        keys = table_info$keys,
        scd_type = table_info$scd_type,
        datastore_key = table_info$datastore_key,
        columns = table_info$columns
      ))
      table$create(data)
    },

    read = function(table_info, filters) {
      table <- self$orchestrator$get_table(self$datastore_key, TableInfo$new(
        table_name = table_info$table_name,
        keys = table_info$keys,
        scd_type = table_info$scd_type,
        datastore_key = table_info$datastore_key,
        columns = table_info$columns
      ))
      table$read(filters)
    },

    update = function(table_info, data, filters) {
      table <- self$orchestrator$get_table(self$datastore_key, TableInfo$new(
        table_name = table_info$table_name,
        keys = table_info$keys,
        scd_type = table_info$scd_type,
        datastore_key = table_info$datastore_key,
        columns = table_info$columns
      ))
      table$update(data, filters)
    },

    delete = function(table_info, filters) {
      table <- self$orchestrator$get_table(self$datastore_key, TableInfo$new(
        table_name = table_info$table_name,
        keys = table_info$keys,
        scd_type = table_info$scd_type,
        datastore_key = table_info$datastore_key,
        columns = table_info$columns
      ))
      table$delete(filters)
    },

    sync_to = function(source_table_info, target_driver, target_table, method) {
      if (method != "full_load") {
        stop(sprintf("Unsupported sync method: %s", method))
      }
      source_table <- source_table_info$table_name
      source_schema <- self$orchestrator$adapters[[self$datastore_key]]$profile$schema
      target_datastore_key <- target_driver$datastore_key
      target_db <- strsplit(target_datastore_key, ":")[[1]][1]
      target_schema <- strsplit(target_datastore_key, ":")[[1]][2]
      self$orchestrator$replicate(
        source_db = self$orchestrator$adapters[[self$datastore_key]]$profile$dbname,
        source_schema = source_schema,
        source_table = source_table,
        target_db = target_db,
        target_schema = target_schema,
        target_table = target_table
      )
    }
  )
)