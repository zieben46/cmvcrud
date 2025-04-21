library(R6)
library(sparklyr)
library(data.table)

DatabricksAdapter <- R6Class(
  "DatabricksAdapter",
  inherit = DatastoreAdapter,
  public = list(
    connection = NULL,

    initialize = function(profile) {
      super$initialize(profile)
      self$connection <- DatastoreConnection$new(profile)
    },

    validate_keys = function(table_name, table_info_keys) {
      tryCatch({
        df <- spark_read_delta(self$connection$spark, sprintf("%s.%s", self$profile$schema, table_name))
        columns <- colnames(df)
        if (!all(table_info_keys %in% columns)) {
          stop(sprintf("Table %s columns %s do not include all TableInfo keys %s",
                       table_name, paste(columns, collapse = ","), paste(table_info_keys, collapse = ",")))
        }
      }, error = function(e) {
        message(sprintf("Failed to validate keys for table %s: %s", table_name, e$message))
      })
    },

    insert = function(table_name, data) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      dt <- as.data.table(data)
      df <- copy_to(self$connection$spark, dt, temporary = TRUE)
      spark_write_delta(df, sprintf("%s.%s", self$profile$schema, table_name), mode = "append")
    },

    select = function(table_name, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      df <- spark_read_delta(self$connection$spark, sprintf("%s.%s", self$profile$schema, table_name))
      if (!is.null(filters) && length(filters) > 0) {
        for (name in names(filters)) {
          df <- dplyr::filter(df, !!sym(name) == filters[[name]])
        }
      }
      as.list(collect(df))
    },

    update = function(table_name, data, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      delta_table <- spark_read_delta(self$connection$spark, sprintf("%s.%s", self$profile$schema, table_name))
      for (update_data in data) {
        condition <- paste(sprintf("target.%s = '%s'", names(filters), unlist(filters)), collapse = " AND ")
        update_clause <- paste(sprintf("%s = '%s'", names(update_data), unlist(update_data)), collapse = ", ")
        spark_sql(self$connection$spark, sprintf(
          "UPDATE %s.%s SET %s WHERE %s",
          self$profile$schema, table_name, update_clause, condition
        ))
      }
    },

    delete = function(table_name, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      condition <- paste(sprintf("%s = '%s'", names(filters), unlist(filters)), collapse = " AND ")
      spark_sql(self$connection$spark, sprintf(
        "DELETE FROM %s.%s WHERE %s",
        self$profile$schema, table_name, condition
      ))
    },

    list_tables = function(schema) {
      tables <- spark_sql(self$connection$spark, sprintf("SHOW TABLES IN %s", schema))
      collect(tables)$tableName
    },

    get_table_metadata = function(schema) {
      tables <- self$list_tables(schema)
      metadata <- lapply(tables, function(table) {
        df <- spark_sql(self$connection$spark, sprintf("DESCRIBE TABLE %s.%s", schema, table))
        cols <- collect(df)
        cols <- cols[!grepl("^#", cols$col_name), ]
        list(
          columns = setNames(as.list(cols$data_type), cols$col_name),
          primary_keys = strsplit(self$table_info$keys, ",")[[1]]
        )
      })
      setNames(metadata, tables)
    },

    create_table = function(table_name, schema, schema_name) {
      columns <- sapply(names(schema), function(col) {
        type <- switch(schema[[col]],
                       integer = "INTEGER",
                       character = "STRING",
                       numeric = "FLOAT",
                       stop(sprintf("Unsupported column type: %s", schema[[col]])))
        sprintf("%s %s", col, type)
      })
      query <- sprintf("CREATE TABLE %s.%s (%s) USING DELTA", schema_name, table_name, paste(columns, collapse = ", "))
      spark_sql(self$connection$spark, query)
    }
  )
)