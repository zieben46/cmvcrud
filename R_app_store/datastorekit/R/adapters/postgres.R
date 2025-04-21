library(R6)
library(DBI)
library(data.table)

PostgresAdapter <- R6Class(
  "PostgresAdapter",
  inherit = DatastoreAdapter,
  public = list(
    connection = NULL,

    initialize = function(profile) {
      super$initialize(profile)
      self$connection <- DatastoreConnection$new(profile)
    },

    validate_keys = function(table_name, table_info_keys) {
      tryCatch({
        query <- sprintf("SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '%s' AND table_schema = '%s'",
                         table_name, self$profile$schema)
        db_keys <- DBI::dbGetQuery(self$connection$conn, query)$column_name
        if (length(db_keys) == 0) return()
        if (!setequal(db_keys, table_info_keys)) {
          stop(sprintf("Table %s primary keys %s do not match TableInfo keys %s",
                       table_name, paste(db_keys, collapse = ","), paste(table_info_keys, collapse = ",")))
        }
      }, error = function(e) {
        stop(sprintf("Failed to validate keys for table %s: %s", table_name, e$message))
      })
    },

    insert = function(table_name, data) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      dt <- as.data.table(data)
      DBI::dbWriteTable(self$connection$conn, c(self$profile$schema, table_name), dt, append = TRUE)
    },

    select = function(table_name, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      query <- sprintf("SELECT * FROM %s.%s", self$profile$schema, table_name)
      if (!is.null(filters) && length(filters) > 0) {
        conditions <- sprintf("%s = '%s'", names(filters), unlist(filters))
        query <- sprintf("%s WHERE %s", query, paste(conditions, collapse = " AND "))
      }
      as.list(DBI::dbGetQuery(self$connection$conn, query))
    },

    update = function(table_name, data, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      for (update_data in data) {
        set_clause <- sprintf("%s = '%s'", names(update_data), unlist(update_data))
        where_clause <- sprintf("%s = '%s'", names(filters), unlist(filters))
        query <- sprintf("UPDATE %s.%s SET %s WHERE %s",
                         self$profile$schema, table_name,
                         paste(set_clause, collapse = ", "),
                         paste(where_clause, collapse = " AND "))
        DBI::dbExecute(self$connection$conn, query)
      }
    },

    delete = function(table_name, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      where_clause <- sprintf("%s = '%s'", names(filters), unlist(filters))
      query <- sprintf("DELETE FROM %s.%s WHERE %s",
                       self$profile$schema, table_name,
                       paste(where_clause, collapse = " AND "))
      DBI::dbExecute(self$connection$conn, query)
    },

    list_tables = function(schema) {
      query <- sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema)
      DBI::dbGetQuery(self$connection$conn, query)$table_name
    },

    get_table_metadata = function(schema) {
      tables <- self$list_tables(schema)
      metadata <- lapply(tables, function(table) {
        query <- sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'",
                         table, schema)
        cols <- DBI::dbGetQuery(self$connection$conn, query)
        pk_query <- sprintf("SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '%s' AND table_schema = '%s'",
                            table, schema)
        pks <- DBI::dbGetQuery(self$connection$conn, pk_query)$column_name
        list(
          columns = setNames(as.list(cols$data_type), cols$column_name),
          primary_keys = pks
        )
      })
      setNames(metadata, tables)
    },

    create_table = function(table_name, schema, schema_name) {
      columns <- sapply(names(schema), function(col) {
        type <- switch(schema[[col]],
                       integer = "INTEGER",
                       character = "VARCHAR",
                       numeric = "FLOAT",
                       stop(sprintf("Unsupported column type: %s", schema[[col]])))
        if (col == "unique_id") {
          sprintf("%s %s PRIMARY KEY", col, type)
        } else {
          sprintf("%s %s", col, type)
        }
      })
      query <- sprintf("CREATE TABLE %s.%s (%s)", schema_name, table_name, paste(columns, collapse = ", "))
      DBI::dbExecute(self$connection$conn, query)
    }
  )
)