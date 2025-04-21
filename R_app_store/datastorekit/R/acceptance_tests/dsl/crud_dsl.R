library(R6)
library(data.table)

DatabaseDSL <- R6Class(
  "DatabaseDSL",
  public = list(
    driver = NULL,
    selected_table = NULL,

    initialize = function(driver) {
      self$driver <- driver
    },

    create_table = function(table_name, schema, key = "unique_id") {
      self$selected_table <- list(table_name = table_name, key = key)
      self$driver$create_table(table_name, schema)
      message(sprintf("Created table %s with schema %s", table_name, paste(names(schema), collapse = ", ")))
      self
    },

    select_table = function(table_name, key = "unique_id") {
      self$selected_table <- list(table_name = table_name, key = key)
      self
    },

    setup_data = function(records) {
      if (is.null(self$selected_table)) {
        stop("No table selected")
      }
      self$driver$create(self$selected_table, records)
      message(sprintf("Inserted %d records into %s", length(records), self$selected_table$table_name))
      self
    },

    assert_table_has = function(expected_records) {
      if (is.null(self$selected_table)) {
        stop("No table selected")
      }
      actual_data <- self$driver$read(self$selected_table, list())
      actual_dt <- as.data.table(actual_data)
      expected_dt <- as.data.table(expected_records)
      expect_equal(actual_dt, expected_dt, ignore.col.order = TRUE)
      message(sprintf("Assertion passed for table %s", self$selected_table$table_name))
      self
    },

    assert_record_exists = function(filters, expected_data) {
      if (is.null(self$selected_table)) {
        stop("No table selected")
      }
      data <- self$driver$read(self$selected_table, filters)
      expect_length(data, 1)
      for (key in names(expected_data)) {
        expect_equal(data[[1]][[key]], expected_data[[key]], info = sprintf("Mismatch in %s", key))
      }
      self
    }
  )
)

CrudDSL <- R6Class(
  "CrudDSL",
  inherit = DatabaseDSL,
  public = list(
    execute_crud = function(operation, data, filters = NULL) {
      if (is.null(self$selected_table)) {
        stop("No table selected")
      }
      if (operation == "CREATE") {
        self$driver$create(self$selected_table, data)
      } else if (operation == "READ") {
        return(self$driver$read(self$selected_table, filters))
      } else if (operation == "UPDATE") {
        self$driver$update(self$selected_table, data, filters)
      } else if (operation == "DELETE") {
        self$driver$delete(self$selected_table, filters)
      } else {
        stop(sprintf("Unknown operation: %s", operation))
      }
      self
    }
  )
)