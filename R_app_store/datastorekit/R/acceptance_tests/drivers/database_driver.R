library(R6)

DatabaseDriver <- R6Class(
  "DatabaseDriver",
  public = list(
    create_table = function(table_name, schema) {
      stop("create_table must be implemented by subclass")
    },

    create = function(table_info, data) {
      stop("create must be implemented by subclass")
    },

    read = function(table_info, filters) {
      stop("read must be implemented by subclass")
    },

    update = function(table_info, data, filters) {
      stop("update must be implemented by subclass")
    },

    delete = function(table_info, filters) {
      stop("delete must be implemented by subclass")
    },

    sync_to = function(source_table_info, target_driver, target_table, method) {
      stop("sync_to must be implemented by subclass")
    }
  )
)