library(R6)

DBTable <- R6Class(
  "DBTable",
  public = list(
    adapter = NULL,
    table_info = NULL,
    table_name = NULL,
    keys = NULL,
    scd_type = NULL,

    initialize = function(adapter, table_info) {
      self$adapter <- adapter
      self$table_info <- table_info
      self$table_name <- table_info$table_name
      self$keys <- strsplit(table_info$keys, ",")[[1]]
      self$scd_type <- table_info$scd_type
      self$adapter$table_info <- table_info
    },

    create = function(data) {
      self$adapter$insert(self$table_name, data)
    },

    read = function(filters) {
      self$adapter$select(self$table_name, filters)
    },

    update = function(data, filters) {
      self$adapter$update(self$table_name, data, filters)
    },

    delete = function(filters) {
      self$adapter$delete(self$table_name, filters)
    }
  )
)