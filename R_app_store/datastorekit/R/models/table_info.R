library(R6)

TableInfo <- R6Class(
  "TableInfo",
  public = list(
    table_name = NULL,
    keys = NULL,
    scd_type = NULL,
    datastore_key = NULL,
    columns = NULL,

    initialize = function(table_name, keys, scd_type, datastore_key, columns = NULL) {
      self$table_name <- table_name
      self$keys <- keys
      self$scd_type <- scd_type
      self$datastore_key <- datastore_key
      self$columns <- columns
    }
  )
)