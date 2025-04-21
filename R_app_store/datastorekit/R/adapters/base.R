library(R6)

DatastoreAdapter <- R6Class(
  "DatastoreAdapter",
  public = list(
    profile = NULL,
    table_info = NULL,

    initialize = function(profile) {
      self$profile <- profile
    },

    validate_keys = function(table_name, table_info_keys) {
      stop("validate_keys must be implemented by subclass")
    },

    insert = function(table_name, data) {
      stop("insert must be implemented by subclass")
    },

    select = function(table_name, filters) {
      stop("select must be implemented by subclass")
    },

    update = function(table_name, data, filters) {
      stop("update must be implemented by subclass")
    },

    delete = function(table_name, filters) {
      stop("delete must be implemented by subclass")
    },

    list_tables = function(schema) {
      stop("list_tables must be implemented by subclass")
    },

    get_table_metadata = function(schema) {
      stop("get_table_metadata must be implemented by subclass")
    },

    create_table = function(table_name, schema, schema_name) {
      stop("create_table must be implemented by subclass")
    }
  )
)