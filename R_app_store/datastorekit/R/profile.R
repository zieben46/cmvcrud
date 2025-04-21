library(R6)

DatabaseProfile <- R6Class(
  "DatabaseProfile",
  public = list(
    connection_string = NULL,
    host = NULL,
    port = NULL,
    username = NULL,
    password = NULL,
    dbname = NULL,
    token = NULL,
    http_path = NULL,
    catalog = NULL,
    schema = NULL,
    db_type = NULL,

    initialize = function(connection_string = NULL, host = NULL, port = NULL,
                         username = NULL, password = NULL, dbname = NULL,
                         token = NULL, http_path = NULL, catalog = NULL,
                         schema = NULL, db_type = NULL) {
      self$connection_string <- connection_string
      self$host <- host
      self$port <- port
      self$username <- username
      self$password <- password
      self$dbname <- dbname
      self$token <- token
      self$http_path <- http_path
      self$catalog <- catalog
      self$schema <- schema
      self$db_type <- db_type
    }
  )
)

# Factory functions for profiles
DatabaseProfile_postgres <- function(env_file) {
  env_vars <- read.dcf(env_file)[1, ]
  username <- env_vars[["PG_USERNAME"]]
  password <- env_vars[["PG_PASSWORD"]]
  host <- env_vars[["PG_HOST"]] %||% "localhost"
  port <- env_vars[["PG_PORT"]] %||% "5432"
  dbname <- env_vars[["PG_DBNAME"]]
  if (is.null(username) || is.null(password) || is.null(dbname)) {
    stop(sprintf("PG_USERNAME, PG_PASSWORD, and PG_DBNAME must be provided in %s", env_file))
  }
  conn_str <- sprintf("postgresql://%s:%s@%s:%s/%s", username, password, host, port, dbname)
  DatabaseProfile$new(
    connection_string = conn_str,
    host = host,
    port = port,
    username = username,
    password = password,
    dbname = dbname,
    schema = env_vars[["SCHEMA"]] %||% "public",
    db_type = "postgres"
  )
}

DatabaseProfile_databricks <- function(env_file) {
  env_vars <- read.dcf(env_file)[1, ]
  host <- env_vars[["DATABRICKS_HOST"]]
  token <- env_vars[["DATABRICKS_TOKEN"]]
  http_path <- env_vars[["DATABRICKS_HTTP_PATH"]]
  catalog <- env_vars[["DATABRICKS_CATALOG"]] %||% "hive_metastore"
  schema <- env_vars[["DATABRICKS_SCHEMA"]] %||% "default"
  if (is.null(host) || is.null(token) || is.null(http_path)) {
    stop(sprintf("DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be provided in %s", env_file))
  }
  conn_str <- sprintf("databricks://token:%s@%s?http_path=%s&catalog=%s&schema=%s", token, host, http_path, catalog, schema)
  DatabaseProfile$new(
    connection_string = conn_str,
    host = host,
    token = token,
    http_path = http_path,
    catalog = catalog,
    schema = schema,
    db_type = "databricks"
  )
}

DatabaseProfile_csv <- function(env_file) {
  env_vars <- read.dcf(env_file)[1, ]
  base_dir <- env_vars[["CSV_BASE_DIR"]] %||% "./data"
  dbname <- env_vars[["CSV_DBNAME"]] %||% "csv_db"
  DatabaseProfile$new(
    connection_string = base_dir,
    dbname = dbname,
    schema = "default",
    db_type = "csv"
  )
}

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x