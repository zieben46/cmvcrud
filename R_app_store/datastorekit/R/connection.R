library(R6)
library(DBI)
library(sparklyr)

DatastoreConnection <- R6Class(
  "DatastoreConnection",
  public = list(
    profile = NULL,
    conn = NULL,
    spark = NULL,

    initialize = function(profile) {
      self$profile <- profile
      if (profile$db_type == "databricks") {
        self$spark <- private$create_spark_session()
      } else if (profile$db_type == "postgres") {
        self$conn <- private$create_postgres_connection()
      } else if (profile$db_type == "csv") {
        # No connection needed for CSV
      } else {
        stop(sprintf("Unsupported db_type: %s", profile$db_type))
      }
    },

    finalize = function() {
      self$stop()
    },

    stop = function() {
      if (!is.null(self$spark)) {
        spark_disconnect(self$spark)
      }
      if (!is.null(self$conn)) {
        DBI::dbDisconnect(self$conn)
      }
    }
  ),
  private = list(
    create_postgres_connection = function() {
      tryCatch({
        DBI::dbConnect(
          RPostgreSQL::PostgreSQL(),
          host = self$profile$host,
          port = self$profile$port,
          user = self$profile$username,
          password = self$profile$password,
          dbname = self$profile$dbname
        )
      }, error = function(e) {
        stop(sprintf("Failed to create PostgreSQL connection: %s", e$message))
      })
    },

    create_spark_session = function() {
      tryCatch({
        spark_config <- spark_config()
        spark_config$spark.databricks.service.address <- sprintf("https://%s", self$profile$host)
        spark_config$spark.databricks.service.token <- self$profile$token
        spark_config$spark.databricks.service.cluster <- basename(self$profile$http_path)
        spark_config$spark.sql.catalogImplementation <- "hive"
        spark_connect(
          master = "local",
          method = "databricks",
          config = spark_config
        )
      }, error = function(e) {
        stop(sprintf("Failed to create Spark session: %s", e$message))
      })
    }
  )
)