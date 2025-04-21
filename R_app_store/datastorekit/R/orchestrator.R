library(R6)

AdapterRegistry <- R6Class(
  "AdapterRegistry",
  public = list(
    adapters = list(
      postgres = "PostgresAdapter",
      databricks = "DatabricksAdapter",
      csv = "CSVAdapter"
    ),

    register_adapter = function(db_type, adapter_class) {
      self$adapters[[db_type]] <- adapter_class
    },

    get_adapter_class = function(db_type) {
      self$adapters[[db_type]]
    }
  )
)

DataStoreOrchestrator <- R6Class(
  "DataStoreOrchestrator",
  public = list(
    adapters = list(),
    registry = NULL,

    initialize = function(env_paths = NULL, profiles = NULL) {
      self$registry <- AdapterRegistry$new()
      if (!is.null(env_paths)) {
        for (path in env_paths) {
          profile <- private$load_profile(path)
          private$add_adapter(profile)
        }
      }
      if (!is.null(profiles)) {
        for (profile in profiles) {
          private$add_adapter(profile)
        }
      }
      if (length(self$adapters) == 0) {
        stop("No valid datastore configurations provided")
      }
    },

    list_adapters = function() {
      names(self$adapters)
    },

    list_tables = function(db_name, schema, include_metadata = FALSE) {
      key <- sprintf("%s:%s", db_name, schema %||% "default")
      if (!(key %in% names(self$adapters))) {
        stop(sprintf("No datastore found for key: %s", key))
      }
      if (include_metadata) {
        return(self$adapters[[key]]$get_table_metadata(schema %||% "public"))
      }
      self$adapters[[key]]$list_tables(schema %||% "public")
    },

    get_table = function(db_key, table_info) {
      if (!(db_key %in% names(self$adapters))) {
        stop(sprintf("No datastore found for key: %s", db_key))
      }
      adapter <- self$adapters[[db_key]]
      adapter$table_info <- table_info
      DBTable$new(adapter, table_info)
    },

    replicate = function(source_db, source_schema, source_table,
                        target_db, target_schema, target_table, filters = NULL) {
      tryCatch({
        source_key <- sprintf("%s:%s", source_db, source_schema %||% "default")
        target_key <- sprintf("%s:%s", target_db, target_schema %||% "default")
        source_adapter <- self$adapters[[source_key]]
        target_adapter <- self$adapters[[target_key]]
        if (is.null(source_adapter) || is.null(target_adapter)) {
          stop(sprintf("Source (%s) or target (%s) datastore not found", source_key, target_key))
        }

        source_table_info <- TableInfo$new(
          table_name = source_table,
          keys = "unique_id",
          scd_type = "type1",
          datastore_key = source_key,
          columns = list(unique_id = "integer", category = "character", amount = "numeric")
        )
        target_table_info <- TableInfo$new(
          table_name = target_table,
          keys = "unique_id",
          scd_type = "type1",
          datastore_key = target_key,
          columns = list(unique_id = "integer", category = "character", amount = "numeric")
        )

        source_table <- DBTable$new(source_adapter, source_table_info)
        target_table <- DBTable$new(target_adapter, target_table_info)

        target_tables <- self$list_tables(target_db, target_schema)
        if (!(target_table %in% target_tables)) {
          message(sprintf("Creating target table %s.%s...", target_schema, target_table))
          private$create_target_table(target_adapter, target_db, target_schema, target_table, source_table)
        }

        data <- source_table$read(filters)
        if (length(data) == 0) {
          message(sprintf("No data to replicate from %s.%s", source_schema, source_table))
          return()
        }

        target_table$create(data)
        message(sprintf("Replicated %d records from %s:%s.%s to %s:%s.%s",
                        length(data), source_db, source_schema, source_table,
                        target_db, target_schema, target_table))
      }, error = function(e) {
        message(sprintf("Replication failed: %s", e$message))
        stop(e)
      })
    }
  ),
  private = list(
    load_profile = function(env_path) {
      profile_map <- list(
        postgres = DatabaseProfile_postgres,
        databricks = DatabaseProfile_databricks,
        csv = DatabaseProfile_csv
      )
      env_path_lower <- tolower(env_path)
      for (key in names(profile_map)) {
        if (grepl(key, env_path_lower)) {
          return(profile_map[[key]](env_path))
        }
      }
      stop(sprintf("Unknown datastore type for %s", env_path))
    },

    add_adapter = function(profile) {
      adapter <- private$create_adapter(profile)
      key <- sprintf("%s:%s", profile$dbname, profile$schema %||% "default")
      self$adapters[[key]] <- adapter
      message(sprintf("Initialized datastore: %s", key))
    },

    create_adapter = function(profile) {
      adapter_class_name <- self$registry$get_adapter_class(profile$db_type)
      if (is.null(adapter_class_name)) {
        stop(sprintf("Unsupported db_type: %s", profile$db_type))
      }
      adapter_class <- get(adapter_class_name)
      adapter_class$new(profile)
    },

    create_target_table = function(target_adapter, target_db, target_schema, target_table, source_table) {
      tryCatch({
        sample_data <- source_table$read(NULL)
        schema <- target_table$table_info$columns %||% list(
          unique_id = "integer",
          category = "character",
          amount = "numeric"
        )
        target_adapter$create_table(target_table$table_name, schema, target_schema)
      }, error = function(e) {
        stop(sprintf("Failed to create target table %s.%s: %s",
                     target_schema, target_table$table_name, e$message))
      })
    }
  )
)