library(R6)
library(data.table)

CSVAdapter <- R6Class(
  "CSVAdapter",
  inherit = DatastoreAdapter,
  public = list(
    base_dir = NULL,

    initialize = function(profile) {
      super$initialize(profile)
      self$base_dir <- profile$connection_string
      dir.create(self$base_dir, showWarnings = FALSE, recursive = TRUE)
    },

    get_file_path = function(table_name) {
      file.path(self$base_dir, sprintf("%s.csv", table_name))
    },

    validate_keys = function(table_name, table_info_keys) {
      file_path <- self$get_file_path(table_name)
      if (!file.exists(file_path)) return()
      dt <- fread(file_path, nrows = 1)
      columns <- colnames(dt)
      if (!all(table_info_keys %in% columns)) {
        stop(sprintf("Table %s CSV columns %s do not include all TableInfo keys %s",
                     table_name, paste(columns, collapse = ","), paste(table_info_keys, collapse = ",")))
      }
    },

    insert = function(table_name, data) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      file_path <- self$get_file_path(table_name)
      dt <- as.data.table(data)
      if (file.exists(file_path)) {
        existing_dt <- fread(file_path)
        dt <- rbind(existing_dt, dt)
      }
      fwrite(dt, file_path)
    },

    select = function(table_name, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      file_path <- self$get_file_path(table_name)
      if (!file.exists(file_path)) return(list())
      dt <- fread(file_path)
      if (!is.null(filters) && length(filters) > 0) {
        for (name in names(filters)) {
          dt <- dt[get(name) == filters[[name]]]
        }
      }
      as.list(dt)
    },

    update = function(table_name, data, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      file_path <- self$get_file_path(table_name)
      if (!file.exists(file_path)) return()
      dt <- fread(file_path)
      mask <- rep(TRUE, nrow(dt))
      for (name in names(filters)) {
        mask <- mask & dt[[name]] == filters[[name]]
      }
      for (update_data in data) {
        for (name in names(update_data)) {
          dt[mask, (name) := update_data[[name]]]
        }
      }
      fwrite(dt, file_path)
    },

    delete = function(table_name, filters) {
      self$validate_keys(table_name, strsplit(self$table_info$keys, ",")[[1]])
      file_path <- self$get_file_path(table_name)
      if (!file.exists(file_path)) return()
      dt <- fread(file_path)
      mask <- rep(TRUE, nrow(dt))
      for (name in names(filters)) {
        mask <- mask & dt[[name]] == filters[[name]]
      }
      dt <- dt[!mask]
      fwrite(dt, file_path)
    },

    list_tables = function(schema) {
      files <- list.files(self$base_dir, pattern = "\\.csv$", full.names = FALSE)
      sub("\\.csv$", "", files)
    },

    get_table_metadata = function(schema) {
      tables <- self$list_tables(schema)
      metadata <- lapply(tables, function(table) {
        file_path <- self$get_file_path(table)
        if (file.exists(file_path)) {
          dt <- fread(file_path, nrows = 1)
          cols <- sapply(dt, function(x) class(x)[1])
          list(
            columns = as.list(cols),
            primary_keys = strsplit(self$table_info$keys, ",")[[1]]
          )
        } else {
          list(columns = list(), primary_keys = list())
        }
      })
      setNames(metadata, tables)
    },

    create_table = function(table_name, schema, schema_name) {
      file_path <- self$get_file_path(table_name)
      dt <- as.data.table(setNames(lapply(schema, function(x) vector(x, 0)), names(schema)))
      fwrite(dt, file_path)
    }
  )
)