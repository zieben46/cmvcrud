library(R6)
library(data.table)

SyncDSL <- R6Class(
  "SyncDSL",
  inherit = DatabaseDSL,
  public = list(
    target_driver = NULL,
    target_table = NULL,

    initialize = function(source_driver, target_driver) {
      super$initialize(source_driver)
      self$target_driver <- target_driver
    },

    sync_to_target = function(target_table, sync_method = "full_load") {
      if (is.null(self$selected_table)) {
        stop("No source table selected")
      }
      self$target_table <- list(table_name = target_table, key = self$selected_table$key)
      self$driver$sync_to(self$selected_table, self$target_driver, target_table, sync_method)
      self
    },

    assert_tables_synced = function() {
      if (is.null(self$selected_table) || is.null(self$target_table)) {
        stop("Tables not selected")
      }
      source_data <- self$driver$read(self$selected_table, list())
      target_data <- self$target_driver$read(self$target_table, list())
      source_dt <- as.data.table(source_data)
      target_dt <- as.data.table(target_data)
      expect_equal(source_dt, target_dt, ignore.col.order = TRUE)
      self
    }
  )
)