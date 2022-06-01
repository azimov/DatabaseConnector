# @file ConnectionManager.R
#
# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of DatabaseConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Connection Handler
#' @description
#' R6 Class for handling database connections and querying databases in transparent way.
#' Deisgned to allow implementation of subclasses (See `PooledConnectionHandler`) in a way that is transparent so
#' implementations can act in common way.
#' This functionality is intended to be used in situtations like shiny apps and R plumber APIs where abstraction
#' is required to ensure that connections are used in long running processes and can be more easily managed.
#'
#' @field connectionDetails         A DatabaseConnector::connectionDetails instance that can be used to connect to a db
#' @field tempEmulationSchema       (Optional) Charachter - used by databases (e.g. oracle) that have require a real
#'                                  schema to emulate temporary tables.
#' @field isActive                  Boolean status of connection - active or not
#'
#' @field useCamelCaseColNames      Boolean - if returned results should, by default, use camel case column names
#'
#' @import R6
#' @importFrom DBI dbIsValid
#' @importFrom SqlRender render translate
#' @export
ConnectionHandler <- R6::R6Class(
  "ConnectionHandler",
  private = list(
    con = NULL,
    #' @description
    #' Base underlying query functionality
    #' Override this in subclass to implement different query method
    queryFunction = function(sql) {
      querySql(private$con, sql, snakeCaseToCamelCase = self$snakeCaseToCamelCase)
    }
  ),
  public = list(
    connectionDetails = NULL,
    tempEmulationSchema = NULL,
    isActive = FALSE,
    snakeCaseToCamelCase = TRUE,
    #' @description
    #' initialize connection hander, including initalizing database connection
    #' @param connectionDetails             A DatabaseConnector::connectionDetails instance that can be used to connect to a db
    #' @param tempEmulationSchema           (Optional) Charachter - used by databases (e.g. oracle) that have require a real schema to emulate temporary tables.
    initialize = function(connectionDetails,
                          tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                          snakeCaseToCamelCase = TRUE) {
      stopifnot("connectionDetails" %in% class(connectionDetails))
      self$connectionDetails <- connectionDetails
      self$tempEmulationSchema <- tempEmulationSchema
      self$snakeCaseToCamelCase <- snakeCaseToCamelCase
      self$initConnection()
    },

    #' @description
    #' initialize connection (if it isn't already active). In the case of an active connection, closes and re-initializes
    initConnection = function() {
      if (self$isActive) {
        warning("Closing existing connection")
        self$closeConnection()
      }
      private$con <- connect(connectionDetails = self$connectionDetails)
      self$isActive <- TRUE
    },

    #' @description
    #' Return the DatabaseConnector connection object
    getConnection = function() {
      return(private$con)
    },

    #' @description
    #' Disconnect from db
    closeConnection = function() {
      if (DBI::dbIsValid(dbObj = private$con)) {
        disconnect(private$con)
      }
      self$isActive <- FALSE
    },

    #' @description
    #' Makes sure db connection exits cleanly
    finalize = function() {
      if (self$isActive & DBI::dbIsValid(dbObj = private$con)) {
        self$closeConnection()
      }
    },

    #' @description
    #' Public method for querying database returns a data.frame of results
    queryDb = function(sql, ...) {
      sql <- self$renderTranslateSql(sql, ...)
      tryCatch(
        {
          data <- private$queryFunction(sql)
        },
        error = function(error) {
          #' Handles issue where transactions need to be aborted on certain RDBMSes
          if (self$connectionDetails$dbms %in% c("postgresql", "redshift")) {
            dbExecute(private$con, "ABORT;")
          }
          stop(paste(error, sql, sep = "\n"))
        }
      )

      return(data)
    },

    #' @description
    #' Call out to SqlRender to render and translate sql (without executing)
    #' @param sql        query string
    #' @param ...        additional query parameters
    renderTranslateSql = function(sql, ...) {
      sql <- SqlRender::render(sql = sql, ...)
      SqlRender::translate(sql,
                           targetDialect = self$connectionDetails$dbms,
                           tempEmulationSchema = self$tempEmulationSchema)
    },

    #' @description
    #' Query with sql file
    #' @param sqlFilename               Path to file
    #' @param packageName               If not null, will use SqlRender to load file from package,
    #'                                  Otherwise file will be loaded directly
    #' @param snakeCaseToCamelCase      convert result headers to camel case or not
    queryDbFile = function(sqlFilename, packageName = NULL, ...) {

      if (!is.null(packageName)) {
        sql <- SqlRender::loadRenderTranslateSql(sqlFilename = sqlFilename,
                                                 packageName = packageName,
                                                 dbms = private$connectionDetails$dbms,
                                                 tempEmulationSchema = self$tempEmulationSchema,
                                                 ...)
      } else {
        stopifnot(file.exists(sqlFilename))
        sql <- SqlRender::readSql(sqlFilename)
        self$renderTranslateSql(sql, ...)
      }
      private$queryFunction(sql)
    }
  )
)

#' Pooled Connection Handler
#' @description
#' R6 Class for handling pooled database connections and querying databases in transparent way.
#' Subclasses (See `ConnectionHandler`) for standard implementation.
#'
#' This functionality is intended to be used in situtations like shiny apps and R plumber APIs where abstraction
#' is required to ensure that connections are used in long running processes and can be more easily managed.
#' Pooled connections are required in
#' Uses the pool library.
PooledConnectionHandler <- R6::R6Class(
  "PooledConnectionHandler",
  inherit = ConnectionHandler,
  private = list(
    #' Underlying query function
    #' @inheritParams  SqlRender::loadRenderTranslateSql
    queryFunction = function(sql) {
      data <- dbGetQuery(private$con, sql)
      if (self$useCamelCaseColNames) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      } else {
        colnames(data) <- toupper(colnames(data))
      }
      return(data)
    }
  ),
  public = list(
    initConnection = function() {
      ensure_installed("pool")
      if (self$isActive) {
        warning("Closing existing connection")
        self$closeConnection()
      }

      private$con <- pool::dbPool(
        drv = DatabaseConnectorDriver(),
        dbms = self$connectionDetails$dbms,
        server = self$connectionDetails$server(),
        port = self$connectionDetails$port(),
        user = self$connectionDetails$user(),
        password = self$connectionDetails$password()
      )
      self$isActive <- TRUE
    },

    #' @description
    #' Disconnect from db
    closeConnection = function() {
      if (DBI::dbIsValid(dbObj = private$con)) {
        pool::poolClose(pool = private$con)
      }
      self$isActive <- FALSE
    }
  )
)