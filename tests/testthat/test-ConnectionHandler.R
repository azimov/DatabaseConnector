# Implementations of database connection should function in the same way
genericTests <- function(HanderClass, classes, connectionClass, connectionDetails, cdmDatabaseSchema) {
  connectionHandler <- HanderClass$new(connectionDetails)
  for (cVal in classes) {
    expect_true(cVal %in% class(connectionHandler))
  }

  on.exit(
    {
      connectionHandler$finalize()
    },
    add = TRUE
  )

  expect_true("ConnectionHandler" %in% class(connectionHandler))
  expect_true(connectionHandler$isActive)
  expect_true(DBI::dbIsValid(dbObj = connectionHandler$getConnection()))

  data <- connectionHandler$queryDb("SELECT count(*) AS cnt_test FROM @cdm_database_schema.concept;",
                                    cdm_database_schema = cdmDatabaseSchema)

  expect_true(is.data.frame(data))
  connectionHandler$snakeCaseToCamelCase <- FALSE
  data2 <- connectionHandler$queryDb("SELECT count(*) AS cnt_test FROM @cdm_database_schema.concept;",
                                     cdm_database_schema = cdmDatabaseSchema)

  expect_true(is.data.frame(data2))

  expect_error(connectionHandler$queryDb("SELECT 1 * WHERE;"))

  connectionHandler$closeConnection()
  expect_false(connectionHandler$isActive)
  expect_false(DBI::dbIsValid(dbObj = connectionHandler$con))
  connectionHandler$initConnection()
  expect_true(connectionHandler$isActive)

  expect_warning(connectionHandler$initConnection(), "Closing existing connection")

  connectionHandler$closeConnection()
}

connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
)
cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

test_that("Database Connector Class works", {
  genericTests(ConnectionHandler, c("ConnectionHandler"), "DatabaseConnectorDbiConnection", connectionDetails, cdmDatabaseSchema)
})

test_that("Pooled connector Class works", {
  genericTests(PooledConnectionHandler, c("PooledConnectionHandler", "ConnectionHandler"), "Pool", connectionDetails, cdmDatabaseSchema)
})
