# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of FeatureExtractionForExtensions
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

#' Get covariate information from extension tables
#'
#' @description
#' Constructs covariates using custom extension tables in the OMOP CDM database.
#' Extension tables are custom tables that extend the standard CDM schema with
#' additional data elements not part of the core specification.
#'
#' @param connection A connection to the server containing the schema as created
#'   using the \code{connect} function in the \code{DatabaseConnector} package.
#' @param oracleTempSchema DEPRECATED. Use tempEmulationSchema instead.
#' @param tempEmulationSchema Schema for emulating temp tables on Oracle, Impala,
#'   and similar platforms.
#' @param cdmDatabaseSchema The name of the database schema that contains the OMOP
#'   CDM instance. On SQL Server, this will specify both the database and the schema,
#'   so for example 'cdm_instance.dbo'.
#' @param cdmVersion Defines the OMOP CDM version used: currently supports "5".
#' @param cohortTable Name of the table holding the cohort for which we want to
#'   construct covariates. This is a fully specified name, so either the name of a
#'   temp table (e.g. '#cohort_table'), or a permanent table including its database
#'   schema (e.g. 'cdm_schema.dbo.cohort').
#' @param cohortId DEPRECATED. Use cohortIds instead.
#' @param cohortIds The cohort definition IDs of the cohort. If set to -1, use all
#'   entries in the cohort table.
#' @param rowIdField The name of the field in the cohort temp table that is to be
#'   used as the row_id field in the output table. This can be especially useful if
#'   there is more than one period per person.
#' @param covariateSettings An object of type \code{covariateSettings} as created
#'   using the \code{\link{createExtensionCovariateSettings}} function.
#' @param aggregated Should covariates be constructed per-person, or aggregated
#'   across the cohort?
#' @param minCharacterizationMean The minimum mean value for characterization output.
#'   Values below this will be cut off from output. This will help reduce the file
#'   size of the characterization output, but will remove information on covariates
#'   that have very low values. The default is 0.
#'
#' @return
#' An object of type \code{CovariateData} from the FeatureExtraction package.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- DatabaseConnector::createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/ohdsi",
#'   user = "joe",
#'   password = "secret"
#' )
#' connection <- DatabaseConnector::connect(connectionDetails)
#'
#' covariateSettings <- createExtensionCovariateSettings(
#'   analysisId = 998,
#'   extensionDatabaseSchema = "my_extensions",
#'   extensionTableName = "patient_biomarkers",
#'   extensionFields = c("biomarker_id", "biomarker_value"),
#'   joinField = "person_id",
#'   covariateIdField = "biomarker_id",
#'   covariateValueField = "biomarker_value",
#'   covariateNameField = "biomarker_name"
#' )
#'
#' covariateData <- getDbExtensionCovariateData(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm",
#'   cohortTable = "#cohort",
#'   cohortIds = 1,
#'   covariateSettings = covariateSettings
#' )
#'
#' DatabaseConnector::disconnect(connection)
#' }
#'
#' @export
getDbExtensionCovariateData <- function(connection,
                                        oracleTempSchema = NULL,
                                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                        cdmDatabaseSchema,
                                        cdmVersion = "5",
                                        cohortTable = "#cohort_person",
                                        cohortId = -1,
                                        cohortIds = c(-1),
                                        rowIdField = "subject_id",
                                        covariateSettings,
                                        aggregated = FALSE,
                                        minCharacterizationMean = 0) {

  # Input validation
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connection, "DatabaseConnectorConnection", add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(cohortTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(rowIdField, len = 1, add = errorMessages)
  checkmate::assertClass(covariateSettings, "covariateSettings", add = errorMessages)
  checkmate::assertLogical(aggregated, len = 1, add = errorMessages)
  checkmate::assertNumeric(minCharacterizationMean, lower = 0, upper = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  # Handle deprecated parameters
  if (!missing(cohortId)) {
    warning("cohortId argument has been deprecated, please use cohortIds")
    cohortIds <- cohortId
  }
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    rlang::warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.",
      .frequency = "regularly",
      .frequency_id = "oracleTempSchema"
    )
    tempEmulationSchema <- oracleTempSchema
  }

  if (cdmVersion == "4") {
    stop("Common Data Model version 4 is not supported")
  }

  if (aggregated) {
    stop("Aggregation not currently supported for extension table covariates")
  }

  start <- Sys.time()
  writeLines("Constructing covariates from extension tables")

  # Build the SQL query to extract covariates from extension table
  sql <- buildExtensionCovariateQuery(
    cohortTable = cohortTable,
    rowIdField = rowIdField,
    cohortIds = cohortIds,
    covariateSettings = covariateSettings
  )

  # Render and translate SQL
  sql <- SqlRender::render(sql,
    cohort_table = cohortTable,
    row_id_field = rowIdField,
    cohort_ids = cohortIds,
    extension_database_schema = covariateSettings$extensionDatabaseSchema,
    extension_table = covariateSettings$extensionTableName,
    join_field = covariateSettings$joinField,
    covariate_id_field = covariateSettings$covariateIdField,
    covariate_value_field = covariateSettings$covariateValueField
  )

  sql <- SqlRender::translate(sql,
    targetDialect = attr(connection, "dbms"),
    tempEmulationSchema = tempEmulationSchema
  )

  # Execute query to get covariates
  covariates <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)

  # Build covariate reference table
  covariateRef <- buildCovariateReference(
    connection = connection,
    tempEmulationSchema = tempEmulationSchema,
    covariateSettings = covariateSettings
  )

  # Build analysis reference
  analysisRef <- data.frame(
    analysisId = covariateSettings$analysisId,
    analysisName = paste("Extension table:", covariateSettings$extensionTableName),
    domainId = "Extension",
    startDay = ifelse(is.null(covariateSettings$startDay), NA, covariateSettings$startDay),
    endDay = ifelse(is.null(covariateSettings$endDay), NA, covariateSettings$endDay),
    isBinary = ifelse(covariateSettings$isBinary, "Y", "N"),
    missingMeansZero = ifelse(covariateSettings$missingMeansZero, "Y", "N")
  )

  # Create metadata
  metaData <- list(
    sql = sql,
    call = match.call(),
    extensionTable = covariateSettings$extensionTableName
  )

  # Create empty CovariateData object with correct structure
  result <- FeatureExtraction::createEmptyCovariateData(
    cohortIds = cohortIds,
    aggregated = aggregated,
    temporal = FALSE
  )

  # Populate with actual data
  result$covariates <- covariates
  result$covariateRef <- covariateRef
  result$analysisRef <- analysisRef
  attr(result, "metaData") <- metaData

  delta <- Sys.time() - start
  writeLines(paste("Loading took", signif(delta, 3), attr(delta, "units")))

  return(result)
}

#' Create extension table covariate settings
#'
#' @description
#' Creates an object specifying how to construct covariates from custom extension
#' tables. Extension tables are custom tables that extend the standard OMOP CDM
#' schema with additional data elements.
#'
#' @param analysisId A unique identifier for this analysis (integer between 1-999).
#' @param extensionDatabaseSchema The database schema where the extension table
#'   can be found.
#' @param extensionTableName The name of the extension table.
#' @param extensionFields A character vector of field names from the extension
#'   table to include in the analysis.
#' @param joinField The field in the extension table that links to the cohort
#'   (typically 'person_id' or 'subject_id').
#' @param covariateIdField The field in the extension table that contains the
#'   covariate identifier.
#' @param covariateValueField The field in the extension table that contains the
#'   covariate value.
#' @param covariateNameField (Optional) The field in a reference table that contains
#'   the covariate names. If NULL, names will be generated automatically.
#' @param covariateRefTable (Optional) The name of a reference table containing
#'   covariate definitions (similar to CONCEPT table in CDM).
#' @param startDay (Optional) The start day relative to index date for temporal
#'   filtering. If NULL, no temporal filtering is applied.
#' @param endDay (Optional) The end day relative to index date for temporal
#'   filtering. If NULL, no temporal filtering is applied.
#' @param dateField (Optional) The date field in the extension table to use for
#'   temporal filtering.
#' @param isBinary Are these binary covariates (values should only be 0 or 1)?
#' @param missingMeansZero Should missing values be interpreted as 0?
#' @param warnOnAnalysisIdOverlap Warn if the provided analysisId overlaps with
#'   any predefined analysis from FeatureExtraction.
#'
#' @return
#' An object of type \code{covariateSettings}, to be used in other functions.
#'
#' @examples
#' \dontrun{
#' # Example 1: Simple biomarker table
#' covariateSettings <- createExtensionCovariateSettings(
#'   analysisId = 998,
#'   extensionDatabaseSchema = "my_extensions",
#'   extensionTableName = "patient_biomarkers",
#'   extensionFields = c("biomarker_id", "biomarker_value"),
#'   joinField = "person_id",
#'   covariateIdField = "biomarker_id",
#'   covariateValueField = "biomarker_value",
#'   isBinary = FALSE,
#'   missingMeansZero = FALSE
#' )
#'
#' # Example 2: Temporal extension table with date filtering
#' covariateSettings <- createExtensionCovariateSettings(
#'   analysisId = 997,
#'   extensionDatabaseSchema = "my_extensions",
#'   extensionTableName = "patient_scores",
#'   extensionFields = c("score_id", "score_value", "score_date"),
#'   joinField = "person_id",
#'   covariateIdField = "score_id",
#'   covariateValueField = "score_value",
#'   dateField = "score_date",
#'   startDay = -365,
#'   endDay = 0,
#'   isBinary = FALSE,
#'   missingMeansZero = TRUE
#' )
#' }
#'
#' @export
createExtensionCovariateSettings <- function(analysisId,
                                              extensionDatabaseSchema,
                                              extensionTableName,
                                              extensionFields,
                                              joinField = "person_id",
                                              covariateIdField,
                                              covariateValueField,
                                              covariateNameField = NULL,
                                              covariateRefTable = NULL,
                                              startDay = NULL,
                                              endDay = NULL,
                                              dateField = NULL,
                                              isBinary = FALSE,
                                              missingMeansZero = FALSE,
                                              warnOnAnalysisIdOverlap = TRUE) {

  # Input validation
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertInt(analysisId, lower = 1, upper = 999, add = errorMessages)
  checkmate::assertCharacter(extensionDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(extensionTableName, len = 1, add = errorMessages)
  checkmate::assertCharacter(extensionFields, min.len = 1, add = errorMessages)
  checkmate::assertCharacter(joinField, len = 1, add = errorMessages)
  checkmate::assertCharacter(covariateIdField, len = 1, add = errorMessages)
  checkmate::assertCharacter(covariateValueField, len = 1, add = errorMessages)
  checkmate::assertCharacter(covariateNameField, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(covariateRefTable, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(startDay, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(endDay, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(dateField, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertLogical(isBinary, len = 1, add = errorMessages)
  checkmate::assertLogical(missingMeansZero, len = 1, add = errorMessages)
  checkmate::assertLogical(warnOnAnalysisIdOverlap, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  # Check that temporal parameters are consistent
  if (!is.null(startDay) || !is.null(endDay)) {
    if (is.null(dateField)) {
      stop("dateField must be specified when using startDay or endDay")
    }
    if (!is.null(startDay) && !is.null(endDay) && startDay > endDay) {
      stop("startDay must be <= endDay")
    }
  }

  # Warn if analysis ID overlaps with FeatureExtraction predefined analyses
  if (warnOnAnalysisIdOverlap) {
    tryCatch({
      csvFile <- system.file("csv", "PrespecAnalyses.csv", package = "FeatureExtraction")
      if (file.exists(csvFile)) {
        preSpecAnalysis <- read.csv(csvFile)
        if (analysisId %in% preSpecAnalysis$analysisId) {
          warning(sprintf(
            "Analysis ID %d also used for prespecified analysis '%s' in FeatureExtraction.",
            analysisId,
            preSpecAnalysis$analysisName[preSpecAnalysis$analysisId == analysisId][1]
          ))
        }
      }
    }, error = function(e) {
      # Silently continue if we can't check
    })
  }

  # Build the settings object
  covariateSettings <- list()
  for (name in names(formals(createExtensionCovariateSettings))) {
    covariateSettings[[name]] <- get(name)
  }

  # Set the function attribute
  attr(covariateSettings, "fun") <- "getDbExtensionCovariateData"
  class(covariateSettings) <- "covariateSettings"

  return(covariateSettings)
}

# Helper function to build the SQL query for extracting covariates
buildExtensionCovariateQuery <- function(cohortTable,
                                         rowIdField,
                                         cohortIds,
                                         covariateSettings) {

  # Start building the SELECT clause
  selectClause <- paste(
    "SELECT",
    "@row_id_field AS row_id,",
    "@covariate_id_field AS covariate_id,",
    "@covariate_value_field AS covariate_value"
  )

  # Build FROM clause
  fromClause <- paste(
    "FROM @cohort_table c",
    "INNER JOIN @extension_database_schema.@extension_table ext",
    paste0("ON c.subject_id = ext.@join_field")
  )

  # Build WHERE clause
  whereConditions <- c()

  # Add cohort ID filter if specified
  if (!(-1 %in% cohortIds)) {
    whereConditions <- c(whereConditions, "c.cohort_definition_id IN (@cohort_ids)")
  }

  # Add temporal filtering if specified
  if (!is.null(covariateSettings$startDay) && !is.null(covariateSettings$endDay)) {
    whereConditions <- c(
      whereConditions,
      sprintf(
        "ext.%s >= DATEADD(day, %d, c.cohort_start_date)",
        covariateSettings$dateField,
        covariateSettings$startDay
      ),
      sprintf(
        "ext.%s <= DATEADD(day, %d, c.cohort_start_date)",
        covariateSettings$dateField,
        covariateSettings$endDay
      )
    )
  }

  # Combine WHERE conditions
  if (length(whereConditions) > 0) {
    whereClause <- paste("WHERE", paste(whereConditions, collapse = " AND "))
  } else {
    whereClause <- ""
  }

  # Combine all parts
  sql <- paste(selectClause, fromClause, whereClause)

  return(sql)
}

# Helper function to build the covariate reference table
buildCovariateReference <- function(connection,
                                    tempEmulationSchema,
                                    covariateSettings) {

  # If a reference table is specified, query it
  if (!is.null(covariateSettings$covariateRefTable) &&
      !is.null(covariateSettings$covariateNameField)) {

    refSql <- paste(
      "SELECT",
      paste0(covariateSettings$covariateIdField, " AS covariate_id,"),
      paste0(covariateSettings$covariateNameField, " AS covariate_name"),
      "FROM",
      paste0(covariateSettings$extensionDatabaseSchema, ".", covariateSettings$covariateRefTable)
    )

    refSql <- SqlRender::translate(refSql,
      targetDialect = attr(connection, "dbms"),
      tempEmulationSchema = tempEmulationSchema
    )

    covariateRef <- DatabaseConnector::querySql(connection, refSql, snakeCaseToCamelCase = TRUE)
  } else {
    # Create a minimal reference table
    covariateRef <- data.frame(
      covariateId = numeric(0),
      covariateName = character(0)
    )
  }

  # Add additional required columns
  if (nrow(covariateRef) > 0) {
    covariateRef$analysisId <- covariateSettings$analysisId
    covariateRef$conceptId <- 0
  } else {
    covariateRef <- data.frame(
      covariateId = numeric(0),
      covariateName = character(0),
      analysisId = numeric(0),
      conceptId = numeric(0)
    )
  }

  return(covariateRef)
}
