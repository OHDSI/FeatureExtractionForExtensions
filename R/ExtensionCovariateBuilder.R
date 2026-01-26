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
#' covariateData <- getDbExtCovariateData(
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
getDbExtCovariateData <- function(connection,
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

  # Query population size from cohort table (needed for PLP's covariate tidying)
  popSql <- "SELECT cohort_definition_id, COUNT_BIG(*) AS population_size
             FROM @cohort_table
             {@cohort_ids != -1} ? {WHERE cohort_definition_id IN (@cohort_ids)}
             GROUP BY cohort_definition_id;"
  popSql <- SqlRender::render(
    sql = popSql,
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  popSql <- SqlRender::translate(
    sql = popSql,
    targetDialect = attr(connection, "dbms"),
    tempEmulationSchema = tempEmulationSchema
  )
  popTemp <- DatabaseConnector::querySql(connection, popSql, snakeCaseToCamelCase = TRUE)

  if (aggregated) {
    populationSize <- popTemp$populationSize
    names(populationSize) <- popTemp$cohortDefinitionId
  } else {
    populationSize <- sum(popTemp$populationSize)
  }

  writeLines(paste("Population size:", populationSize))

  # Build the SQL query to extract covariates from extension table
  sql <- buildExtensionCovariateQuery(
    cohortTable = cohortTable,
    rowIdField = rowIdField,
    cohortIds = cohortIds,
    covariateSettings = covariateSettings
  )

  # Render and translate SQL
  renderParams <- list(
    cohort_table = cohortTable,
    row_id_field = rowIdField,
    cohort_ids = cohortIds,
    extension_database_schema = covariateSettings$extensionDatabaseSchema,
    extension_table = covariateSettings$extensionTableName,
    join_field = covariateSettings$joinField,
    covariate_id_field = covariateSettings$covariateIdField,
    covariate_value_field = covariateSettings$covariateValueField
  )

  # Add parent table parameters if specified
  if (!is.null(covariateSettings$parentTable)) {
    renderParams$parent_table <- covariateSettings$parentTable
    renderParams$parent_join_field <- covariateSettings$parentJoinField
  }

  sql <- do.call(SqlRender::render, c(list(sql = sql), renderParams))

  sql <- SqlRender::translate(sql,
    targetDialect = attr(connection, "dbms"),
    tempEmulationSchema = tempEmulationSchema
  )

  # Debug: Optionally save SQL for inspection
  if (getOption("FeatureExtractionForExtensions.saveSql", FALSE)) {
    sqlFile <- file.path(tempdir(), "extensionCovariateQuery.sql")
    writeLines(sql, sqlFile)
    writeLines(paste("SQL saved to:", sqlFile))
  }

  # Execute query to get covariates
  covariates <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)

  # Debug: Report covariate extraction results
  writeLines(paste("Extension covariate query returned", nrow(covariates), "rows"))
  if (nrow(covariates) > 0) {
    writeLines(paste("  Unique rowIds:", length(unique(covariates$rowId))))
    writeLines(paste("  Unique covariateIds:", paste(unique(covariates$covariateId), collapse=", ")))
  } else {
    writeLines("  WARNING: No covariate data extracted from extension table!")
    writeLines("  Check that:")
    writeLines("    1. Extension table has data matching the cohort")
    writeLines("    2. Temporal filters (startDay/endDay) are not too restrictive")
    writeLines("    3. Join conditions are correct")
  }

  # Ensure covariates data frame has the required structure for FeatureExtraction
  # Standard FeatureExtraction expects: rowId, covariateId, covariateValue
  # Verify column names match expected structure
  if (!all(c("rowId", "covariateId", "covariateValue") %in% names(covariates))) {
    stop("Extension covariate query must return columns: rowId, covariateId, covariateValue")
  }

  # The Andromeda schema created by createEmptyCovariateData() may include additional columns
  # that standard FeatureExtraction uses. We need to provide ALL expected columns.
  if (nrow(covariates) > 0) {
    covariates$rowId <- as.integer(covariates$rowId)
    covariates$covariateId <- as.numeric(covariates$covariateId)
    covariates$covariateValue <- as.numeric(covariates$covariateValue)
  } else {
    # Create empty data frame with all required columns
    covariates <- data.frame(
      rowId = integer(0),
      covariateId = numeric(0),
      covariateValue = numeric(0)
    )
  }

  # Select columns in the correct order for Andromeda compatibility
  covariates <- covariates[, c("rowId", "covariateId", "covariateValue")]

  # Build covariate reference table
  covariateRef <- buildCovariateReference(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    covariateSettings = covariateSettings
  )

  # Build analysis reference
  analysisRef <- data.frame(
    analysisId = as.integer(covariateSettings$analysisId),
    analysisName = as.character(paste("Extension table:", covariateSettings$extensionTableName)),
    domainId = as.character(""),
    startDay = as.integer(ifelse(is.null(covariateSettings$startDay), NA, covariateSettings$startDay)),
    endDay = as.integer(ifelse(is.null(covariateSettings$endDay), NA, covariateSettings$endDay)),
    isBinary = as.character(ifelse(covariateSettings$isBinary, "Y", "N")),
    missingMeansZero = as.character(ifelse(covariateSettings$missingMeansZero, "Y", "N")),
    stringsAsFactors = FALSE
  )

  # Create metadata with population size (required for PLP)
  metaData <- list(
    sql = sql,
    call = match.call(),
    extensionTable = covariateSettings$extensionTableName,
    populationSize = populationSize,
    cohortIds = cohortIds
  )

  # Create empty CovariateData object with correct Andromeda structure
  result <- createEmptyExtCovariateData(
    cohortIds = cohortIds,
    aggregated = aggregated,
    temporal = FALSE
  )

  # Append data to Andromeda tables (don't directly assign)
  # This ensures compatibility with FeatureExtraction's expected schema
  if (nrow(covariates) > 0) {
    Andromeda::appendToTable(result$covariates, covariates)
  }
  if (nrow(covariateRef) > 0) {
    Andromeda::appendToTable(result$covariateRef, covariateRef)
  }
  if (nrow(analysisRef) > 0) {
    Andromeda::appendToTable(result$analysisRef, analysisRef)
  }

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
#' @param parentTable (Optional) Name of parent table to join through if extension
#'   table doesn't have person_id directly (e.g., "waveform_occurrence" for "waveform_feature").
#' @param parentJoinField (Optional) Field to join extension table to parent table
#'   (e.g., "waveform_occurrence_id").
#' @param conceptSet (Optional) A Capr concept set object or a list of concept sets
#'   containing the concept IDs to use as covariates. This is used to build the
#'   covariateRef table with the correct concept IDs. If the concept sets use
#'   descendants(), the function will query the vocabulary to expand them.
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
#'
#' # Example 3: Extension table requiring parent table join (waveform data)
#' # waveform_feature doesn't have person_id or date fields - must join through waveform_occurrence
#' # IMPORTANT: Must provide conceptSet so covariateRef has the correct concept IDs
#' # Can pass a single concept set or a list of concept sets
#' hrv_concepts <- cs(descendants(2082499946), name = "Heart Rate Variability")
#' rr_irreg_concepts <- cs(descendants(2082499949), name = "RR Irregularity")
#' qrs_concepts <- cs(descendants(2082499955), name = "QRS Width Variability")
#'
#' covariateSettings <- createExtensionCovariateSettings(
#'   analysisId = 999,
#'   extensionDatabaseSchema = "cdm",
#'   extensionTableName = "waveform_feature",
#'   extensionFields = c("algorithm_concept_id", "value_as_number"),
#'   joinField = "person_id",  # Field in parent table
#'   covariateIdField = "algorithm_concept_id",
#'   covariateValueField = "value_as_number",
#'   dateField = "waveform_occurrence_start_datetime",  # Date field in PARENT table
#'   parentTable = "waveform_occurrence",  # Parent table with person_id and dates
#'   parentJoinField = "waveform_occurrence_id",  # Join field
#'   conceptSet = list(hrv_concepts, rr_irreg_concepts, qrs_concepts),  # List of concept sets
#'   startDay = -365,
#'   endDay = 0,
#'   isBinary = FALSE,
#'   missingMeansZero = FALSE
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
                                              parentTable = NULL,
                                              parentJoinField = NULL,
                                              conceptSet = NULL,
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
  checkmate::assertCharacter(parentTable, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(parentJoinField, len = 1, null.ok = TRUE, add = errorMessages)
  # conceptSet validation - accept NULL or list
  checkmate::assert(
    checkmate::checkNull(conceptSet),
    checkmate::checkList(conceptSet),
    add = errorMessages
  )
  checkmate::assertLogical(isBinary, len = 1, add = errorMessages)
  checkmate::assertLogical(missingMeansZero, len = 1, add = errorMessages)
  checkmate::assertLogical(warnOnAnalysisIdOverlap, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  # Check that parent table parameters are consistent
  if (!is.null(parentTable) && is.null(parentJoinField)) {
    stop("parentJoinField must be specified when using parentTable")
  }
  if (!is.null(parentJoinField) && is.null(parentTable)) {
    stop("parentTable must be specified when using parentJoinField")
  }

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
  attr(covariateSettings, "fun") <- "getDbExtCovariateData"
  class(covariateSettings) <- "covariateSettings"

  return(covariateSettings)
}

# Helper function to build the SQL query for extracting covariates
buildExtensionCovariateQuery <- function(cohortTable,
                                         rowIdField,
                                         cohortIds,
                                         covariateSettings) {

  # Start building the SELECT clause
  # Note: Always use table aliases - 'c' for cohort, 'ext' for extension table
  selectClause <- paste(
    "SELECT",
    "c.@row_id_field AS row_id,",
    "ext.@covariate_id_field AS covariate_id,",
    "MAX(ext.@covariate_value_field) AS covariate_value"
  )

  # Build FROM clause with optional parent table join
  if (!is.null(covariateSettings$parentTable) && !is.null(covariateSettings$parentJoinField)) {
    # Need to join through parent table: cohort -> parent -> extension
    fromClause <- paste(
      "FROM @cohort_table c",
      "INNER JOIN @extension_database_schema.@parent_table parent",
      paste0("ON c.subject_id = parent.@join_field"),
      "INNER JOIN @extension_database_schema.@extension_table ext",
      paste0("ON parent.@parent_join_field = ext.@parent_join_field")
    )
  } else {
    # Direct join: cohort -> extension
    fromClause <- paste(
      "FROM @cohort_table c",
      "INNER JOIN @extension_database_schema.@extension_table ext",
      paste0("ON c.subject_id = ext.@join_field")
    )
  }

  # Build WHERE clause
  whereConditions <- c()

  # Add cohort ID filter if specified
  if (!(-1 %in% cohortIds)) {
    whereConditions <- c(whereConditions, "c.cohort_definition_id IN (@cohort_ids)")
  }

  # Add temporal filtering if specified
  if (!is.null(covariateSettings$startDay) && !is.null(covariateSettings$endDay)) {
    # Determine which table alias to use for date field
    # If using parent table join, date likely comes from parent (e.g., waveform_occurrence)
    # Otherwise, date comes from extension table
    dateTableAlias <- if (!is.null(covariateSettings$parentTable)) {
      "parent"  # Date field in parent table (e.g., waveform_occurrence_start_datetime)
    } else {
      "ext"     # Date field in extension table
    }

    whereConditions <- c(
      whereConditions,
      sprintf(
        "%s.%s >= DATEADD(day, %d, c.cohort_start_date)",
        dateTableAlias,
        covariateSettings$dateField,
        covariateSettings$startDay
      ),
      sprintf(
        "%s.%s <= DATEADD(day, %d, c.cohort_start_date)",
        dateTableAlias,
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

  # Group Clause (consolidate to max)
  groupClause <- paste(
      "GROUP BY c.@row_id_field,",
      "ext.@covariate_id_field"
    )
  # Combine all parts
  sql <- paste(selectClause, fromClause, whereClause, groupClause)

  return(sql)
}

# Helper function to build the covariate reference table
buildCovariateReference <- function(connection,
                                    cdmDatabaseSchema,
                                    tempEmulationSchema,
                                    covariateSettings) {

  # If a reference table is specified, query it
  if (!is.null(covariateSettings$covariateRefTable) &&
      !is.null(covariateSettings$covariateNameField)) {

    refSql <- paste(
      "SELECT",
      paste0(covariateSettings$covariateIdField, " AS covariate_id,"),
      paste0(covariateSettings$covariateNameField, " AS covariate_name,"),
      paste0(covariateSettings$analysisId, " AS analysis_id,"),
      paste0(covariateSettings$covariateIdField, " AS concept_id,"),
      "CAST(NULL AS INTEGER) AS value_as_concept_id,",
      "CAST(NULL AS INTEGER) AS collisions",
      "FROM",
      paste0(covariateSettings$extensionDatabaseSchema, ".", covariateSettings$covariateRefTable)
    )

    refSql <- SqlRender::translate(refSql,
      targetDialect = attr(connection, "dbms"),
      tempEmulationSchema = tempEmulationSchema
    )

    covariateRef <- DatabaseConnector::querySql(connection, refSql, snakeCaseToCamelCase = TRUE)
  } else if (!is.null(covariateSettings$conceptSet)) {
    # Extract concept IDs from the provided concept set(s)
    # This will expand descendants if needed by querying the vocabulary
    conceptIds <- extractConceptIdsFromConceptSet(
      covariateSettings$conceptSet,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema
    )

    if (length(conceptIds) > 0) {
      # Try to get concept names from the vocabulary
      conceptNames <- tryCatch({
        nameSql <- "
          SELECT concept_id, concept_name
          FROM @cdm_database_schema.concept
          WHERE concept_id IN (@concept_ids)
        "
        nameSql <- SqlRender::render(
          nameSql,
          cdm_database_schema = cdmDatabaseSchema,
          concept_ids = conceptIds
        )
        nameSql <- SqlRender::translate(
          nameSql,
          targetDialect = attr(connection, "dbms"),
          tempEmulationSchema = tempEmulationSchema
        )
        namesDf <- DatabaseConnector::querySql(connection, nameSql, snakeCaseToCamelCase = TRUE)
        # Create a lookup
        names_lookup <- setNames(namesDf$conceptName, namesDf$conceptId)
        names_lookup
      }, error = function(e) {
        warning("Could not query concept names from vocabulary: ", e$message)
        NULL
      })

      # Build reference with names if available
      if (!is.null(conceptNames)) {
        covariateRef <- data.frame(
          covariateId = as.numeric(conceptIds),
          covariateName = sapply(conceptIds, function(id) {
            name <- conceptNames[as.character(id)]
            if (is.null(name) || is.na(name)) {
              paste(covariateSettings$extensionTableName, "concept:", id)
            } else {
              paste(covariateSettings$extensionTableName, "-", name)
            }
          }),
          analysisId = covariateSettings$analysisId,
          conceptId = as.numeric(conceptIds),
          valueAsConceptId = NA_integer_,
          collisions = NA_integer_,
          stringsAsFactors = FALSE
        )
      } else {
        # Fallback without names
        covariateRef <- data.frame(
          covariateId = as.numeric(conceptIds),
          covariateName = paste(
            covariateSettings$extensionTableName,
            "concept:",
            conceptIds
          ),
          analysisId = covariateSettings$analysisId,
          conceptId = as.numeric(conceptIds),
          valueAsConceptId = NA_integer_,
          collisions = NA_integer_,
          stringsAsFactors = FALSE
        )
      }
    } else {
      # Empty concept set
      covariateRef <- data.frame(
        covariateId = numeric(0),
        covariateName = character(0),
        analysisId = integer(0),
        conceptId = integer(0),
        valueAsConceptId = integer(0),
        collisions = integer(0),
        stringsAsFactors = FALSE
      )
    }
  } else {
    # No reference table or concept set - create minimal placeholder
    # This will likely result in missing covariates in the model
    warning("No covariateRefTable or conceptSet provided. Covariate reference will be incomplete.")
    covariateRef <- data.frame(
      covariateId = numeric(0),
      covariateName = character(0),
      analysisId = integer(0),
      conceptId = integer(0),
      valueAsConceptId = integer(0),
      collisions = integer(0),
      stringsAsFactors = FALSE
    )
  }

  # Ensure correct column order and types for Andromeda compatibility
  if (nrow(covariateRef) > 0) {
    covariateRef <- covariateRef[, c("covariateId", "covariateName", "analysisId", "conceptId", "valueAsConceptId", "collisions")]
    covariateRef$covariateId <- as.numeric(covariateRef$covariateId)
    covariateRef$covariateName <- as.character(covariateRef$covariateName)
    covariateRef$analysisId <- as.integer(covariateRef$analysisId)
    covariateRef$conceptId <- as.integer(covariateRef$conceptId)
    covariateRef$valueAsConceptId <- as.integer(covariateRef$valueAsConceptId)
    covariateRef$collisions <- as.integer(covariateRef$collisions)
  }

  return(covariateRef)
}

# Helper function to extract concept IDs from a Capr concept set or list of concept sets
# Expands descendants by querying the vocabulary if needed
extractConceptIdsFromConceptSet <- function(conceptSet,
                                           connection = NULL,
                                           cdmDatabaseSchema = NULL,
                                           tempEmulationSchema = NULL) {
  allConceptIds <- c()

  # Check if this is a single Capr ConceptSet object or list of them
  if (inherits(conceptSet, "ConceptSet")) {
    # Single Capr ConceptSet object
    conceptSetList <- list(conceptSet)
  } else if (is.list(conceptSet) && length(conceptSet) > 0) {
    # Check if first element is a Capr ConceptSet
    firstElem <- conceptSet[[1]]

    if (inherits(firstElem, "ConceptSet")) {
      # List of Capr ConceptSet objects
      conceptSetList <- conceptSet
    } else if (is.list(firstElem)) {
      if (!is.null(firstElem$Expression) ||
          !is.null(firstElem$conceptId) ||
          !is.null(firstElem$id)) {
        # List of concept sets (old format)
        conceptSetList <- conceptSet
      } else {
        # Single concept set
        conceptSetList <- list(conceptSet)
      }
    } else {
      # Unknown format
      conceptSetList <- list(conceptSet)
    }
  } else if (is.numeric(conceptSet) || is.integer(conceptSet)) {
    # Direct vector of concept IDs
    return(unique(as.numeric(conceptSet)))
  } else if (is.list(conceptSet) && !is.null(names(conceptSet)) &&
             any(c("Expression", "conceptId", "id") %in% names(conceptSet))) {
    # Single concept set with named fields
    conceptSetList <- list(conceptSet)
  } else {
    # Unknown format
    conceptSetList <- list(conceptSet)
  }

  # Process each concept set
  for (i in seq_along(conceptSetList)) {
    cs <- conceptSetList[[i]]
    baseConceptIds <- c()
    needsDescendants <- FALSE

    # Handle Capr ConceptSet objects (S4 class with @Expression slot)
    if (inherits(cs, "ConceptSet")) {
      # Capr ConceptSet is an S4 object with @Expression slot
      # Each item in @Expression has @Concept@concept_id and includeDescendants field
      tryCatch({
        # Check if this is an S4 object with @Expression slot
        if (isS4(cs) && "Expression" %in% methods::slotNames(cs)) {
          expressions <- methods::slot(cs, "Expression")

          # Extract concept IDs from each expression item
          for (j in seq_along(expressions)) {
            expr_item <- expressions[[j]]

            # Try to extract concept_id from @Concept slot
            if (isS4(expr_item)) {
              tryCatch({
                concept <- methods::slot(expr_item, "Concept")
                concept_id <- methods::slot(concept, "concept_id")
                baseConceptIds <- c(baseConceptIds, concept_id)

                # Check for includeDescendants
                incl_desc <- methods::slot(expr_item, "includeDescendants")
                if (incl_desc == TRUE) {
                  needsDescendants <- TRUE
                }
              }, error = function(e) {
                warning("Error extracting from expression item: ", e$message)
              })
            }
          }
        }
      }, error = function(e) {
        warning("Error extracting from ConceptSet S4: ", e$message)
      })
    } else if (is.list(cs)) {
      # Handle different concept set structures from Capr (old format)
      if (!is.null(cs$Expression)) {
        # Extract concept IDs from each item in Expression
        for (j in seq_along(cs$Expression)) {
          item <- cs$Expression[[j]]

          # Get the concept ID
          conceptId <- NULL
          if (!is.null(item$concept$CONCEPT_ID)) {
            conceptId <- item$concept$CONCEPT_ID
          } else if (!is.null(item$Concept$CONCEPT_ID)) {
            conceptId <- item$Concept$CONCEPT_ID
          } else if (!is.null(item$concept$conceptId)) {
            conceptId <- item$concept$conceptId
          } else if (!is.null(item$Concept$conceptId)) {
            conceptId <- item$Concept$conceptId
          }

          if (!is.null(conceptId)) {
            baseConceptIds <- c(baseConceptIds, conceptId)

            # Check if descendants should be included
            if (!is.null(item$includeDescendants) && item$includeDescendants == TRUE) {
              needsDescendants <- TRUE
            } else if (!is.null(item$isExcluded) && item$isExcluded == FALSE &&
                      !is.null(item$includeDescendants)) {
              needsDescendants <- TRUE
            }
          }
        }
      } else if (!is.null(cs$conceptId)) {
        # Simple list with conceptId field
        baseConceptIds <- cs$conceptId
      } else if (!is.null(cs$id)) {
        # Simple list with id field
        baseConceptIds <- cs$id
      }
    }

    # If descendants are needed and we have a connection, query for them
    if (needsDescendants && !is.null(connection) && !is.null(cdmDatabaseSchema) &&
        length(baseConceptIds) > 0) {

      descendantIds <- tryCatch({
        descSql <- "
          SELECT DISTINCT descendant_concept_id
          FROM @cdm_database_schema.concept_ancestor
          WHERE ancestor_concept_id IN (@ancestor_ids)
          UNION
          SELECT DISTINCT @ancestor_ids AS descendant_concept_id
        "
        descSql <- SqlRender::render(
          descSql,
          cdm_database_schema = cdmDatabaseSchema,
          ancestor_ids = baseConceptIds
        )
        descSql <- SqlRender::translate(
          descSql,
          targetDialect = attr(connection, "dbms"),
          tempEmulationSchema = tempEmulationSchema
        )
        descDf <- DatabaseConnector::querySql(connection, descSql, snakeCaseToCamelCase = TRUE)
        descDf$descendantConceptId
      }, error = function(e) {
        warning("Could not query descendants from vocabulary: ", e$message, ". Using base concept IDs only.")
        baseConceptIds
      })

      allConceptIds <- c(allConceptIds, descendantIds)
    } else {
      # No descendants needed, just use base IDs
      allConceptIds <- c(allConceptIds, baseConceptIds)
    }
  }

  # Return unique concept IDs
  return(unique(as.numeric(allConceptIds)))
}


createEmptyExtCovariateData <- function(cohortIds, aggregated, temporal) {
  dummy <- tibble(
    covariateId = 1,
    covariateValue = 1
  )
  if (!aggregated) {
    dummy$rowId <- 1
  }
  if (!is.null(temporal) && temporal) {
    dummy$timeId <- 1
  }
  covariateData <- Andromeda::andromeda(
    covariates = dummy[!1, ],
    covariateRef = tibble(
      covariateId = 1,
      covariateName = "",
      analysisId = 1,
      conceptId = 1,
      valueAsConceptId = 1,
      collisions = 1
    )[!1, ],
    analysisRef = tibble(
      analysisId = 1,
      analysisName = "",
      domainId = "",
      startDay = 1,
      endDay = 1,
      isBinary = "",
      missingMeansZero = ""
    )[!1, ]
  )
  attr(covariateData, "metaData") <- list(
    populationSize = 0,
    cohortIds = cohortIds
  )
  class(covariateData) <- "CovariateData"
  return(covariateData)
}

