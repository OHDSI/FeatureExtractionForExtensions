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

#' FeatureExtractionForExtensions
#'
#' @description
#' FeatureExtractionForExtensions is an R package that enables users to create
#' covariate objects for analytics pipelines that reference extension tables in
#' OMOP CDM databases. Extension tables are custom tables that extend the standard
#' CDM schema with additional data elements not part of the core specification.
#'
#' This package builds upon the OHDSI FeatureExtraction package and follows the
#' same design patterns for creating custom covariate builders.
#'
#' @section Main Functions:
#' \itemize{
#'   \item \code{\link{createExtensionCovariateSettings}}: Create settings for
#'         extension table covariates
#'   \item \code{\link{getDbExtCovariateData}}: Extract covariates from
#'         extension tables
#' }
#'
#' @section Typical Workflow:
#' \enumerate{
#'   \item Create a connection to your database using DatabaseConnector
#'   \item Define your covariate settings using \code{createExtensionCovariateSettings}
#'   \item Extract covariates using \code{getDbExtCovariateData}
#'   \item Use the resulting CovariateData object with other OHDSI tools
#' }
#'
#' @section Extension Table Requirements:
#' Your extension table should have:
#' \itemize{
#'   \item A join field linking to patients (typically person_id)
#'   \item A covariate ID field identifying each covariate
#'   \item A covariate value field containing the covariate values
#'   \item (Optional) A date field for temporal filtering
#' }
#'
#' @examples
#' \dontrun{
#' library(FeatureExtractionForExtensions)
#' library(DatabaseConnector)
#'
#' # Connect to database
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/ohdsi"
#' )
#' connection <- connect(connectionDetails)
#'
#' # Create covariate settings
#' covariateSettings <- createExtensionCovariateSettings(
#'   analysisId = 998,
#'   extensionDatabaseSchema = "my_extensions",
#'   extensionTableName = "patient_biomarkers",
#'   extensionFields = c("biomarker_id", "biomarker_value"),
#'   joinField = "person_id",
#'   covariateIdField = "biomarker_id",
#'   covariateValueField = "biomarker_value"
#' )
#'
#' # Extract covariates
#' covariateData <- getDbExtCovariateData(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm",
#'   cohortTable = "#cohort",
#'   cohortIds = 1,
#'   covariateSettings = covariateSettings
#' )
#'
#' # View summary
#' summary(covariateData)
#'
#' disconnect(connection)
#' }
#'
#' @docType package
#' @name FeatureExtractionForExtensions-package
#' @aliases FeatureExtractionForExtensions
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom Andromeda andromeda
#' @importFrom SqlRender render translate
#' @importFrom dplyr %>% filter select mutate collect
## usethis namespace: end
NULL
