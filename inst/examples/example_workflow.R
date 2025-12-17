# Example workflow for using FeatureExtractionForExtensions
# This script demonstrates how to use the package to extract covariates
# from extension tables

# Install and load required packages
# install.packages("remotes")
# remotes::install_github("OHDSI/FeatureExtractionForExtensions")

library(FeatureExtractionForExtensions)
library(FeatureExtraction)
library(DatabaseConnector)

# ============================================================================
# Step 1: Setup Database Connection
# ============================================================================

# Configure your database connection details
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",  # or "sql server", "databricks", "redshift", etc.
  server = "localhost/ohdsi",
  user = "your_username",
  password = "your_password",
  port = 5432
)

# Connect to the database
connection <- connect(connectionDetails)

# ============================================================================
# Step 2: Define Extension Table Covariate Settings
# ============================================================================

# Example 1: Simple biomarker covariates
biomarkerSettings <- createExtensionCovariateSettings(
  analysisId = 998,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_biomarkers",
  extensionFields = c("biomarker_id", "biomarker_value"),
  joinField = "person_id",
  covariateIdField = "biomarker_id",
  covariateValueField = "biomarker_value",
  covariateRefTable = "biomarker_definitions",
  covariateNameField = "biomarker_name",
  isBinary = FALSE,
  missingMeansZero = FALSE
)

# Example 2: Temporal covariates (last 30 days)
temporalBiomarkerSettings <- createExtensionCovariateSettings(
  analysisId = 997,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_biomarkers",
  extensionFields = c("biomarker_id", "biomarker_value", "measurement_date"),
  joinField = "person_id",
  covariateIdField = "biomarker_id",
  covariateValueField = "biomarker_value",
  dateField = "measurement_date",
  startDay = -30,
  endDay = 0,
  isBinary = FALSE,
  missingMeansZero = FALSE
)
# ============================================================================
# Step 3: Extract Covariates
# ============================================================================

# Extract covariates from extension table only
extensionCovariateData <- getDbExtensionCovariateData(
  connection = connection,
  cdmDatabaseSchema = "cdm_schema",
  cohortTable = "#cohort_table",
  cohortIds = 1,
  rowIdField = "subject_id",
  covariateSettings = biomarkerSettings,
  aggregated = FALSE
)

# View summary
print(summary(extensionCovariateData))

# ============================================================================
# Step 4: Combine with Standard FeatureExtraction Covariates
# ============================================================================

# Define standard demographic covariates
demographicsSettings <- createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAgeGroup = TRUE,
  useDemographicsRace = TRUE,
  useDemographicsEthnicity = TRUE
)

# Define standard condition covariates
conditionSettings <- createCovariateSettings(
  useConditionOccurrenceAnyTimePrior = TRUE
)

# Combine all covariate settings
allCovariateSettings <- list(
  demographicsSettings,
  conditionSettings,
  biomarkerSettings,
  sdohSettings
)

# Extract all covariates together
allCovariateData <- getDbCovariateData(
  connection = connection,
  cdmDatabaseSchema = "cdm_schema",
  cohortDatabaseSchema = "results_schema",
  cohortTable = "cohort",
  cohortIds = c(1, 2),  # Multiple cohorts
  covariateSettings = allCovariateSettings
)

# View comprehensive summary
print(summary(allCovariateData))

# ============================================================================
# Step 5: Use with PatientLevelPrediction
# ============================================================================

library(PatientLevelPrediction)

# Get PLP data including extension covariates
plpData <- getPlpData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "cdm_schema",
  cohortDatabaseSchema = "results_schema",
  cohortTable = "cohort",
  cohortId = 1,
  covariateSettings = allCovariateSettings,
  outcomeDatabaseSchema = "results_schema",
  outcomeTable = "cohort",
  outcomeIds = 2
)

# Define model settings
modelSettings <- setLassoLogisticRegression()

# Run the prediction model
model <- runPlp(
  plpData = plpData,
  outcomeId = 2,
  modelSettings = modelSettings,
  analysisId = "Extension_Covariates_Model",
  analysisName = "Model with Extension Table Covariates"
)

# View results
print(model)

# ============================================================================
# Step 6: Save and Load Covariate Data
# ============================================================================

# Save covariate data to file
saveCovariateData(
  covariateData = allCovariateData,
  file = "covariateData.zip"
)

# Load covariate data from file
loadedCovariateData <- loadCovariateData("covariateData.zip")

# ============================================================================
# Step 7: Explore Covariate Data
# ============================================================================

# View covariate reference (what covariates were extracted)
covariateRef <- allCovariateData$covariateRef %>%
  collect()
head(covariateRef)

# View analysis reference (what analyses were performed)
analysisRef <- allCovariateData$analysisRef %>%
  collect()
print(analysisRef)

# View actual covariate values (first 100 rows)
covariates <- allCovariateData$covariates %>%
  head(100) %>%
  collect()
print(covariates)

# ============================================================================
# Bonus Points - Some More Examples
# ============================================================================

# Example: Using temporal windows for different time periods
# Past year (excluding last 30 days)
historicalSettings <- createExtensionCovariateSettings(
  analysisId = 996,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_biomarkers",
  extensionFields = c("biomarker_id", "biomarker_value", "measurement_date"),
  joinField = "person_id",
  covariateIdField = "biomarker_id",
  covariateValueField = "biomarker_value",
  dateField = "measurement_date",
  startDay = -365,
  endDay = -31,
  isBinary = FALSE
)

# Most recent (last 7 days)
recentSettings <- createExtensionCovariateSettings(
  analysisId = 995,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_biomarkers",
  extensionFields = c("biomarker_id", "biomarker_value", "measurement_date"),
  joinField = "person_id",
  covariateIdField = "biomarker_id",
  covariateValueField = "biomarker_value",
  dateField = "measurement_date",
  startDay = -7,
  endDay = 0,
  isBinary = FALSE
)

# Compare temporal patterns
temporalSettingsList <- list(
  historicalSettings,
  recentSettings
)

print("Example workflow completed successfully!")
