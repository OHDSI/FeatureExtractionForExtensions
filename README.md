# FeatureExtractionForExtensions

## Overview

**FeatureExtractionForExtensions** is an R package that enables users to create covariate objects for analytics pipelines that reference extension tables in OMOP CDM databases. Extension tables are custom tables that extend the standard CDM schema with additional data elements not part of the core specification.

This package builds upon the [OHDSI FeatureExtraction](https://github.com/OHDSI/FeatureExtraction) package and follows the patterns (and specifically, the vignette) for creating custom covariate builders.

## Features

- Create covariates from custom extension tables
- Support for both binary and continuous covariates
- Temporal filtering based on date fields
- Integration with FeatureExtraction and other OHDSI tools
- Compatible with all DatabaseConnector-supported database platforms

## Installation

```r
# Install from GitHub
# install.packages("remotes")
remotes::install_github("OHDSI/FeatureExtractionForExtensions")
```

## Requirements

- R >= 3.2.2
- FeatureExtraction >= 3.0.0
- DatabaseConnector >= 3.0.0
- SqlRender >= 1.18.0
- Andromeda >= 1.0.0

## Quick Start

### Basic Usage

```r
library(FeatureExtractionForExtensions)
library(DatabaseConnector)

# Connect to your database
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/ohdsi",
  user = "user",
  password = "password"
)
connection <- connect(connectionDetails)

# Create covariate settings for an extension table
covariateSettings <- createExtensionCovariateSettings(
  analysisId = 998,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_biomarkers",
  extensionFields = c("biomarker_id", "biomarker_value"),
  joinField = "person_id",
  covariateIdField = "biomarker_id",
  covariateValueField = "biomarker_value",
  isBinary = FALSE,
  missingMeansZero = FALSE
)

# Extract covariates
covariateData <- getDbExtensionCovariateData(
  connection = connection,
  cdmDatabaseSchema = "cdm",
  cohortTable = "#cohort",
  cohortIds = 1,
  covariateSettings = covariateSettings
)

# Use with FeatureExtraction functions
summary(covariateData)

disconnect(connection)
```

### Temporal Filtering

You can filter covariates based on a date field relative to the index date:

```r
covariateSettings <- createExtensionCovariateSettings(
  analysisId = 997,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_scores",
  extensionFields = c("score_id", "score_value", "score_date"),
  joinField = "person_id",
  covariateIdField = "score_id",
  covariateValueField = "score_value",
  dateField = "score_date",
  startDay = -365,  # 365 days before index
  endDay = 0,       # Up to index date
  isBinary = FALSE,
  missingMeansZero = TRUE
)
```

### Combining with Standard Covariates

You can combine extension table covariates with standard FeatureExtraction covariates:

```r
library(FeatureExtraction)

# Standard demographics
stdSettings <- createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAgeGroup = TRUE
)

# Extension table covariates
extSettings <- createExtensionCovariateSettings(
  analysisId = 998,
  extensionDatabaseSchema = "my_extensions",
  extensionTableName = "patient_biomarkers",
  joinField = "person_id",
  covariateIdField = "biomarker_id",
  covariateValueField = "biomarker_value"
)

# Combine both
covariateSettingsList <- list(stdSettings, extSettings)

covariateData <- getDbCovariateData(
  connection = connection,
  cdmDatabaseSchema = "cdm",
  cohortTable = "#cohort",
  cohortIds = 1,
  covariateSettings = covariateSettingsList
)
```

## Extension Table Structure

Your extension table should follow these general guidelines:

1. **Join Field**: A field that links to the cohort (typically `person_id`)
2. **Covariate ID Field**: A field that uniquely identifies each covariate
3. **Covariate Value Field**: A field containing the covariate value
4. **Date Field** (optional): A date field for temporal filtering

### Example Extension Table Schema

```sql
CREATE TABLE my_extensions.patient_biomarkers (
  person_id BIGINT NOT NULL,
  biomarker_id INT NOT NULL,
  biomarker_value FLOAT,
  measurement_date DATE,
  PRIMARY KEY (person_id, biomarker_id, measurement_date)
);
```

### Example Reference Table Schema

```sql
CREATE TABLE my_extensions.biomarker_definitions (
  biomarker_id INT PRIMARY KEY,
  biomarker_name VARCHAR(255),
  biomarker_unit VARCHAR(50)
);
```

## Use Cases

### 1. Lab Results and Biomarkers

Extract lab values and biomarkers stored in custom tables:

```r
covariateSettingsEXT <- createExtensionCovariateSettings(
  analysisId = 950,
  extensionDatabaseSchema = "lab_extensions",
  extensionTableName = "lab_results",
  joinField = "person_id",
  covariateIdField = "lab_test_id",
  covariateValueField = "result_value",
  covariateRefTable = "lab_test_definitions",
  covariateNameField = "test_name",
  dateField = "result_date",
  startDay = -30,
  endDay = 0
)
```

## Integration with OHDSI Tools

### PatientLevelPrediction

```r
library(PatientLevelPrediction)

plpData <- getPlpData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "cdm",
  cohortDatabaseSchema = "results",
  cohortTable = "cohort",
  cohortId = 1,
  covariateSettings = covariateSettingsEXT, # Note that this only uses the extension covariates and nothing else!
  outcomeDatabaseSchema = "results",
  outcomeTable = "cohort",
  outcomeIds = 2
)
```

### CohortMethod

```r
library(CohortMethod)

covariateData <- getDbCovariateData(
  connection = connection,
  cdmDatabaseSchema = "cdm",
  cohortDatabaseSchema = "results",
  cohortTable = "cohort",
  cohortIds = c(1, 2),
  covariateSettings = covariateSettingsEXT
)
```

## Documentation

For more detailed documentation, see:

- [Creating Custom Covariate Builders](vignettes/CreatingExtensionCovariates.Rmd)
- Package documentation: `?createExtensionCovariateSettings`
- [OHDSI FeatureExtraction Documentation](https://ohdsi.github.io/FeatureExtraction/)

## Contributing

Contributions are welcome! Please see the [HADES documention](https://ohdsi.github.io/Hades/contribute.html) for info on getting involved

## License

This project is licensed under Apache License 2.0.
