# Server-side aggregation of extension covariates.
#
# The database computes per cohort and covariate: count, min, max, sum and sum of squares, and the values at the
# ranks needed for the percentiles. The statistics are then assembled here with exactly the formulas of
# FeatureExtraction::aggregateCovariates (including the zero-imputation used when missingMeansZero is "Y"), so
# the result equals aggregating the person-level data in R.

PERCENTILE_PROBS <- c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1)

# 1-based index of the order statistic that stats::quantile(x, probs, type = 1) returns for n values
quantileType1Index <- function(n, probs) {
  fuzz <- 4 * .Machine$double.eps
  nppm <- n * probs
  j <- floor(nppm + fuzz)
  h <- as.numeric(nppm > j)
  pmin(pmax(j + h, 1), n)
}

# For one covariate in one cohort: which probabilities have to be read from the data (the others are zeros
# when missing values mean zero) and the rank of the value to read for each of them
percentileRanks <- function(n, populationSize, missingMeansZero) {
  if (missingMeansZero) {
    zeroFraction <- 1 - n / populationSize
    probs <- PERCENTILE_PROBS[PERCENTILE_PROBS >= zeroFraction]
    probs <- (probs - zeroFraction) / (1 - zeroFraction)
  } else {
    probs <- PERCENTILE_PROBS
  }
  quantileType1Index(n, probs)
}

# Ranks to fetch from the server for every row of the statistics table
planPercentileRanks <- function(stats, populationSize, missingMeansZero) {
  plan <- vector("list", nrow(stats))
  for (i in seq_len(nrow(stats))) {
    ranks <- unique(percentileRanks(stats$n[i], populationSize[[as.character(stats$cohortDefinitionId[i])]], missingMeansZero))
    plan[[i]] <- data.frame(
      cohortDefinitionId = stats$cohortDefinitionId[i],
      covariateId = stats$covariateId[i],
      rn = ranks
    )
  }
  if (length(plan) == 0) {
    return(data.frame(cohortDefinitionId = numeric(0), covariateId = numeric(0), rn = numeric(0)))
  }
  do.call(rbind, plan)
}

# Continuous statistics from the database summaries and the fetched order statistics
assembleContinuousStatistics <- function(stats, rankValues, populationSize, missingMeansZero) {
  rows <- vector("list", nrow(stats))
  for (i in seq_len(nrow(stats))) {
    n <- stats$n[i]
    pop <- populationSize[[as.character(stats$cohortDefinitionId[i])]]
    ranks <- percentileRanks(n, pop, missingMeansZero)
    values <- rankValues[rankValues$cohortDefinitionId == stats$cohortDefinitionId[i] &
                           rankValues$covariateId == stats$covariateId[i], ]
    quants <- values$covariateValue[match(ranks, values$rn)]
    quants <- c(rep(0, length(PERCENTILE_PROBS) - length(quants)), quants)
    if (missingMeansZero) {
      average <- stats$sumValue[i] / pop
      standardDeviation <- sqrt((pop * stats$sumSq[i] - stats$sumValue[i]^2) / (pop * (pop - 1)))
    } else {
      average <- stats$sumValue[i] / n
      standardDeviation <- sqrt((n * stats$sumSq[i] - stats$sumValue[i]^2) / (n * (n - 1)))
    }
    rows[[i]] <- data.frame(
      cohortDefinitionId = stats$cohortDefinitionId[i],
      covariateId = stats$covariateId[i],
      countValue = n,
      minValue = quants[1],
      maxValue = quants[7],
      averageValue = average,
      standardDeviation = standardDeviation,
      medianValue = quants[4],
      p10Value = quants[2],
      p25Value = quants[3],
      p75Value = quants[5],
      p90Value = quants[6]
    )
  }
  do.call(rbind, rows)
}

# Aggregate on the database. rowsSql is the (rendered, not yet translated) query that returns one row per cohort
# row and covariate: cohort_definition_id, subject_id, cohort_start_date, covariate_id, covariate_value.
aggregateExtensionCovariatesOnServer <- function(connection,
                                                 rowsSql,
                                                 tempEmulationSchema,
                                                 covariateRef,
                                                 analysisRef,
                                                 populationSize,
                                                 minCharacterizationMean = 0) {
  dbms <- attr(connection, "dbms")
  isBinary <- analysisRef$isBinary[1] == "Y"
  missingMeansZero <- analysisRef$missingMeansZero[1] == "Y"
  cohortIds <- as.numeric(names(populationSize))
  result <- createEmptyExtCovariateData(cohortIds = cohortIds, aggregated = TRUE, temporal = FALSE)

  run <- function(sql) {
    sql <- SqlRender::translate(sql, targetDialect = dbms, tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  query <- function(sql) {
    sql <- SqlRender::translate(sql, targetDialect = dbms, tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  }

  run(paste0("SELECT cohort_definition_id, subject_id, cohort_start_date, covariate_id, covariate_value ",
             "INTO #ext_cov_rows FROM (", rowsSql, ") level1;"))
  on.exit({
    try(run("TRUNCATE TABLE #ext_cov_rows; DROP TABLE #ext_cov_rows;"), silent = TRUE)
    try(run("TRUNCATE TABLE #ext_cov_ranks; DROP TABLE #ext_cov_ranks;"), silent = TRUE)
  }, add = TRUE)

  stats <- query("
    SELECT cohort_definition_id, covariate_id, COUNT(*) AS n,
           SUM(CAST(covariate_value AS FLOAT)) AS sum_value,
           SUM(CAST(covariate_value AS FLOAT) * CAST(covariate_value AS FLOAT)) AS sum_sq
    FROM #ext_cov_rows
    GROUP BY cohort_definition_id, covariate_id;")
  stats$cohortDefinitionId <- as.numeric(stats$cohortDefinitionId)
  stats$covariateId <- as.numeric(stats$covariateId)
  stats$n <- as.numeric(stats$n)
  stats <- stats[order(stats$cohortDefinitionId, stats$covariateId), , drop = FALSE]

  if (nrow(stats) > 0) {
    if (isBinary) {
      binary <- data.frame(
        cohortDefinitionId = stats$cohortDefinitionId,
        covariateId = stats$covariateId,
        sumValue = stats$sumValue,
        averageValue = stats$sumValue / unname(populationSize[as.character(stats$cohortDefinitionId)])
      )
      binary <- binary[binary$averageValue >= minCharacterizationMean, , drop = FALSE]
      if (nrow(binary) > 0) {
        Andromeda::appendToTable(result$covariates, binary)
      }
    } else {
      ranks <- planPercentileRanks(stats, populationSize, missingMeansZero)
      DatabaseConnector::insertTable(
        connection = connection,
        tableName = "#ext_cov_ranks",
        data = ranks,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        tempTable = TRUE,
        tempEmulationSchema = tempEmulationSchema,
        progressBar = FALSE,
        camelCaseToSnakeCase = TRUE
      )
      rankValues <- query("
        SELECT r.cohort_definition_id, r.covariate_id, r.rn, r.covariate_value
        FROM (
          SELECT cohort_definition_id, covariate_id, covariate_value,
                 ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, covariate_id ORDER BY covariate_value) AS rn
          FROM #ext_cov_rows
        ) r
        INNER JOIN #ext_cov_ranks k
          ON r.cohort_definition_id = k.cohort_definition_id AND r.covariate_id = k.covariate_id AND r.rn = k.rn;")
      rankValues$cohortDefinitionId <- as.numeric(rankValues$cohortDefinitionId)
      rankValues$covariateId <- as.numeric(rankValues$covariateId)
      rankValues$rn <- as.numeric(rankValues$rn)
      continuous <- assembleContinuousStatistics(stats, rankValues, populationSize, missingMeansZero)
      Andromeda::appendToTable(result$covariatesContinuous, continuous)
    }
  }

  if (nrow(covariateRef) > 0) {
    Andromeda::appendToTable(result$covariateRef, covariateRef)
  }
  Andromeda::appendToTable(result$analysisRef, analysisRef)
  return(result)
}
