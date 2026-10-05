make_inputs <- function(isBinary = "N", missingMeansZero = "N") {
  covariates <- data.frame(
    cohortDefinitionId = c(1, 1, 1, 1, 2, 2),
    subjectId = c(10, 10, 11, 12, 20, 21),
    cohortStartDate = as.Date("2016-01-01"),
    covariateId = c(101, 102, 101, 101, 101, 102),
    covariateValue = c(1, 3.5, 2, 4, 10, 20)
  )
  if (isBinary == "Y") covariates$covariateValue <- 1  # binary covariates carry the value 1
  covariateRef <- data.frame(covariateId = c(101, 102), covariateName = c("a", "b"), analysisId = 1L,
                             conceptId = c(101L, 102L), valueAsConceptId = NA_integer_, collisions = NA_integer_)
  analysisRef <- data.frame(analysisId = 1L, analysisName = "x", domainId = "", startDay = NA_integer_, endDay = NA_integer_,
                            isBinary = isBinary, missingMeansZero = missingMeansZero, stringsAsFactors = FALSE)
  list(covariates = covariates, covariateRef = covariateRef, analysisRef = analysisRef,
       populationSize = c("1" = 4, "2" = 3))
}
aggregate <- function(x, ...) {
  FeatureExtractionForExtensions:::aggregateExtensionCovariates(x$covariates, x$covariateRef, x$analysisRef, x$populationSize, ...)
}

test_that("continuous covariates are aggregated per cohort", {
  x <- make_inputs()
  agg <- aggregate(x)
  cont <- dplyr::collect(agg$covariatesContinuous)
  c1 <- cont[cont$cohortDefinitionId == 1 & cont$covariateId == 101, ]
  expect_equal(c1$countValue, 3)
  expect_equal(c1$averageValue, mean(c(1, 2, 4)))
  expect_equal(c1$standardDeviation, sd(c(1, 2, 4)))
  expect_equal(c1$minValue, 1)
  expect_equal(c1$maxValue, 4)
  expect_equal(nrow(dplyr::collect(agg$covariates)), 0)
  Andromeda::close(agg)
})

test_that("binary covariates give a count and a proportion of the cohort population", {
  agg <- aggregate(make_inputs(isBinary = "Y"))
  bin <- dplyr::collect(agg$covariates)
  b <- bin[bin$cohortDefinitionId == 1 & bin$covariateId == 101, ]
  expect_equal(b$sumValue, 3)
  expect_equal(b$averageValue, 3 / 4)
  b2 <- bin[bin$cohortDefinitionId == 2 & bin$covariateId == 102, ]
  expect_equal(b2$averageValue, 1 / 3)
  Andromeda::close(agg)
})

test_that("minCharacterizationMean drops rare binary covariates", {
  agg <- aggregate(make_inputs(isBinary = "Y"), minCharacterizationMean = 0.5)
  bin <- dplyr::collect(agg$covariates)
  expect_true(all(bin$averageValue >= 0.5))
  expect_false(any(bin$cohortDefinitionId == 2 & bin$covariateId == 102))
  Andromeda::close(agg)
})

test_that("a cohort row with the same subject but a different start date counts separately", {
  x <- make_inputs(isBinary = "Y")
  extra <- x$covariates[x$covariates$cohortDefinitionId == 1 & x$covariates$subjectId == 10 & x$covariates$covariateId == 101, ]
  extra$cohortStartDate <- as.Date("2017-01-01")
  x$covariates <- rbind(x$covariates, extra)
  agg <- aggregate(x)
  bin <- dplyr::collect(agg$covariates)
  expect_equal(bin$sumValue[bin$cohortDefinitionId == 1 & bin$covariateId == 101], 4)
  Andromeda::close(agg)
})

test_that("the aggregated query groups by cohort row", {
  s <- createExtensionCovariateSettings(
    analysisId = 990, extensionDatabaseSchema = "ext", extensionTableName = "tbl",
    extensionFields = c("concept_id", "value"), covariateIdField = "concept_id",
    covariateValueField = "value", warnOnAnalysisIdOverlap = FALSE)
  sql <- FeatureExtractionForExtensions:::buildExtensionCovariateQuery("cohort", "subject_id", -1, s, aggregated = TRUE)
  expect_match(sql, "c.cohort_definition_id AS cohort_definition_id", fixed = TRUE)
  expect_match(sql, "GROUP BY c.cohort_definition_id, c.subject_id, c.cohort_start_date, ext.@covariate_id_field", fixed = TRUE)
})
