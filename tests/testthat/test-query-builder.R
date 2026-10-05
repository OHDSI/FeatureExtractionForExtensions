make_settings <- function(...) {
  createExtensionCovariateSettings(
    analysisId = 990, extensionDatabaseSchema = "ext", extensionTableName = "tbl",
    extensionFields = c("concept_id", "value"), covariateIdField = "concept_id",
    covariateValueField = "value", warnOnAnalysisIdOverlap = FALSE, ...)
}
build <- function(settings, conceptIds = NULL) {
  FeatureExtractionForExtensions:::buildExtensionCovariateQuery("#cohort", "row_id", -1, settings, conceptIds = conceptIds)
}

test_that("covariate id encodes the analysis id", {
  sql <- build(make_settings())
  expect_match(sql, "CAST(ext.@covariate_id_field AS BIGINT) * 1000 + @analysis_id AS covariate_id", fixed = TRUE)
})

test_that("value aggregation is applied", {
  expect_match(build(make_settings()), "MAX(ext.@covariate_value_field)", fixed = TRUE)
  expect_match(build(make_settings(valueAggregation = "mean")), "AVG(ext.@covariate_value_field)", fixed = TRUE)
  expect_match(build(make_settings(valueAggregation = "count")), "COUNT(ext.@covariate_value_field)", fixed = TRUE)
  expect_error(make_settings(valueAggregation = "median"))
})

test_that("isBinary returns 1 per person and covariate", {
  sql <- build(make_settings(isBinary = TRUE))
  expect_match(sql, "1 AS covariate_value", fixed = TRUE)
  expect_false(grepl("MAX(", sql, fixed = TRUE))
})

test_that("concept ids restrict the rows", {
  sql <- build(make_settings(), conceptIds = c(2052499839, 123))
  expect_match(sql, "ext.@covariate_id_field IN (2052499839, 123)", fixed = TRUE)
  expect_false(grepl("IN \\(", build(make_settings())))
})

test_that("window uses start date only, or interval overlap with an end date field", {
  start_only <- build(make_settings(dateField = "d", startDay = -30, endDay = -1))
  expect_match(start_only, "ext.d >= DATEADD(day, -30, c.cohort_start_date)", fixed = TRUE)
  expect_match(start_only, "ext.d <= DATEADD(day, -1, c.cohort_start_date)", fixed = TRUE)
  overlap <- build(make_settings(dateField = "d", endDateField = "e", startDay = -30, endDay = -1))
  expect_match(overlap, "ext.d <= DATEADD(day, -1, c.cohort_start_date)", fixed = TRUE)
  expect_match(overlap, "COALESCE(ext.e, ext.d) >= DATEADD(day, -30, c.cohort_start_date)", fixed = TRUE)
  expect_error(make_settings(endDateField = "e"))
})
