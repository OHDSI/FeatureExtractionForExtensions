test_that("quantileType1Index matches stats::quantile(type = 1)", {
  set.seed(1)
  probs <- c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1)
  for (n in c(1, 2, 3, 5, 10, 11, 100, 2717, 3099)) {
    x <- sort(sample(1:50, n, replace = TRUE) + runif(n))
    idx <- FeatureExtractionForExtensions:::quantileType1Index(n, probs)
    expect_equal(x[idx], unname(stats::quantile(x, probs, type = 1)), info = paste("n =", n))
  }
})

# Compute the database summaries in R from person-level rows (what the server would return)
simulate_server <- function(rows, populationSize, missingMeansZero) {
  stats <- do.call(rbind, lapply(split(rows, list(rows$cohortDefinitionId, rows$covariateId), drop = TRUE), function(d) {
    data.frame(cohortDefinitionId = d$cohortDefinitionId[1], covariateId = d$covariateId[1], n = nrow(d),
               sumValue = sum(d$covariateValue), sumSq = sum(d$covariateValue^2))
  }))
  stats <- stats[order(stats$cohortDefinitionId, stats$covariateId), ]
  ranks <- FeatureExtractionForExtensions:::planPercentileRanks(stats, populationSize, missingMeansZero)
  rankValues <- do.call(rbind, lapply(seq_len(nrow(ranks)), function(i) {
    d <- rows[rows$cohortDefinitionId == ranks$cohortDefinitionId[i] & rows$covariateId == ranks$covariateId[i], ]
    data.frame(cohortDefinitionId = ranks$cohortDefinitionId[i], covariateId = ranks$covariateId[i], rn = ranks$rn[i],
               covariateValue = sort(d$covariateValue)[ranks$rn[i]])
  }))
  FeatureExtractionForExtensions:::assembleContinuousStatistics(stats, rankValues, populationSize, missingMeansZero)
}

test_that("server-side assembly equals FeatureExtraction::aggregateCovariates", {
  set.seed(42)
  populationSize <- c("1" = 120, "2" = 80)
  rows <- do.call(rbind, lapply(c(1, 2), function(cohort) {
    pop <- populationSize[[as.character(cohort)]]
    do.call(rbind, lapply(c(101, 102), function(cov) {
      present <- sample.int(pop, if (cov == 101) pop else round(pop * 0.6))
      data.frame(cohortDefinitionId = cohort, subjectId = present, cohortStartDate = as.Date("2016-01-01"),
                 covariateId = cov, covariateValue = round(rexp(length(present), 0.2), 3) + (cov == 102) * 5)
    }))
  }))
  for (missingMeansZero in c(FALSE, TRUE)) {
    analysisRef <- data.frame(analysisId = 1L, analysisName = "x", domainId = "", startDay = NA_integer_, endDay = NA_integer_,
                              isBinary = "N", missingMeansZero = ifelse(missingMeansZero, "Y", "N"), stringsAsFactors = FALSE)
    covariateRef <- data.frame(covariateId = c(101, 102), covariateName = c("a", "b"), analysisId = 1L,
                               conceptId = c(101L, 102L), valueAsConceptId = NA_integer_, collisions = NA_integer_)
    reference <- FeatureExtractionForExtensions:::aggregateExtensionCovariates(rows, covariateRef, analysisRef, populationSize)
    expected <- dplyr::collect(reference$covariatesContinuous)
    expected <- as.data.frame(expected[order(expected$cohortDefinitionId, expected$covariateId), ])
    rownames(expected) <- NULL
    actual <- simulate_server(rows, populationSize, missingMeansZero)
    actual <- as.data.frame(actual[order(actual$cohortDefinitionId, actual$covariateId), colnames(expected)])
    rownames(actual) <- NULL
    expect_equal(actual, expected, tolerance = 1e-9, info = paste("missingMeansZero =", missingMeansZero))
    Andromeda::close(reference)
  }
})
