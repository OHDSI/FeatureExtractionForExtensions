-- Generic SQL template for extracting covariates from extension tables
-- This template can be customized for different extension table structures or formats

SELECT
  @row_id_field AS row_id,
  ext.@covariate_id_field * 1000 + @analysis_id AS covariate_id,
  {@covariate_value_field != ''} ? {
    CAST(ext.@covariate_value_field AS FLOAT) AS covariate_value
  } : {
    1.0 AS covariate_value
  }
FROM @cohort_table c
INNER JOIN @extension_database_schema.@extension_table ext
  ON c.subject_id = ext.@join_field
WHERE 1 = 1
{@cohort_ids != -1} ? {
  AND c.cohort_definition_id IN (@cohort_ids)
}
{@has_date_field} ? {
  AND ext.@date_field >= DATEADD(day, @start_day, c.cohort_start_date)
  AND ext.@date_field <= DATEADD(day, @end_day, c.cohort_start_date)
}
{@additional_where_clause != ''} ? {
  AND @additional_where_clause
}
;
