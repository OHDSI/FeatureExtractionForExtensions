-- SQL template for retrieving covariate reference information from extension tables

SELECT
  ref.@covariate_id_field * 1000 + @analysis_id AS covariate_id,
  {@covariate_name_field != ''} ? {
    ref.@covariate_name_field AS covariate_name
  } : {
    CAST(ref.@covariate_id_field AS VARCHAR(255)) AS covariate_name
  },
  @analysis_id AS analysis_id,
  0 AS concept_id
FROM @extension_database_schema.@covariate_ref_table ref
{@has_filter} ? {
WHERE ref.@covariate_id_field IN (@covariate_id_filter)
}
ORDER BY ref.@covariate_id_field
;
