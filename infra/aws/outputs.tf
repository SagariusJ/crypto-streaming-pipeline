output "lake_bucket" {
  description = "Bucket del data lake"
  value       = aws_s3_bucket.lake.bucket
}

output "lake_uri_s3a" {
  description = "Valor de LAKE_URI para Spark"
  value       = "s3a://${aws_s3_bucket.lake.bucket}"
}

output "pipeline_role_arn" {
  description = "Rol que asume Spark (role_arn del perfil crypto-pipeline)"
  value       = aws_iam_role.pipeline.arn
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}
