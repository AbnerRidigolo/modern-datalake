output "bucket_names" {
  description = "Nomes dos buckets S3 por camada do lakehouse"
  value       = { for k, b in aws_s3_bucket.lakehouse : k => b.id }
}

output "bucket_arns" {
  description = "ARNs dos buckets S3 por camada"
  value       = { for k, b in aws_s3_bucket.lakehouse : k => b.arn }
}

output "etl_role_arn" {
  description = "ARN da IAM role usada pelos jobs de ETL (Spark/Glue)"
  value       = aws_iam_role.lakehouse_etl.arn
}

output "glue_databases" {
  description = "Nomes dos databases do Glue Catalog por camada"
  value       = { for k, db in aws_glue_catalog_database.lakehouse : k => db.name }
}
