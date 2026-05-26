variable "aws_region" {
  description = "Região AWS onde os recursos do lakehouse serão provisionados"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefixo usado para nomear buckets, roles e databases"
  type        = string
  default     = "lakehouse-olist"
}

variable "environment" {
  description = "Ambiente lógico (dev, staging, prod) usado como sufixo de nomes"
  type        = string
  default     = "dev"
}
