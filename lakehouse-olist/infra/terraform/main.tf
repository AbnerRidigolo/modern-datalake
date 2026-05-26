terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  layers = ["bronze", "silver", "gold"]
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ---------------------------------------------------------------------------
# S3 buckets — uma camada por bucket (bronze / silver / gold)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "lakehouse" {
  for_each = toset(local.layers)

  bucket = "${var.project_name}-${each.key}-${var.environment}"
  tags   = merge(local.common_tags, { Layer = each.key })
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# IAM role assumível pelos jobs Spark/Glue para ler/escrever no lakehouse
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lakehouse_etl" {
  name               = "${var.project_name}-etl-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "lakehouse_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = concat(
      [for b in aws_s3_bucket.lakehouse : b.arn],
      [for b in aws_s3_bucket.lakehouse : "${b.arn}/*"],
    )
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:BatchCreatePartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "lakehouse_access" {
  name   = "${var.project_name}-access-${var.environment}"
  role   = aws_iam_role.lakehouse_etl.id
  policy = data.aws_iam_policy_document.lakehouse_access.json
}

# ---------------------------------------------------------------------------
# Glue Catalog — um database por camada
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "lakehouse" {
  for_each = toset(local.layers)

  name        = "${var.project_name}_${each.key}"
  description = "Catálogo Glue da camada ${each.key} do ${var.project_name}"
}
