# Bucket del data lake. El nombre incluye el ID de la cuenta porque los nombres
# de bucket son unicos a nivel mundial.
resource "aws_s3_bucket" "lake" {
  bucket = "${var.project}-lake-${data.aws_caller_identity.current.account_id}"

  # Proyecto de portafolio: "terraform destroy" debe poder borrarlo aunque tenga datos.
  # En produccion esto iria en false para evitar borrados accidentales.
  force_destroy = true
}

# Nada de este bucket puede ser publico, aunque alguien se equivoque con una politica.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Sin ACLs: los permisos se controlan solo con IAM y politicas de bucket.
resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Cifrado en reposo con claves administradas por S3 (sin costo extra).
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Spark sube archivos por partes (multipart). Si el job se cae a la mitad, las
  # partes huerfanas cuestan almacenamiento y no aparecen al listar el bucket.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Los resultados de Athena solo sirven unos dias.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

# Rechaza cualquier acceso que no use HTTPS.
resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.lake]
}
