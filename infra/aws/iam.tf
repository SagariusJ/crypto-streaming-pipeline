# Rol que usa Spark para escribir en el lake. Minimo privilegio: solo puede
# leer/escribir las carpetas del pipeline en ESTE bucket, nada mas en la cuenta.
#
# No hay access keys permanentes: tu sesion de IAM Identity Center asume este rol
# y recibe credenciales temporales (perfil "crypto-pipeline" en ~/.aws/config).

locals {
  pipeline_prefixes = ["trades", "trades_1m", "quarantine", "_smoke_test"]
}

resource "aws_iam_role" "pipeline" {
  name        = "${var.project}-pipeline"
  description = "Spark streaming job: escribe Parquet en el data lake"
  # Al asumirlo desde la sesion SSO (role chaining) AWS limita cada sesion a 1 hora,
  # pero el SDK de AWS la renueva solo mientras la sesion SSO siga vigente.
  max_session_duration = 43200

  # Quien puede asumir el rol: solo los roles de IAM Identity Center con
  # AdministratorAccess de esta cuenta (es decir, tu usuario).
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = {
        ArnLike = {
          "aws:PrincipalArn" = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/aws-reserved/sso.amazonaws.com/*AWSReservedSSO_AdministratorAccess_*"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "pipeline_s3" {
  name = "lake-read-write"
  role = aws_iam_role.pipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListOnlyPipelinePrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
        Resource = aws_s3_bucket.lake.arn
        Condition = {
          StringLike = {
            "s3:prefix" = flatten([for p in local.pipeline_prefixes : [p, "${p}/*"]])
          }
        }
      },
      {
        Sid    = "ReadWritePipelineObjects"
        Effect = "Allow"
        # Delete y multipart los necesita S3A (archivos temporales y subidas por partes)
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = [for p in local.pipeline_prefixes : "${aws_s3_bucket.lake.arn}/${p}/*"]
      },
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = "s3:GetBucketLocation"
        Resource = aws_s3_bucket.lake.arn
      },
    ]
  })
}
