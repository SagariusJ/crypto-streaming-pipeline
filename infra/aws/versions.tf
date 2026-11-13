terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }

  # State local (infra/aws/terraform.tfstate, ignorado por git). Para un equipo
  # se usaria un backend remoto (S3 + bloqueo), pero para un proyecto personal basta.
}

provider "aws" {
  region = var.region

  # Todas las recursos quedan etiquetados: se pueden filtrar en Billing y en la consola.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Repo      = "github.com/SagariusJ/crypto-streaming-pipeline"
    }
  }
}

data "aws_caller_identity" "current" {}
