variable "region" {
  description = "Region de AWS para todos los recursos"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefijo de nombres y valor de la etiqueta Project"
  type        = string
  default     = "crypto-streaming"
}

variable "product_ids" {
  description = "Pares que procesa el pipeline (para la proyeccion de particiones de Athena)"
  type        = list(string)
  default     = ["BTC-USD", "ETH-USD", "SOL-USD"]
}

variable "alert_email" {
  description = "Email que recibe las alertas de presupuesto"
  type        = string
}

variable "monthly_budget_usd" {
  description = "Presupuesto mensual: avisa al 80% real y al 100% proyectado"
  type        = number
  default     = 5
}

variable "athena_scan_limit_bytes" {
  description = "Maximo de bytes que puede escanear una consulta de Athena (control de costo)"
  type        = number
  default     = 1073741824 # 1 GB
}
