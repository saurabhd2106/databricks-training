variable "subscription_id" {
  type        = string
  description = "Azure subscription ID used by the azurerm provider (required in azurerm 4.x)."
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID from the Account Console. Required for account-level Unity Catalog APIs. Caller must be an Account Admin."
}

variable "azure_tenant_id" {
  type        = string
  description = "Microsoft Entra tenant ID for Azure CLI auth to the Databricks account API (az account show --query tenantId)."
}

variable "existing_metastore_id" {
  type        = string
  description = "If the account already has a metastore in this region (one-per-region limit), set its ID to skip create and reuse it. Leave empty to create a new metastore."
  default     = ""
}

variable "prefix" {
  type        = string
  description = "Short prefix for shared UC bootstrap resources (e.g. dbx-uc). Must be unique in the subscription."

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{1,14}[a-z0-9])$", var.prefix))
    error_message = "prefix must be 3-16 chars, lowercase alphanumeric/hyphen, and start/end with alphanumeric."
  }
}

variable "location" {
  type        = string
  description = "Azure region for the metastore and storage (must match participant workspace regions)."
  default     = "eastus"
}

variable "environment" {
  type        = string
  description = "Environment tag value (for example: dev, staging, prod)."
  default     = "dev"
}

variable "owner" {
  type        = string
  description = "Optional owner tag for cost attribution (e.g. your name or email)."
  default     = ""
}

variable "tags" {
  type        = map(string)
  description = "Additional tags merged into the default tag set."
  default     = {}
}

variable "storage_account_name" {
  type        = string
  description = "Optional globally unique ADLS Gen2 storage account name (3–24 lowercase alphanumeric, no hyphens). Leave empty to derive from prefix + \"metastore\"."
  default     = ""

  validation {
    condition     = var.storage_account_name == "" || can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name must be empty or 3–24 lowercase alphanumeric characters (no hyphens)."
  }
}
