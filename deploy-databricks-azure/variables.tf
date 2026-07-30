variable "subscription_id" {
  type        = string
  description = "Azure subscription ID used by the azurerm provider (required in azurerm 4.x)."
}

variable "prefix" {
  type        = string
  description = "Unique short prefix per deployer (e.g. dbx-alice). Must be unique in the subscription."

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{1,14}[a-z0-9])$", var.prefix))
    error_message = "prefix must be 3-16 chars, lowercase alphanumeric/hyphen, and start/end with alphanumeric."
  }
}

variable "location" {
  type        = string
  description = "Azure region for all resources."
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

variable "vnet_address_space" {
  type        = string
  description = "CIDR for the customer virtual network used for Databricks VNet injection."
  default     = "10.30.0.0/16"
}

variable "public_subnet_prefix" {
  type        = string
  description = "CIDR for the Databricks public (host) subnet."
  default     = "10.30.1.0/24"
}

variable "private_subnet_prefix" {
  type        = string
  description = "CIDR for the Databricks private (container) subnet."
  default     = "10.30.2.0/24"
}

variable "cluster_name" {
  type        = string
  description = "Name of the all-purpose Databricks cluster. Leave empty to use \"<prefix>-cluster\"."
  default     = ""
}

variable "node_type_id" {
  type        = string
  description = "Node type for the single-node cluster. Defaults to Standard_E4ads_v7 (Databricks-supported v7; E4ds_v7 is not). Avoid EDSv5 (often 0 quota) and DS3_v2 (stockouts)."
  default     = "Standard_E4ads_v7"
}

variable "driver_node_type_id" {
  type        = string
  description = "Driver node type. Leave empty to match node_type_id. On single-node clusters this is the only VM."
  default     = ""
}

variable "min_workers" {
  type        = number
  description = "Deprecated / unused: the provisioned cluster is single-node (no workers). Kept so existing terraform.tfvars still parse."
  default     = 1
}

variable "max_workers" {
  type        = number
  description = "Deprecated / unused: the provisioned cluster is single-node (no workers). Kept so existing terraform.tfvars still parse."
  default     = 1
}

variable "autotermination_minutes" {
  type        = number
  description = "Minutes of inactivity before the cluster auto-terminates. Keep at 60 or below."
  default     = 30

  validation {
    condition     = var.autotermination_minutes > 0 && var.autotermination_minutes <= 60
    error_message = "autotermination_minutes must be between 1 and 60."
  }
}

variable "tags" {
  type        = map(string)
  description = "Additional tags merged into the default tag set."
  default     = {}
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID from the Account Console. Required for metastore assignment (account-level API)."
}

variable "azure_tenant_id" {
  type        = string
  description = "Microsoft Entra tenant ID for Azure CLI auth to the Databricks account API (az account show --query tenantId)."
}

variable "metastore_id" {
  type        = string
  description = "Unity Catalog metastore ID from the bootstrap-unity-catalog module output. Assigns this workspace to the shared metastore."
}

variable "create_actuarial_catalog" {
  type        = bool
  description = "When true, create the shared actuarial catalog (bronze/silver/gold) and grants. Set true for exactly one participant to avoid create races."
  default     = false
}

variable "uc_storage_root" {
  type        = string
  description = "abfss URI from bootstrap-unity-catalog storage_root output. Required when create_actuarial_catalog is true (auto-created metastores have no metastore-level storage)."
  default     = ""
}

variable "uc_access_connector_id" {
  type        = string
  description = "Azure resource ID of the Databricks access connector from bootstrap-unity-catalog access_connector_id output. Required when create_actuarial_catalog is true."
  default     = ""
}
