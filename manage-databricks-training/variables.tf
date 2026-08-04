variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID from the Account Console."
}

variable "azure_tenant_id" {
  type        = string
  description = "Microsoft Entra tenant ID (az account show --query tenantId)."
}

variable "databricks_workspace_id" {
  type        = string
  description = "Numeric Databricks workspace ID (azurerm_databricks_workspace.this.workspace_id from deploy-databricks-azure). Used for account-level workspace assignment."
}

variable "databricks_host" {
  type        = string
  description = "HTTPS host URL for the Databricks workspace (deploy-databricks-azure output databricks_host)."
}

variable "cluster_id" {
  type        = string
  description = "All-purpose cluster ID from deploy-databricks-azure output cluster_id."
}

variable "uc_storage_root" {
  type        = string
  description = "abfss URI from bootstrap-unity-catalog storage_root. New catalogs use {uc_storage_root}/sandbox/{catalog_name}."
}

variable "entra_domain" {
  type        = string
  description = "Entra ID primary domain used to build UPNs (e.g. contoso.onmicrosoft.com)."
}

variable "training_users" {
  type = map(object({
    mail_nickname          = string
    display_name           = string
    create_entra_user      = optional(bool, true)
    user_principal_name    = optional(string)
    department             = optional(string)
    job_title              = optional(string)
    workspace_permission   = optional(string, "USER")
    allow_cluster_create   = optional(bool, false)
    allow_sql_access       = optional(bool, true)
    allow_workspace_access = optional(bool, true)
  }))
  description = "Map of training users. Keys are stable Terraform identifiers; mail_nickname + entra_domain form the UPN unless user_principal_name is set."

  validation {
    condition = alltrue([
      for _, u in var.training_users :
      contains(["USER", "ADMIN"], u.workspace_permission)
    ])
    error_message = "training_users.*.workspace_permission must be USER or ADMIN."
  }
}

variable "training_groups" {
  type = map(object({
    display_name           = string
    members                = optional(list(string), [])
    workspace_permission   = optional(string, "USER")
    allow_cluster_create   = optional(bool, false)
    allow_sql_access       = optional(bool, true)
    allow_workspace_access = optional(bool, true)
  }))
  description = "Map of training groups. members are keys from training_users."
  default     = {}

  validation {
    condition = alltrue([
      for _, g in var.training_groups :
      contains(["USER", "ADMIN"], g.workspace_permission)
    ])
    error_message = "training_groups.*.workspace_permission must be USER or ADMIN."
  }

  validation {
    condition = alltrue(flatten([
      for _, g in var.training_groups : [
        for m in g.members : contains(keys(var.training_users), m)
      ]
    ]))
    error_message = "training_groups.*.members must reference keys that exist in training_users."
  }
}

variable "catalogs" {
  type = map(object({
    comment         = optional(string, "")
    schemas         = list(string)
    grant_to_users  = optional(list(string), [])
    grant_to_groups = optional(list(string), [])
    privileges      = optional(list(string))
    force_destroy   = optional(bool, true)
  }))
  description = "On-demand Unity Catalog catalogs and schemas to create (does not manage the shared actuarial catalog)."
  default     = {}

  validation {
    condition = alltrue(flatten([
      for _, c in var.catalogs : [
        for u in c.grant_to_users : contains(keys(var.training_users), u)
      ]
    ]))
    error_message = "catalogs.*.grant_to_users must reference keys that exist in training_users."
  }

  validation {
    condition = alltrue(flatten([
      for _, c in var.catalogs : [
        for g in c.grant_to_groups : contains(keys(var.training_groups), g)
      ]
    ]))
    error_message = "catalogs.*.grant_to_groups must reference keys that exist in training_groups."
  }
}

variable "default_catalog_privileges" {
  type        = list(string)
  description = "Default UC privileges granted on each training catalog when catalogs.*.privileges is omitted."
  default = [
    "USE_CATALOG",
    "USE_SCHEMA",
    "CREATE_SCHEMA",
    "CREATE_TABLE",
    "CREATE_MATERIALIZED_VIEW",
    "CREATE_VOLUME",
    "SELECT",
    "MODIFY",
  ]
}
