provider "azuread" {
  tenant_id = var.azure_tenant_id
}

# Workspace-level provider for catalogs, schemas, grants, and entitlements.
provider "databricks" {
  host            = var.databricks_host
  auth_type       = "azure-cli"
  azure_tenant_id = var.azure_tenant_id
}

# Account-level provider for users, groups, and workspace assignment.
# Requires Databricks ACCOUNT admin (or equivalent).
provider "databricks" {
  alias           = "accounts"
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.databricks_account_id
  auth_type       = "azure-cli"
  azure_tenant_id = var.azure_tenant_id
}
