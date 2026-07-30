provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

provider "databricks" {
  host = azurerm_databricks_workspace.this.workspace_url
}

# Account-level provider for metastore assignment.
# Requires Databricks ACCOUNT admin (or equivalent) and account_id from Account Console.
provider "databricks" {
  alias           = "accounts"
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.databricks_account_id
  auth_type       = "azure-cli"
  azure_tenant_id = var.azure_tenant_id
}
