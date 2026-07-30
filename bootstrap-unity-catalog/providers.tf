provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# Account-level provider — requires Databricks ACCOUNT admin (not workspace admin).
# Get account_id from the Databricks Account Console.
# auth_type azure-cli + tenant avoids azure_tenant_id=common auth failures.
provider "databricks" {
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.databricks_account_id
  auth_type       = "azure-cli"
  azure_tenant_id = var.azure_tenant_id
}
