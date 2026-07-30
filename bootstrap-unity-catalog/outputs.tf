output "metastore_id" {
  description = "Unity Catalog metastore ID. Pass this to each participant deploy-databricks-azure apply as metastore_id."
  value       = local.metastore_id
}

output "metastore_name" {
  description = "Display name of the Unity Catalog metastore (empty when reusing an existing metastore by ID only)."
  value       = var.existing_metastore_id == "" ? databricks_metastore.this[0].name : null
}

output "resource_group_name" {
  description = "Name of the Azure resource group holding UC bootstrap storage and access connector."
  value       = azurerm_resource_group.this.name
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account used as the metastore root."
  value       = azurerm_storage_account.this.name
}

output "storage_container_name" {
  description = "Container used as the metastore storage root."
  value       = azurerm_storage_container.this.name
}

output "access_connector_id" {
  description = "Azure resource ID of the Databricks access connector."
  value       = azurerm_databricks_access_connector.this.id
}

output "storage_root" {
  description = "abfss URI of the metastore storage root."
  value = format(
    "abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_container.this.name,
    azurerm_storage_account.this.name
  )
}
