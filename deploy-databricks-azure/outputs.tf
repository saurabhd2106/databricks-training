output "resource_group_name" {
  description = "Name of the Azure resource group."
  value       = azurerm_resource_group.this.name
}

output "databricks_workspace_id" {
  description = "Azure resource ID of the Databricks workspace."
  value       = azurerm_databricks_workspace.this.id
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace (without https://)."
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_host" {
  description = "HTTPS host URL for the Databricks workspace."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "cluster_id" {
  description = "ID of the provisioned all-purpose cluster."
  value       = databricks_cluster.this.id
}

output "cluster_name" {
  description = "Name of the provisioned all-purpose cluster."
  value       = databricks_cluster.this.cluster_name
}

output "cluster_policy_id" {
  description = "ID of the Databricks cluster policy."
  value       = databricks_cluster_policy.this.id
}

output "sandbox_external_location_name" {
  description = "Name of the training sandbox external location (set when create_actuarial_catalog is true). Learners use this for ad hoc catalogs and external tables."
  value       = var.create_actuarial_catalog ? databricks_external_location.sandbox[0].name : null
}

output "sandbox_external_location_path" {
  description = "abfss base path of the training sandbox external location. Learners append /<username>/... at runtime."
  value       = var.create_actuarial_catalog ? local.sandbox_storage_root : null
}
