# Create only when the region does not already have a metastore (one per region per account).
# Many Azure accounts already have an auto-created metastore (e.g. metastore_azure_eastus);
# set existing_metastore_id in that case.
resource "databricks_metastore" "this" {
  count = var.existing_metastore_id == "" ? 1 : 0

  name = local.metastore_name
  storage_root = format(
    "abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_container.this.name,
    azurerm_storage_account.this.name
  )
  region        = var.location
  force_destroy = true

  depends_on = [azurerm_role_assignment.metastore_blob_contributor]
}

# Binds the access connector MI as the default credential for metastore managed storage.
resource "databricks_metastore_data_access" "this" {
  metastore_id = local.metastore_id
  name         = "${local.prefix}-mi-dac"
  is_default   = true

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }

  depends_on = [
    azurerm_role_assignment.metastore_blob_contributor,
    databricks_metastore.this,
  ]
}
