locals {
  prefix         = var.prefix
  resource_group = "${local.prefix}-rg"
  # Storage account names: 3–24 chars, lowercase alphanumeric only (no hyphens); globally unique in Azure.
  storage_account_name = var.storage_account_name != "" ? var.storage_account_name : substr(replace("${local.prefix}metastore", "-", ""), 0, 24)
  container_name       = "metastore"
  access_connector     = "${local.prefix}-access-connector"
  metastore_name = "${local.prefix}-metastore"
  # one() is null when count=0 (reusing existing_metastore_id).
  metastore_id = coalesce(one(databricks_metastore.this[*].id), var.existing_metastore_id)

  tags = merge(
    {
      Environment   = var.environment
      Project       = "databricks-azure"
      ProvisionedBy = "terraform"
    },
    var.owner != "" ? { Owner = var.owner } : {},
    var.tags
  )
}
