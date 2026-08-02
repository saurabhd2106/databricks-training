resource "databricks_metastore_assignment" "this" {
  provider     = databricks.accounts
  metastore_id = var.metastore_id
  workspace_id = azurerm_databricks_workspace.this.workspace_id

  depends_on = [time_sleep.workspace_ready]
}

locals {
  uc_storage_root_trimmed = trimsuffix(var.uc_storage_root, "/")
  actuarial_storage_root  = "${local.uc_storage_root_trimmed}/actuarial"
  # Distinct path from actuarial so UC external locations do not overlap.
  sandbox_storage_root = "${local.uc_storage_root_trimmed}/sandbox"
}

# Catalog-level managed storage is required when the metastore has no storage_root
# (typical for auto-created Azure metastores with Default Storage enabled).
resource "databricks_storage_credential" "actuarial" {
  count = var.create_actuarial_catalog ? 1 : 0

  name = "actuarial-uc-credential"
  azure_managed_identity {
    access_connector_id = var.uc_access_connector_id
  }
  comment = "Managed by Terraform — bootstrap access connector for actuarial catalog storage."

  lifecycle {
    precondition {
      condition     = var.uc_access_connector_id != ""
      error_message = "uc_access_connector_id is required when create_actuarial_catalog is true (copy access_connector_id from bootstrap-unity-catalog output)."
    }
  }

  depends_on = [
    databricks_metastore_assignment.this,
    time_sleep.workspace_ready,
  ]
}

resource "databricks_external_location" "actuarial" {
  count = var.create_actuarial_catalog ? 1 : 0

  name            = "actuarial-uc-location"
  # Scoped to /actuarial (not container root) so sandbox-uc-location can coexist.
  url             = local.actuarial_storage_root
  credential_name = databricks_storage_credential.actuarial[0].id
  comment         = "Managed by Terraform — storage for shared actuarial catalog."
  # Required when narrowing URL on an already-applied location that backs the actuarial catalog.
  # Safe here: catalog storage_root is already under /actuarial.
  force_update    = true

  lifecycle {
    precondition {
      condition     = var.uc_storage_root != ""
      error_message = "uc_storage_root is required when create_actuarial_catalog is true (copy storage_root from bootstrap-unity-catalog output)."
    }
  }

  depends_on = [
    databricks_metastore_assignment.this,
    databricks_storage_credential.actuarial,
  ]
}

resource "databricks_external_location" "sandbox" {
  count = var.create_actuarial_catalog ? 1 : 0

  name            = "sandbox-uc-location"
  url             = local.sandbox_storage_root
  credential_name = databricks_storage_credential.actuarial[0].id
  comment         = "Managed by Terraform — training sandbox for ad hoc catalogs and external tables."

  lifecycle {
    precondition {
      condition     = var.uc_storage_root != ""
      error_message = "uc_storage_root is required when create_actuarial_catalog is true (copy storage_root from bootstrap-unity-catalog output)."
    }
  }

  # Actuarial location must be narrowed off the container root first (no overlapping URLs).
  depends_on = [
    databricks_metastore_assignment.this,
    databricks_storage_credential.actuarial,
    databricks_external_location.actuarial,
  ]
}

resource "databricks_grants" "actuarial_credential" {
  count = var.create_actuarial_catalog ? 1 : 0

  storage_credential = databricks_storage_credential.actuarial[0].id

  grant {
    principal  = "account users"
    privileges = ["CREATE_EXTERNAL_TABLE"]
  }
}

resource "databricks_grants" "actuarial_location" {
  count = var.create_actuarial_catalog ? 1 : 0

  external_location = databricks_external_location.actuarial[0].id

  grant {
    principal = "account users"
    privileges = [
      "CREATE_EXTERNAL_TABLE",
      "CREATE_MANAGED_STORAGE",
      "READ_FILES",
      "WRITE_FILES",
    ]
  }
}

resource "databricks_grants" "sandbox_location" {
  count = var.create_actuarial_catalog ? 1 : 0

  external_location = databricks_external_location.sandbox[0].id

  grant {
    principal = "account users"
    privileges = [
      "CREATE_EXTERNAL_TABLE",
      "CREATE_MANAGED_STORAGE",
      "READ_FILES",
      "WRITE_FILES",
    ]
  }
}

resource "databricks_catalog" "actuarial" {
  count = var.create_actuarial_catalog ? 1 : 0

  name          = "actuarial"
  comment       = "Shared actuarial medallion catalog (bronze / silver / gold)."
  storage_root  = local.actuarial_storage_root
  force_destroy = true

  depends_on = [
    databricks_metastore_assignment.this,
    databricks_external_location.actuarial,
    databricks_grants.actuarial_location,
    time_sleep.workspace_ready,
  ]
}

resource "databricks_schema" "bronze" {
  count = var.create_actuarial_catalog ? 1 : 0

  catalog_name = databricks_catalog.actuarial[0].name
  name         = "bronze"
  comment      = "Bronze (raw) actuarial landing zone."

  depends_on = [databricks_catalog.actuarial]
}

resource "databricks_schema" "silver" {
  count = var.create_actuarial_catalog ? 1 : 0

  catalog_name = databricks_catalog.actuarial[0].name
  name         = "silver"
  comment      = "Silver (cleaned) actuarial zone."

  depends_on = [databricks_catalog.actuarial]
}

resource "databricks_schema" "gold" {
  count = var.create_actuarial_catalog ? 1 : 0

  catalog_name = databricks_catalog.actuarial[0].name
  name         = "gold"
  comment      = "Gold (curated) actuarial zone."

  depends_on = [databricks_catalog.actuarial]
}

resource "databricks_grants" "actuarial" {
  count = var.create_actuarial_catalog ? 1 : 0

  catalog = databricks_catalog.actuarial[0].name

  grant {
    principal = "account users"
    privileges = [
      "USE_CATALOG",
      "USE_SCHEMA",
      "CREATE_SCHEMA",
      "CREATE_TABLE",
      # Required for serverless Lakeflow @dp.table publishes (materialized views).
      # CREATE_STREAMING_TABLE is not applicable on this metastore privilege version (1.0).
      "CREATE_MATERIALIZED_VIEW",
      "CREATE_VOLUME",
      "SELECT",
      "MODIFY",
    ]
  }

  depends_on = [
    databricks_schema.bronze,
    databricks_schema.silver,
    databricks_schema.gold,
  ]
}
