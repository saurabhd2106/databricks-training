resource "databricks_catalog" "training" {
  for_each = var.catalogs

  name          = each.key
  comment       = each.value.comment
  storage_root  = "${local.sandbox_storage_root}/${each.key}"
  force_destroy = each.value.force_destroy

  depends_on = [
    databricks_mws_permission_assignment.user,
    databricks_mws_permission_assignment.group,
  ]
}

resource "databricks_schema" "training" {
  for_each = local.schema_map

  catalog_name = databricks_catalog.training[each.value.catalog_key].name
  name         = each.value.schema_name
  comment      = "Managed by Terraform — training schema ${each.value.schema_name}."

  depends_on = [databricks_catalog.training]
}

resource "databricks_grants" "training_catalog" {
  for_each = {
    for key, c in var.catalogs :
    key => c if length(c.grant_to_users) + length(c.grant_to_groups) > 0
  }

  catalog = databricks_catalog.training[each.key].name

  dynamic "grant" {
    for_each = toset(each.value.grant_to_users)
    content {
      principal  = local.user_upns[grant.key]
      privileges = local.catalog_privileges[each.key]
    }
  }

  dynamic "grant" {
    for_each = toset(each.value.grant_to_groups)
    content {
      principal  = var.training_groups[grant.key].display_name
      privileges = local.catalog_privileges[each.key]
    }
  }

  depends_on = [
    databricks_schema.training,
    databricks_user.training,
    databricks_group.training,
  ]
}
