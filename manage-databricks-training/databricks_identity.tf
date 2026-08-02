resource "databricks_user" "training" {
  for_each = var.training_users
  provider = databricks.accounts

  user_name    = local.user_upns[each.key]
  display_name = each.value.display_name
  force        = true

  depends_on = [
    azuread_user.training,
    data.azuread_user.existing,
  ]
}

resource "databricks_group" "training" {
  for_each = var.training_groups
  provider = databricks.accounts

  display_name = each.value.display_name
  force        = true
}

resource "databricks_group_member" "training" {
  for_each = local.group_memberships
  provider = databricks.accounts

  group_id  = databricks_group.training[each.value.group_key].id
  member_id = databricks_user.training[each.value.user_key].id
}

resource "databricks_mws_permission_assignment" "user" {
  for_each = var.training_users
  provider = databricks.accounts

  workspace_id = var.databricks_workspace_id
  principal_id = databricks_user.training[each.key].id
  permissions  = [each.value.workspace_permission]
}

resource "databricks_mws_permission_assignment" "group" {
  for_each = var.training_groups
  provider = databricks.accounts

  workspace_id = var.databricks_workspace_id
  principal_id = databricks_group.training[each.key].id
  permissions  = [each.value.workspace_permission]
}

resource "databricks_entitlements" "user" {
  for_each = var.training_users

  user_id                    = databricks_user.training[each.key].id
  workspace_access           = each.value.allow_workspace_access
  databricks_sql_access      = each.value.allow_sql_access
  allow_cluster_create       = each.value.allow_cluster_create
  allow_instance_pool_create = false

  depends_on = [databricks_mws_permission_assignment.user]
}

resource "databricks_entitlements" "group" {
  for_each = var.training_groups

  group_id                   = databricks_group.training[each.key].id
  workspace_access           = each.value.allow_workspace_access
  databricks_sql_access      = each.value.allow_sql_access
  allow_cluster_create       = each.value.allow_cluster_create
  allow_instance_pool_create = false

  depends_on = [databricks_mws_permission_assignment.group]
}
