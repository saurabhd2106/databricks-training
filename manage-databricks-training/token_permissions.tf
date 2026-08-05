resource "databricks_permissions" "token_usage" {
  authorization = "tokens"

  dynamic "access_control" {
    for_each = var.training_users
    content {
      user_name        = local.user_upns[access_control.key]
      permission_level = "CAN_USE"
    }
  }

  dynamic "access_control" {
    for_each = var.training_groups
    content {
      group_name       = databricks_group.training[access_control.key].display_name
      permission_level = "CAN_USE"
    }
  }

  depends_on = [
    databricks_mws_permission_assignment.user,
    databricks_mws_permission_assignment.group,
    databricks_entitlements.user,
    databricks_entitlements.group,
  ]
}
