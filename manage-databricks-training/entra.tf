resource "random_password" "training_user" {
  for_each = local.users_to_create

  length           = 24
  special          = true
  override_special = "!@#%^*-_=+"
  min_lower        = 1
  min_upper        = 1
  min_numeric      = 1
  min_special      = 1
}

resource "azuread_user" "training" {
  for_each = local.users_to_create

  user_principal_name   = local.user_upns[each.key]
  display_name          = each.value.display_name
  mail_nickname         = each.value.mail_nickname
  password              = random_password.training_user[each.key].result
  force_password_change = true
  account_enabled       = true
  department            = each.value.department
  job_title             = each.value.job_title
}

data "azuread_user" "existing" {
  for_each = local.users_existing

  user_principal_name = local.user_upns[each.key]
}

locals {
  entra_object_ids = merge(
    { for key, u in azuread_user.training : key => u.object_id },
    { for key, u in data.azuread_user.existing : key => u.object_id },
  )
}

resource "azuread_group" "training" {
  for_each = var.training_groups

  display_name     = each.value.display_name
  security_enabled = true
  mail_enabled     = false
}

resource "azuread_group_member" "training" {
  for_each = local.group_memberships

  group_object_id  = azuread_group.training[each.value.group_key].object_id
  member_object_id = local.entra_object_ids[each.value.user_key]
}
