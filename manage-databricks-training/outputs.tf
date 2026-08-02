output "training_user_upns" {
  description = "Map of training user keys to user principal names."
  value       = local.user_upns
}

output "training_user_passwords" {
  description = "Initial passwords for Entra users created by this stack. Users must change password on first login. Empty for users with create_entra_user = false."
  value = {
    for key in keys(local.users_to_create) :
    key => random_password.training_user[key].result
  }
  sensitive = true
}

output "training_group_names" {
  description = "Map of training group keys to Entra/Databricks display names."
  value = {
    for key, g in var.training_groups : key => g.display_name
  }
}

output "training_catalog_names" {
  description = "Names of catalogs created by this stack."
  value       = [for c in databricks_catalog.training : c.name]
}

output "training_catalog_storage_roots" {
  description = "Managed storage roots for each training catalog under the sandbox path."
  value = {
    for key, c in databricks_catalog.training : key => c.storage_root
  }
}

output "databricks_host" {
  description = "Workspace URL trainees should open."
  value       = var.databricks_host
}
