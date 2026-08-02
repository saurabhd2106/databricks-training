locals {
  uc_storage_root_trimmed = trimsuffix(var.uc_storage_root, "/")
  sandbox_storage_root    = "${local.uc_storage_root_trimmed}/sandbox"

  user_upns = {
    for key, u in var.training_users :
    key => coalesce(u.user_principal_name, "${u.mail_nickname}@${var.entra_domain}")
  }

  users_to_create = {
    for key, u in var.training_users : key => u if u.create_entra_user
  }

  users_existing = {
    for key, u in var.training_users : key => u if !u.create_entra_user
  }

  # Flatten group membership: "group_key:user_key" => { group_key, user_key }
  group_memberships = merge([
    for gkey, g in var.training_groups : {
      for ukey in g.members :
      "${gkey}:${ukey}" => {
        group_key = gkey
        user_key  = ukey
      }
    }
  ]...)

  # Flatten schemas: "catalog_key.schema_name" => { catalog_key, schema_name }
  schema_map = merge([
    for ckey, c in var.catalogs : {
      for schema in c.schemas :
      "${ckey}.${schema}" => {
        catalog_key = ckey
        schema_name = schema
      }
    }
  ]...)

  catalog_privileges = {
    for ckey, c in var.catalogs :
    ckey => coalesce(c.privileges, var.default_catalog_privileges)
  }
}
