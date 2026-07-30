data "databricks_spark_version" "lts" {
  long_term_support = true
  latest            = true

  depends_on = [time_sleep.workspace_ready]
}

data "databricks_node_type" "default" {
  # Prefer the smallest local-disk SKU; category filters often pick large Ddsv5 sizes that hit quota.
  local_disk = true

  depends_on = [time_sleep.workspace_ready]
}

locals {
  node_type_id        = var.node_type_id != "" ? var.node_type_id : data.databricks_node_type.default.id
  driver_node_type_id = var.driver_node_type_id != "" ? var.driver_node_type_id : local.node_type_id
}

resource "databricks_cluster_policy" "this" {
  name = "${local.prefix}-standard-policy"

  depends_on = [time_sleep.workspace_ready]

  # Single-node cluster: no autoscale worker constraints (those fight is_single_node).
  definition = jsonencode({
    "spark_version" : {
      "type" : "unlimited",
      "defaultValue" : "auto:latest-lts"
    },
    "node_type_id" : {
      "type" : "unlimited",
      "defaultValue" : local.node_type_id
    },
    "driver_node_type_id" : {
      "type" : "unlimited",
      "defaultValue" : local.driver_node_type_id
    },
    "data_security_mode" : {
      "type" : "fixed",
      "value" : "USER_ISOLATION"
    },
    "autotermination_minutes" : {
      "type" : "range",
      "minValue" : 10,
      "maxValue" : 60,
      "defaultValue" : var.autotermination_minutes
    }
  })
}

resource "databricks_cluster" "this" {
  cluster_name            = local.cluster_name
  spark_version           = data.databricks_spark_version.lts.id
  node_type_id            = local.node_type_id
  driver_node_type_id     = local.driver_node_type_id
  policy_id               = databricks_cluster_policy.this.id
  data_security_mode      = "USER_ISOLATION"
  autotermination_minutes = var.autotermination_minutes

  # One VM only — avoids eastus stockouts when driver+worker cannot both be acquired.
  is_single_node = true
  kind           = "CLASSIC_PREVIEW"

  # Explicitly disable flexible fallbacks (empty list). Workspace auto-flex was
  # injecting incompatible alternates that fail API validation.
  driver_node_type_flexibility {
    alternate_node_type_ids = []
  }

  custom_tags = local.tags

  timeouts {
    create = "45m"
  }

  depends_on = [time_sleep.workspace_ready]
}
