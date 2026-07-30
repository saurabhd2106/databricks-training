locals {
  prefix         = var.prefix
  workspace_name = "${local.prefix}-workspace"
  resource_group = "${local.prefix}-rg"
  cluster_name   = var.cluster_name != "" ? var.cluster_name : "${local.prefix}-cluster"

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
