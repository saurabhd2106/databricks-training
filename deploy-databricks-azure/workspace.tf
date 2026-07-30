resource "azurerm_databricks_workspace" "this" {
  name                        = local.workspace_name
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = "${local.prefix}-dbx-managed-rg"

  custom_parameters {
    no_public_ip                                         = true
    virtual_network_id                                   = azurerm_virtual_network.this.id
    public_subnet_name                                   = azurerm_subnet.public.name
    private_subnet_name                                  = azurerm_subnet.private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }

  tags = local.tags
}

# Brand-new workspaces can briefly report WorkerEnv not found in central if a
# cluster is created immediately; wait for control-plane registration.
resource "time_sleep" "workspace_ready" {
  create_duration = "90s"

  depends_on = [azurerm_databricks_workspace.this]
}
