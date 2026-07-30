terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.81"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.122"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}
