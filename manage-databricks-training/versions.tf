terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.122"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}
