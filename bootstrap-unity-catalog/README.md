# Unity Catalog Metastore Bootstrap (one-time)

Creates the **shared** Unity Catalog metastore for an Azure Databricks account/region:
ADLS Gen2 root storage, Databricks access connector (system-assigned MI), RBAC, and
the account-level `databricks_metastore`.

> **Account Admin required.** This module talks to
> `https://accounts.azuredatabricks.net` and needs a Databricks **Account Admin**,
> not merely a workspace admin. Obtain your **Databricks account ID** from the
> Account Console (Account settings → Account ID) and set `databricks_account_id`.

Do **not** run this from every participant. A metastore is one-per-region-per-account;
participant workspaces attach via `deploy-databricks-azure` using the `metastore_id` output.

## Prerequisites

- Terraform **>= 1.5**
- Azure CLI (`az`) authenticated with rights to create RG, storage, access connector, and RBAC
- Databricks **Account Admin** for the target account
- Azure Databricks resource provider registered: `Microsoft.Databricks`

```bash
az login
az account set --subscription "<subscription-id>"
```

## Configure

```bash
cd bootstrap-unity-catalog
cp terraform.tfvars.example terraform.tfvars
```

Set at least:

- `subscription_id`
- `databricks_account_id` — from Account Console (Account Admin required)
- `prefix` — e.g. `dbx-uc` (shared bootstrap; not per-person)
- `location` — must match participant workspace regions

## Deploy (once)

```bash
terraform init
terraform plan
terraform apply
```

Copy the `metastore_id` output into each participant's `deploy-databricks-azure/terraform.tfvars`. The one participant who creates the actuarial catalog also needs `storage_root` → `uc_storage_root` and `access_connector_id` → `uc_access_connector_id` (catalog-level managed storage when the region metastore has no storage root).

## Outputs

| Output | Description |
|--------|-------------|
| `metastore_id` | Pass to participant module as `metastore_id` |
| `storage_root` | abfss URI of the metastore root |
| `access_connector_id` | Access connector used as default metastore data access |

## Destroy

```bash
terraform destroy
```

Destroying the metastore affects every workspace assigned to it. Detach workspaces first.
