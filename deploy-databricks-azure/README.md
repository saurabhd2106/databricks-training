# Azure Databricks Cluster (Terraform)

Provisions a **Premium** Azure Databricks workspace with customer VNet injection (Secure Cluster Connectivity), a **single-node** all-purpose cluster with a governance policy, and Unity Catalog workspace assignment to a shared metastore.

## Architecture

| Layer | Provider | Resources |
|-------|----------|-----------|
| Azure plane | `hashicorp/azurerm` (~> 4.81) | Resource group, VNet, Databricks-delegated subnets, NSGs, Premium workspace |
| Workspace plane | `databricks/databricks` (~> 1.122) | Cluster policy, all-purpose cluster, optional UC storage credential + actuarial/sandbox external locations + actuarial catalog/schemas/grants |
| Account plane | `databricks` alias `accounts` | Metastore assignment to the shared UC metastore |

Unity Catalog **metastore** itself is created once by [`bootstrap-unity-catalog/`](../bootstrap-unity-catalog/) (Account Admin required). This module only assigns the workspace and optionally creates the shared `actuarial` catalog.

## Prerequisites

- Terraform **>= 1.5**
- Azure CLI (`az`) authenticated to a subscription where you have **Contributor** (or equivalent)
- Azure Databricks resource provider registered: `Microsoft.Databricks`
- Shared UC metastore already applied via `bootstrap-unity-catalog` (you need its `metastore_id`, and if creating the actuarial catalog also `storage_root` + `access_connector_id`)
- Databricks **account ID** from the Account Console (needed for metastore assignment)

```bash
az login
az account set --subscription "<subscription-id>"
az provider register --namespace Microsoft.Databricks
```

## Configure

```bash
cd deploy-databricks-azure
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set at least:

- `subscription_id` — required by azurerm 4.x
- `prefix` — **required**, unique per person on the shared subscription (e.g. `dbx-alice`)
- `owner` — recommended for cost attribution
- `location` as needed (must match the bootstrap metastore region)
- `databricks_account_id` — from Account Console
- `metastore_id` — from `bootstrap-unity-catalog` output
- `create_actuarial_catalog` — **exactly one** participant sets `true`; others leave `false`
- When `create_actuarial_catalog = true`, also set `uc_storage_root` and `uc_access_connector_id` from bootstrap (`storage_root`, `access_connector_id`) — required because the auto-created Azure metastore has no metastore-level storage

## Multi-user deploys

Multiple people can use this same code on one Azure subscription:

1. Copy `terraform.tfvars.example` to your own `terraform.tfvars` (do not commit it).
2. Set a **unique** `prefix` (3–16 lowercase chars; e.g. `dbx-alice`, `dbx-bob`). All Azure names (RG, VNet, workspace, managed RG) and the default cluster name derive from it.
3. Optionally set `owner` so resources are tagged for cost tracking.
4. Keep your own local `terraform.tfstate` — do not share state or `terraform.tfvars` with others.
5. All participants use the same `metastore_id` from bootstrap. Only one person sets `create_actuarial_catalog = true` (plus `uc_storage_root` / `uc_access_connector_id`) so the actuarial catalog is created once; grants to `account users` then cover everyone.

Colliding prefixes will fail on resource group or managed RG name conflicts in the subscription.

## Deploy

```bash
terraform init
terraform plan
terraform apply
```

Workspace creation typically takes **20–30 minutes**. On first apply, Terraform creates the Azure workspace first, then authenticates to it and provisions the cluster policy, cluster, and UC assignment (data sources are deferred until the workspace exists).

### Recommended order

1. Account Admin applies `bootstrap-unity-catalog` once → copy `metastore_id` (and for the catalog creator, `storage_root` + `access_connector_id`).
2. Each participant applies this module with `metastore_id` + `databricks_account_id`.
3. Exactly one participant sets `create_actuarial_catalog = true` with `uc_storage_root` / `uc_access_connector_id` (others leave `create_actuarial_catalog = false`).

A short post-workspace wait is included so the first cluster create is less likely to hit a transient `WorkerEnv not found in central` race on brand-new workspaces.

## Outputs

| Output | Description |
|--------|-------------|
| `databricks_host` | Workspace URL (`https://adb-....azuredatabricks.net`) |
| `cluster_id` / `cluster_name` | Provisioned single-node all-purpose cluster |
| `cluster_policy_id` | Enforced cluster policy |
| `sandbox_external_location_name` | Training sandbox external location name (when `create_actuarial_catalog = true`) |
| `sandbox_external_location_path` | abfss base path for ad hoc catalogs / external tables (`…/sandbox`) |

Open the workspace URL in a browser (same Azure AD identity as `az login`) to use the cluster.

## Best practices included

- Premium SKU (required for new workspaces; Standard tier is retiring)
- VNet injection with subnets delegated to `Microsoft.Databricks/workspaces`
- `no_public_ip = true` (Secure Cluster Connectivity)
- Latest LTS Spark runtime via data source
- Single-node all-purpose cluster (`is_single_node`) so only one VM is needed — avoids eastus capacity timeouts when driver+worker cannot both be acquired under a ~10 regional vCPU quota
- Auto-termination (default 30 minutes, max 60)
- Default node type `Standard_E4ads_v7` (Databricks-supported). Avoid `Standard_E4ds_v7` (not supported). Flexible alternates disabled (`alternate_node_type_ids = []`) to prevent incompatible auto-fallbacks. Raise Total Regional / family quota before moving to multi-node
- Cluster policy fixing UC shared mode (`USER_ISOLATION`) and idle timeout
- Unity Catalog workspace assignment + optional shared `actuarial` catalog (bronze/silver/gold) and `sandbox-uc-location` for learner ad hoc catalogs / external tables (same bootstrap storage, `/sandbox` path). Catalog grants to `account users` include `CREATE_TABLE` and `CREATE_MATERIALIZED_VIEW` so serverless Lakeflow pipelines can publish medallion tables.
- Consistent tags (`Environment`, `Project`, `ProvisionedBy`, optional `Owner`)
- Avoid Databricks default/reserved cluster tag keys in `custom_tags` and policies (`Vendor`, `ClusterId`, `ClusterName`, `Creator`, `Name`, `RunName`, `JobId`, `ManagedBy`) — policy-enforced conflicts fail cluster create

## Destroy

```bash
terraform destroy
```

Azure also creates a **managed resource group** (`<prefix>-dbx-managed-rg`). Terraform removes it with the workspace; do not delete that RG manually first.

Do not destroy the shared metastore from `bootstrap-unity-catalog` until all workspaces are detached.

## Training users, groups, and ad hoc catalogs

Do **not** add trainees or extra catalogs in this stack. Use the sibling module [`manage-databricks-training/`](../manage-databricks-training/) after this workspace is applied with `create_actuarial_catalog = true` (sandbox external location). That stack creates Entra users/groups, assigns them to the workspace, and provisions on-demand catalogs/schemas under the sandbox storage path—without re-running VNet/workspace/cluster applies.

## Out of scope (optional next steps)

- Private Link / disabling public workspace access
- Customer-managed keys
- Instance pools, jobs, and notebooks
- Remote Terraform state (Azure Storage + blob lock)
