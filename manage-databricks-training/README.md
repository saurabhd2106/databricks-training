# Manage Databricks Training (Terraform)

Decoupled stack for **training users, groups, and on-demand Unity Catalog catalogs**.

Use this after the workspace and shared UC foundation exist. It does **not** create Azure networks, workspaces, clusters, or the shared `actuarial` catalog — those stay in [`bootstrap-unity-catalog/`](../bootstrap-unity-catalog/) and [`deploy-databricks-azure/`](../deploy-databricks-azure/).

## What it manages

| Layer | Provider | Resources |
|-------|----------|-----------|
| Entra ID | `hashicorp/azuread` | Users (optional create), security groups, membership |
| Databricks account | `databricks` alias `accounts` | Account users/groups, workspace assignment (`USER` / `ADMIN`) |
| Databricks workspace | `databricks` | Entitlements, shared-cluster ACLs (`CAN_RESTART`), PAT ACLs (`CAN_USE`), catalogs, schemas, UC grants |

New catalogs use managed storage under the existing sandbox path:

`{uc_storage_root}/sandbox/{catalog_name}`

## Prerequisites

- Terraform **>= 1.5**
- Azure CLI authenticated (`az login`) to the same tenant
- **Entra**: permission to create users/groups (e.g. User Administrator / Group Administrator) when `create_entra_user = true`
- **Databricks**: Account Admin (or equivalent) for account-level user/group APIs and workspace assignment
- Already applied:
  1. `bootstrap-unity-catalog` → copy `storage_root`
  2. `deploy-databricks-azure` with **`create_actuarial_catalog = true`** once (creates `sandbox-uc-location` required for training catalog storage)
- **Personal access tokens**: at least one PAT must already exist in the workspace before Terraform can manage token permissions (Databricks API prerequisite). A workspace/account admin creates that first token once if needed.

```bash
az login
az account show --query tenantId -o tsv
```

## Configure

```bash
cd manage-databricks-training
cp terraform.tfvars.example terraform.tfvars
```

Set at least:

| Variable | Source |
|----------|--------|
| `databricks_account_id` | Account Console |
| `azure_tenant_id` | `az account show --query tenantId -o tsv` |
| `databricks_workspace_id` | Numeric ID from deploy state (see below) |
| `databricks_host` | `deploy-databricks-azure` output `databricks_host` |
| `cluster_id` | `deploy-databricks-azure` output `cluster_id` (`terraform -chdir=../deploy-databricks-azure output -raw cluster_id`) |
| `uc_storage_root` | `bootstrap-unity-catalog` output `storage_root` |
| `entra_domain` | Your Entra primary domain (e.g. `contoso.onmicrosoft.com`) |
| `training_users` | Map of trainees (UPN = `mail_nickname@entra_domain` unless `user_principal_name` is set) |
| `training_groups` | Optional groups; `members` are keys from `training_users` |
| `catalogs` | Optional catalogs/schemas + `grant_to_users` / `grant_to_groups` |

### Numeric workspace ID

```bash
terraform -chdir=../deploy-databricks-azure state show azurerm_databricks_workspace.this
# use attribute workspace_id (digits), not the Azure resource id
```

Or take the number from the workspace URL: `https://adb-<workspace_id>.….azuredatabricks.net`.

### Users: create vs existing

- `create_entra_user = true` (default) — Terraform creates the Entra user with a random password (`force_password_change = true`).
- `create_entra_user = false` — look up an existing Entra user by UPN and only add them to Databricks / groups.

Groups and workspace assignment always run for entries in `training_users` / `training_groups`.

## Deploy

```bash
terraform init
terraform plan
terraform apply
```

After apply, share credentials carefully:

```bash
terraform output training_user_upns
terraform output -json training_user_passwords
```

Trainees open `databricks_host`, sign in with their Entra UPN, and change the password on first login.

Add or remove users, groups, or catalogs by editing `terraform.tfvars` and re-applying — no need to re-run the workspace stack.

## Default catalog privileges

Unless overridden per catalog with `privileges`:

- `USE_CATALOG`, `USE_SCHEMA`, `CREATE_SCHEMA`
- `CREATE_TABLE`, `CREATE_MATERIALIZED_VIEW`, `CREATE_VOLUME`
- `SELECT`, `MODIFY`

Cluster create entitlement defaults to **false**. Apply grants **`CAN_RESTART`** on the shared all-purpose cluster (`cluster_id` from deploy) so trainees can attach to and start it. The example tfvars gives each trainee a personal `actuarial_<firstname>` catalog with those privileges.

Trainees also get **`CAN_USE`** on personal access tokens so they can create PATs for local CLI/SDK authentication without being workspace or account admins. There can be only one token-permissions resource per workspace; this stack owns it for all `training_users` / `training_groups`.

## Destroy

```bash
terraform destroy
```

This removes Entra users/groups **created by this stack**, Databricks account principals it manages, workspace assignments, and training catalogs/schemas (`force_destroy` defaults to true on catalogs).

It does **not** destroy the workspace, `actuarial` catalog, or bootstrap metastore.

## Out of scope

- Shared `actuarial` bronze/silver/gold catalog (owned by `deploy-databricks-azure`)
- Workspace / VNet / cluster provisioning
- Microsoft 365 or other product license assignment for Entra users
