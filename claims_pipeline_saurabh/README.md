# claims_pipeline_saurabh

Databricks Asset Bundle (Declarative Automation Bundle) that ingests cyclone/insurance sample CSVs and builds an actuarial **bronze → silver → gold** medallion lakehouse on Unity Catalog.

Infrastructure for the workspace, all-purpose cluster, and `actuarial` catalog comes from sibling project [`deploy-databricks-azure`](../deploy-databricks-azure/). Sample source files live under `fixtures/sample-data/` (copied from repo [`sample-data`](../sample-data/)).

## What this repo does

1. **Lands** four CSVs into a Unity Catalog Volume (`/Volumes/actuarial/bronze/landing/...`) using a Job notebook on the Terraform all-purpose cluster.
2. **Runs** a serverless Lakeflow Declarative Pipeline that:
   - **Bronze** — raw CSV ingest with `_ingest_ts` / `_source_file`
   - **Silver** — typed, deduped, current-claim views
   - **Gold** — actuarial marts (event losses, policy loss ratio, risk-band performance)

```text
fixtures/sample-data/*.csv
        │
        ▼
 Job: land_sample_data  ──existing_cluster_id──►  Volume actuarial.bronze.landing
        │
        ▼
 Pipeline: claims_pipeline_saurabh_etl  (serverless)
        │
        ├── actuarial.bronze.*   (raw)
        ├── actuarial.silver.*   (cleaned)
        └── actuarial.gold.*     (marts)
```

## Source datasets

| File | Grain / notes | Join keys |
|------|----------------|-----------|
| `claims_bordereau.csv` | SCD-style claim snapshots (`claim_id` + `snapshot_date`) | `policy_id`, `event_id` |
| `premium_bordereau.csv` | One row per policy (~5,000) | `policy_id`, `postcode` |
| `risk_zone_lookup.csv` | Postcode → region / wind risk band (postcode `4825` duplicated; silver keeps one) | `postcode` |
| `cyclone_events.csv` | Six illustrative cyclone events | `event_id` |

## Output tables

| Layer | Schema | Tables | Purpose |
|-------|--------|--------|---------|
| Bronze | `actuarial.bronze` | `claims_bordereau`, `premium_bordereau`, `risk_zone_lookup`, `cyclone_events` | Raw string ingest from landing paths |
| Silver | `actuarial.silver` | `claims_snapshots`, `claims_current`, `policies`, `risk_zones`, `cyclone_events` | Typed dates/decimals; latest claim; deduped zones |
| Gold | `actuarial.gold` | `event_loss_summary`, `policy_loss_ratio`, `risk_band_performance` | Event losses; premium vs incurred; band × region KPIs |

Silver expectations on claims snapshots: `incurred_amount >= paid_to_date` (rows that fail are dropped).

## Repository layout

```text
claims_pipeline_saurabh/
├── databricks.yml                 # Bundle name, workspace host, variables, dev/prod targets
├── pyproject.toml                 # Python package claims_pipeline_saurabh
├── fixtures/sample-data/          # CSVs synced to the workspace on deploy
├── resources/
│   ├── landing.volume.yml         # UC Volume actuarial.bronze.landing
│   ├── claims_pipeline_saurabh_etl.pipeline.yml  # Serverless Lakeflow pipeline
│   └── claims_pipeline_job.job.yml               # Land → refresh_pipeline job
├── src/
│   ├── land_sample_data.ipynb     # Copies CSVs into the landing volume
│   ├── claims_pipeline_saurabh/   # Shared package stub (CLI entrypoint)
│   └── claims_pipeline_saurabh_etl/
│       └── transformations/       # One @dp.table dataset per file (bronze/silver/gold)
└── tests/                         # Local test harness (Databricks Connect / fixtures)
```

### Key config

| File / setting | Role |
|----------------|------|
| `databricks.yml` → `catalog` | `actuarial` |
| `databricks.yml` → `cluster_id` | Terraform all-purpose cluster (`0730-111218-jwuz715u`) for the land task |
| `databricks.yml` → `landing_volume_path` | `/Volumes/actuarial/bronze/landing` |
| Pipeline `serverless: true` | Pipeline compute (Lakeflow cannot use `existing_cluster_id`) |
| Pipeline `configuration.landing_path` | Injected into transforms via `spark.conf.get("landing_path")` |

### Transformations (by layer)

- **Bronze** (`bronze_*.py`) — `spark.read.format("csv")` from volume subfolders `claims/`, `premiums/`, `risk_zones/`, `cyclone_events/`
- **Silver** (`silver_*.py`) — cast types; `claims_current` = latest snapshot per `claim_id`; `risk_zones` dedupe by `postcode`
- **Gold** (`gold_*.py`) — aggregates joining claims ↔ events and claims ↔ policies

## Compute model

| Workload | Compute | Why |
|----------|---------|-----|
| `land_sample_data` notebook | `existing_cluster_id` (Terraform cluster) | Jobs support all-purpose clusters; copies workspace files → Volume |
| `claims_pipeline_saurabh_etl` | Serverless Lakeflow | Avoids Azure VM quota / “Waiting for resources” on classic DLT clusters |

## Prerequisites

- Databricks CLI authenticated to `https://adb-7405611775215693.13.azuredatabricks.net`
- Unity Catalog catalog `actuarial` with schemas `bronze`, `silver`, `gold` ([`deploy-databricks-azure`](../deploy-databricks-azure/))
- Catalog grants for `account users` including `CREATE_TABLE` and `CREATE_MATERIALIZED_VIEW` (serverless `@dp.table` publishes as materialized views)
- All-purpose cluster `0730-111218-jwuz715u` startable for the land task

## Deploy and run

```bash
cd claims_pipeline_saurabh

# Optional local deps for IDE / tests
uv sync --dev

databricks bundle validate --target dev
databricks bundle deploy --target dev

# Land CSVs, then run bronze → silver → gold
databricks bundle run claims_pipeline_job

# Pipeline only (if data already landed)
databricks bundle run claims_pipeline_saurabh_etl
```

Production:

```bash
databricks bundle deploy --target prod
databricks bundle run claims_pipeline_job --target prod
```

Dev mode prefixes job/pipeline names with `[dev <user>]` and pauses schedules.

## CI/CD (GitHub Actions)

Workflow: [`.github/workflows/claims-pipeline.yml`](../.github/workflows/claims-pipeline.yml)

| Trigger | What runs |
|---------|-----------|
| Pull request to `main` (paths under `claims_pipeline_saurabh/**`) | `databricks bundle validate --target prod` |
| Push to `main` (same paths) or **workflow_dispatch** | Validate → `bundle deploy --target prod` → `bundle run claims_pipeline_job --target prod` |

### Required repository secrets

| Secret | Value |
|--------|--------|
| `DATABRICKS_HOST` | `https://adb-7405611775215693.13.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Databricks PAT or service principal token for an identity that can deploy to the prod `root_path` (`/Workspace/Users/databricks-np@saurabhuptut.onmicrosoft.com/...`), start cluster `0730-111218-jwuz715u`, and manage the job/pipeline |

The token identity also needs Unity Catalog rights on `actuarial` (from [`deploy-databricks-azure`](../deploy-databricks-azure/)) so bronze/silver/gold tables and the landing volume can be created or updated.

## Verify data

After a successful run, check counts and joins in a SQL warehouse or notebook.

**Expected rough counts**

| Check | Expected |
|-------|----------|
| Bronze / silver claim snapshots | ~2,829 |
| `actuarial.silver.claims_current` | ~1,533 unique claims |
| `actuarial.silver.policies` | 5,000 |
| `actuarial.silver.risk_zones` | 19 (one postcode deduped) |
| `actuarial.silver.cyclone_events` | 6 |
| `actuarial.gold.event_loss_summary` | 6 |

```sql
-- Row counts
SELECT 'bronze_claims' AS t, COUNT(*) AS n FROM actuarial.bronze.claims_bordereau
UNION ALL SELECT 'silver_snapshots', COUNT(*) FROM actuarial.silver.claims_snapshots
UNION ALL SELECT 'silver_current', COUNT(*) FROM actuarial.silver.claims_current
UNION ALL SELECT 'silver_policies', COUNT(*) FROM actuarial.silver.policies
UNION ALL SELECT 'silver_risk_zones', COUNT(*) FROM actuarial.silver.risk_zones
UNION ALL SELECT 'silver_events', COUNT(*) FROM actuarial.silver.cyclone_events
UNION ALL SELECT 'gold_event_loss', COUNT(*) FROM actuarial.gold.event_loss_summary
UNION ALL SELECT 'gold_policy_lr', COUNT(*) FROM actuarial.gold.policy_loss_ratio
UNION ALL SELECT 'gold_risk_band', COUNT(*) FROM actuarial.gold.risk_band_performance;

-- Orphan checks (expect 0)
SELECT COUNT(*) AS orphan_policies
FROM actuarial.silver.claims_current c
LEFT JOIN actuarial.silver.policies p ON c.policy_id = p.policy_id
WHERE p.policy_id IS NULL;

SELECT COUNT(*) AS orphan_events
FROM actuarial.silver.claims_current c
LEFT JOIN actuarial.silver.cyclone_events e ON c.event_id = e.event_id
WHERE e.event_id IS NULL;

-- Gold samples
SELECT * FROM actuarial.gold.event_loss_summary ORDER BY total_incurred DESC;
SELECT * FROM actuarial.gold.policy_loss_ratio ORDER BY loss_ratio DESC NULLS LAST LIMIT 20;
```

Landing files:

```sql
LIST '/Volumes/actuarial/bronze/landing/claims';
LIST '/Volumes/actuarial/bronze/landing/premiums';
```

## Related repos

| Path | Role |
|------|------|
| [`../deploy-databricks-azure`](../deploy-databricks-azure/) | Azure workspace, cluster, UC `actuarial` catalog/schemas/grants |
| [`../sample-data`](../sample-data/) | Upstream sample CSVs (copied into `fixtures/sample-data/`) |
| [`../claim_pipeline_sample`](../claim_pipeline_sample/) | Minimal DAB / pipeline template this project was patterned after |
