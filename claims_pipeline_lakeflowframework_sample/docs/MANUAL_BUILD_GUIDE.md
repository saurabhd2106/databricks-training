# Manual Build Guide: Claims Pipeline with Lakeflow Framework

This guide walks through building the actuarial **bronze → silver → gold** pipeline **from scratch** using the [Lakeflow Framework](../../lakeflow_framework/) (metadata-driven YAML dataflows). Use this repository as the completed reference: every file you create below already exists under [`claims_pipeline_lakeflowframework_sample/`](../).

For a quick deploy of the finished project, see the [README](../README.md).

---

## What you will build

```text
fixtures/sample-data/*.csv
        │
        ▼
 Job notebooks (create schemas + land CSVs)
        │
        ▼
 /Volumes/actuarial/sample/landing/{claims,premiums,risk_zones,cyclone_events}/
        │
        ▼
 Bronze pipeline (cloudFiles Auto Loader)  →  actuarial.sample.*
        │
        ▼
 Silver pipeline (SCD1 + claims_current MV) →  actuarial.sample.*
        │
        ▼
 Gold pipeline (materialized view marts)    →  actuarial.sample.*
        │
        ▼
 AI/BI Lakeview dashboards (bundle resources) → query gold + published event logs
```

| Approach | Where used |
|----------|------------|
| **Lakeflow Framework YAML** (this guide) | Specs under `src/dataflows/`; runtime is the framework `dlt_pipeline` notebook |
| **Python `@dp.table`** (sibling project) | [`claims_pipeline_saurabh`](../../claims_pipeline_saurabh/) — same domain, different authoring style |

You do **not** write per-table Python transforms. You author YAML/SQL specs; the framework turns them into Lakeflow Declarative Pipeline tables. Lakeview dashboards are separate bundle resources that **consume gold** (and pipeline event logs).

**Defaults in this guide** (change for another workspace):

| Setting | Value |
|---------|--------|
| Catalog | `actuarial` |
| Schema | `sample` (all medallion layers) |
| Landing volume | `/Volumes/actuarial/sample/landing` |
| SQL warehouse (dashboards) | `66d7b615b6cff38c` (`warehouse_id`) |
| Workspace host | `https://adb-7405611775215693.13.azuredatabricks.net` |
| Land/setup cluster | `0730-111218-jwuz715u` |

---

## Prerequisites

1. **Databricks CLI** authenticated to your workspace.
2. **Deploy Lakeflow Framework** first (pipelines attach its `dlt_pipeline` notebook):

   ```bash
   cd ../lakeflow_framework
   databricks bundle deploy -t dev
   ```

   Note the deployed `src` path, typically:

   `/Workspace/Users/<you>/.bundle/lakeflow_framework/dev/current/files/src`

3. **Unity Catalog** rights: create catalog/schemas/volumes/tables/materialized views (or have an admin create `actuarial.sample`).
4. An **all-purpose cluster** for landing notebooks (Jobs can use `existing_cluster_id`; serverless pipelines cannot).
5. Sample CSVs: `claims_bordereau.csv`, `premium_bordereau.csv`, `risk_zone_lookup.csv`, `cyclone_events.csv` (from [`sample-data`](../../sample-data/)).

---

## Step 1: Scaffold the bundle

Create the project root and folder tree:

```text
claims_pipeline_lakeflowframework_sample/
├── databricks.yml
├── .gitignore
├── fixtures/sample-data/
├── resources/                    # pipelines, job, volume, *.dashboard.yml
└── src/
    ├── notebooks/
    ├── pipeline_configs/
    ├── dashboards/               # *.lvdash.json (Lakeview definitions)
    └── dataflows/
        ├── bronze/{dataflowspec,schemas,expectations}/
        ├── silver/{dataflowspec,schemas,expectations,dml}/
        └── gold/{dataflowspec,dml}/
```

Copy the four CSVs into `fixtures/sample-data/`.

### `databricks.yml`

Declare the bundle, variables, and targets. Essentials:

- `include: resources/*.yml` — also picks up every `*.dashboard.yml` (no separate dashboard list)
- Variables: `catalog`, `framework_source_path`, `workspace_host`, `bronze_schema` / `silver_schema` / `gold_schema` (short UC schema names for pipeline resources), `cluster_id`, `landing_volume_path`, `logical_env`, **`warehouse_id`** (SQL warehouse for Lakeview)
- Dev / prod targets: `catalog: actuarial`, `bronze_schema` / `silver_schema` / `gold_schema`: `sample`, `landing_volume_path: /Volumes/actuarial/sample/landing`, `warehouse_id`, and `framework_source_path` pointing at the deployed framework `src/`

See the full file: [`databricks.yml`](../databricks.yml).

Add a `.gitignore` that ignores `.databricks/`, `.venv/`, `scratch/`, etc.

---

## Step 2: Pipeline configs

Lakeflow Framework reads configs from `src/pipeline_configs/`.

### Spec format — `global.json`

Lock the bundle to YAML specs (do not mix JSON and YAML in one bundle):

```json
{
  "pipeline_bundle_spec_format": {
    "format": "yaml"
  }
}
```

Reference: [`src/pipeline_configs/global.json`](../src/pipeline_configs/global.json).

### Substitutions — `dev_substitutions.yaml`

Tokens used inside dataflow specs as `{bronze_schema}`, `{sample_file_location}`, etc. Use **fully qualified** catalog.schema names here (pipeline YAML uses short schema names separately):

```yaml
tokens:
  bronze_schema: actuarial.sample{logical_env}
  silver_schema: actuarial.sample{logical_env}
  gold_schema: actuarial.sample{logical_env}
  sample_file_location: /Volumes/actuarial/sample{logical_env}/landing
```

`{logical_env}` is filled from pipeline configuration `logicalEnv` (empty by default).

Reference: [`src/pipeline_configs/dev_substitutions.yaml`](../src/pipeline_configs/dev_substitutions.yaml).

---

## Step 3: Landing zone

Pipelines read files from a Unity Catalog Volume. Create the catalog/schemas **before** the first `bundle deploy` so the volume resource can attach to `actuarial.sample`.

### Bootstrap SQL (one-time)

```sql
CREATE CATALOG IF NOT EXISTS actuarial;
CREATE SCHEMA IF NOT EXISTS actuarial.sample;
```

### Volume resource

Add [`resources/landing.volume.yml`](../resources/landing.volume.yml):

```yaml
resources:
  volumes:
    actuarial_sample_landing:
      catalog_name: ${var.catalog}
      schema_name: ${var.bronze_schema}
      name: landing
      volume_type: MANAGED
```

### Notebooks

| Notebook | Purpose |
|----------|---------|
| [`initialize.ipynb`](../src/notebooks/initialize.ipynb) | Widgets + path variables (`catalog`, schema FQNs, volume root) |
| [`create_schemas_and_tables.ipynb`](../src/notebooks/create_schemas_and_tables.ipynb) | `%run initialize` then `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` |
| [`land_sample_data.ipynb`](../src/notebooks/land_sample_data.ipynb) | Copy fixtures into volume subfolders |

Landing map (filename → subdirectory under the volume):

| File | Subdir |
|------|--------|
| `claims_bordereau.csv` | `claims/` |
| `premium_bordereau.csv` | `premiums/` |
| `risk_zone_lookup.csv` | `risk_zones/` |
| `cyclone_events.csv` | `cyclone_events/` |

After landing, Auto Loader paths look like `{sample_file_location}/claims/`.

---

## Step 4: Bronze dataflows

Work one entity end-to-end, then repeat the pattern for the other three sources.

### 4a. Schemas

Under `src/dataflows/bronze/schemas/`:

1. **File schema** — columns present in the CSV only (all strings for raw ingest), e.g. `claims_bordereau_file_schema.json`.
2. **Target schema** — file columns plus lineage fields `_ingest_ts` (timestamp) and `_source_file` (string), e.g. `claims_bordereau_schema.json`.

Spark struct JSON shape:

```json
{
  "type": "struct",
  "fields": [
    {"name": "claim_id", "type": "string", "nullable": true, "metadata": {}}
  ]
}
```

### 4b. Dataflow YAML

Create `src/dataflows/bronze/dataflowspec/claims_bordereau_main.yaml`:

- `dataFlowGroup: claims_bronze` (must match the pipeline filter later)
- `sourceType: cloudFiles`
- `path: '{sample_file_location}/claims/'`
- `readerOptions`: `cloudFiles.format: csv`, `header: 'true'`
- `selectExp`: pass through CSV columns; add `current_timestamp() AS _ingest_ts` and `_metadata.file_path AS _source_file`
- Target: `database: '{bronze_schema}'`, `table: claims_bordereau`, CDF enabled

Core shape:

```yaml
dataFlowId: claims_bordereau
dataFlowGroup: claims_bronze
dataFlowType: standard
sourceType: cloudFiles
sourceDetails:
  path: '{sample_file_location}/claims/'
  readerOptions:
    cloudFiles.format: csv
    header: 'true'
  schemaPath: claims_bordereau_file_schema.json
  selectExp:
    - claim_id
    # ... other columns ...
    - current_timestamp() AS _ingest_ts
    - _metadata.file_path AS _source_file
mode: stream
targetFormat: delta
targetDetails:
  database: '{bronze_schema}'
  table: claims_bordereau
  tableProperties:
    delta.enableChangeDataFeed: 'true'
  schemaPath: claims_bordereau_schema.json
```

Full file: [`claims_bordereau_main.yaml`](../src/dataflows/bronze/dataflowspec/claims_bordereau_main.yaml).

Wire bronze DQE (key / non-empty hygiene) the same way as silver — see §6b and [`bronze/expectations/`](../src/dataflows/bronze/expectations/).

### 4c. Repeat for the other sources

| Spec | Volume subdir | Bronze table |
|------|---------------|--------------|
| `premium_bordereau_main.yaml` | `premiums/` | `premium_bordereau` |
| `risk_zone_lookup_main.yaml` | `risk_zones/` | `risk_zone_lookup` |
| `cyclone_events_main.yaml` | `cyclone_events/` | `bronze_cyclone_events` |

All four share `dataFlowGroup: claims_bronze` and enable DQE via `dataQualityExpectationsEnabled` / `dataQualityExpectationsPath`.

---

## Step 5: Bronze pipeline resource

Add [`resources/claims_bronze_pipeline.yml`](../resources/claims_bronze_pipeline.yml):

```yaml
resources:
  pipelines:
    claims_bronze_pipeline:
      name: claims_lff_bronze
      channel: CURRENT
      serverless: true
      catalog: ${var.catalog}
      schema: ${var.bronze_schema}
      libraries:
        - notebook:
            path: ${var.framework_source_path}/dlt_pipeline
      configuration:
        bundle.sourcePath: ${workspace.file_path}/src
        bundle.target: ${bundle.target}
        framework.sourcePath: ${var.framework_source_path}
        workspace.host: ${var.workspace_host}
        pipeline.layer: bronze
        logicalEnv: ${var.logical_env}
        pipeline.dataFlowGroupFilter: claims_bronze
      root_path: ${workspace.file_path}/src
```

Key points:

- **Library** is the framework entry notebook, not your dataflow YAML.
- **`bundle.sourcePath`** is *your* bundle `src/` (specs + substitutions).
- **`pipeline.dataFlowGroupFilter`** selects only `claims_bronze` specs.

---

## Step 6: Silver dataflows

Silver cleans and keys bronze tables. Create specs under `src/dataflows/silver/`.

### 6a. SCD1 typed tables

Pattern for each entity:

1. Target schema JSON with typed columns (`date`, `decimal(18,2)`, etc.).
2. YAML with `sourceType: delta`, `cdfEnabled: true`, `selectExp` casts, and `cdcSettings` SCD Type 1.

Example — claims snapshots (`dataFlowGroup: claims_silver`):

```yaml
sourceDetails:
  database: '{bronze_schema}'
  table: claims_bordereau
  cdfEnabled: true
  selectExp:
    - cast(claim_id as string) as claim_id
    - to_date(date_of_loss) as date_of_loss
    - cast(incurred_amount as decimal(18,2)) as incurred_amount
    # ...
cdcSettings:
  keys:
    - claim_id
    - snapshot_date
  sequence_by: _ingest_ts
  scd_type: '1'
```

| Spec | Source bronze table | Keys | Silver table |
|------|---------------------|------|--------------|
| `claims_snapshots_main.yaml` | `claims_bordereau` | `claim_id`, `snapshot_date` | `claims_snapshots` |
| `policies_main.yaml` | `premium_bordereau` | `policy_id` | `policies` |
| `risk_zones_main.yaml` | `risk_zone_lookup` | `postcode` | `risk_zones` |
| `cyclone_events_main.yaml` | `bronze_cyclone_events` | `event_id` | `cyclone_events` |

For policies, add `is_active` in `selectExp`:

```text
(to_date(policy_end_date) is null or to_date(policy_end_date) >= current_date()) as is_active
```

Full example: [`claims_snapshots_main.yaml`](../src/dataflows/silver/dataflowspec/claims_snapshots_main.yaml).

### 6b. Data quality expectations

Add YAML under each layer’s `expectations/` folder and wire every bronze + silver SCD1 dataflowspec:

```yaml
dataQualityExpectationsEnabled: true
dataQualityExpectationsPath: ./claims_snapshots_dqe.yaml
```

(The framework resolves that path under the group’s `expectations/` folder.)

| Layer | Files |
|-------|-------|
| Bronze | [`bronze/expectations/`](../src/dataflows/bronze/expectations/) — key / non-empty string hygiene |
| Silver | [`claims_snapshots_dqe.yaml`](../src/dataflows/silver/expectations/claims_snapshots_dqe.yaml), [`policies_dqe.yaml`](../src/dataflows/silver/expectations/policies_dqe.yaml), [`cyclone_events_dqe.yaml`](../src/dataflows/silver/expectations/cyclone_events_dqe.yaml), [`risk_zones_dqe.yaml`](../src/dataflows/silver/expectations/risk_zones_dqe.yaml) |

Example silver claims rules (abbreviated):

```yaml
expect_or_drop:
  - name: claim_id_not_null
    constraint: claim_id IS NOT NULL
    tag: Completeness
  - name: incurred_gte_paid
    constraint: incurred_amount >= paid_to_date
    tag: Validity
expect:
  - name: reported_on_or_after_loss
    constraint: reported_date >= date_of_loss
    tag: Validity
  - name: valid_claim_status
    constraint: claim_status IN ('Open', 'Closed', 'Reopened')
    tag: Validity
```

Use `expect_or_drop` for keys and hard financial inconsistencies; use `expect` for soft domain / date-order anomalies (kept + metrics). Gold and `claims_current` have no separate DQE — they inherit from silver.

### 6c. `claims_current` materialized view

Latest snapshot per `claim_id`. SQL in [`src/dataflows/silver/dml/claims_current.sql`](../src/dataflows/silver/dml/claims_current.sql) uses **`live.claims_snapshots`** so the silver pipeline tracks the dependency inside the same run:

```sql
SELECT ...
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY snapshot_date DESC) AS _rn
  FROM live.claims_snapshots
)
WHERE _rn = 1
```

Spec ([`claims_current_main.yaml`](../src/dataflows/silver/dataflowspec/claims_current_main.yaml)):

```yaml
dataFlowId: claims_current
dataFlowGroup: claims_silver
dataFlowType: materialized_view
materializedViews:
  claims_current:
    sqlPath: ./claims_current.sql
    tableDetails:
      database: '{silver_schema}'
```

---

## Step 7: Gold dataflows

Gold marts are SQL materialized views over silver — **consumption-ready tables for Lakeview**. Put SQL under `src/dataflows/gold/dml/` and one group spec under `dataflowspec/`.

### SQL marts

| File | Grain / joins | Powers dashboard |
|------|----------------|------------------|
| [`event_loss_summary.sql`](../src/dataflows/gold/dml/event_loss_summary.sql) | Cat vs non-cat by event × region × peril (`claims_current` ⋈ events ⋈ policies) | `catastrophe_events` |
| [`policy_loss_ratio.sql`](../src/dataflows/gold/dml/policy_loss_ratio.sql) | Insurer × region × band × building × mitigation; `loss_ratio` / `loss_ratio_pct` | `underwriting_portfolio` |
| [`risk_band_performance.sql`](../src/dataflows/gold/dml/risk_band_performance.sql) | Wind risk × region; `claim_frequency` + `loss_ratio` | `underwriting_portfolio` |
| [`claims_summary.sql`](../src/dataflows/gold/dml/claims_summary.sql) | Peril × status × region × band × building; reserve / settlement | `claims_operations` |
| [`claims_development.sql`](../src/dataflows/gold/dml/claims_development.sql) | Peril × region × band × loss/reported month; reporting lag | `claims_development` |
| [`portfolio_exposure.sql`](../src/dataflows/gold/dml/portfolio_exposure.sql) | Sum insured / premium by underwriting dims | `underwriting_portfolio` |
| [`medallion_inventory.sql`](../src/dataflows/gold/dml/medallion_inventory.sql) | Row counts + `MAX(_ingest_ts)` across bronze/silver/gold | `pipeline_monitoring` |

Use **FQN tokens** for cross-pipeline reads, e.g. `{silver_schema}.claims_current` (not `live.*`, because gold is a separate pipeline).

### Spec

[`gold_marts_main.yaml`](../src/dataflows/gold/dataflowspec/gold_marts_main.yaml) registers every mart under `materializedViews` with `database: '{gold_schema}'` and a `sqlPath`. See the file in-repo for the full list (seven MVs).

---

## Step 8: Silver/gold pipelines and orchestrating job

### Pipeline resources

Mirror the bronze pipeline YAML twice:

| Resource file | Filter | Layer / schema var |
|---------------|--------|--------------------|
| [`claims_silver_pipeline.yml`](../resources/claims_silver_pipeline.yml) | `claims_silver` | `pipeline.layer: silver`, `schema: ${var.silver_schema}` |
| [`claims_gold_pipeline.yml`](../resources/claims_gold_pipeline.yml) | `claims_gold` | `pipeline.layer: gold`, `schema: ${var.gold_schema}` |

Same `libraries` notebook path and `bundle.sourcePath` / `framework.sourcePath` configuration.

### Publish event logs (for the event-log dashboard)

On **each** of bronze, silver, and gold pipelines, add an `event_log` block so Lakeview can query a stable UC table (not `event_log(pipeline_id)`). Publish all three into the shared sample schema:

```yaml
event_log:
  name: claims_lff_<layer>_event_log   # bronze | silver | gold
  catalog: ${var.catalog}
  schema: ${var.gold_schema}
```

Do not delete those tables after they are created — removing them can break future pipeline updates. Refresh the pipelines after deploy so the tables are populated before opening `pipeline_event_log`.

### Job

Add [`resources/claims_pipeline_job.job.yml`](../resources/claims_pipeline_job.job.yml) with this task chain:

1. `create_schemas_and_tables` — notebook on `existing_cluster_id`
2. `land_sample_data` — notebook; params `catalog`, `landing_path`, `source_path` (default `${workspace.file_path}/fixtures/sample-data`)
3. `bronze_pipeline` → `claims_bronze_pipeline` (`full_refresh: true`)
4. `silver_pipeline` → `claims_silver_pipeline`
5. `gold_pipeline` → `claims_gold_pipeline`

Each pipeline task depends on the previous. Enable the job queue.

---

## Step 9: AI/BI Lakeview dashboards

Dashboards are **bundle resources**, not Lakeflow Framework dataflows. They read **gold serving tables** (and published event logs) via a SQL warehouse. There is no separate compile step — the checked-in `.lvdash.json` is the definition; `databricks bundle deploy` publishes it.

### End-to-end flow

```text
databricks.yml
  ├─ include: resources/*.yml          ← picks up every *.dashboard.yml
  └─ var.warehouse_id (+ catalog / gold_schema)
         │
         ▼
resources/<name>.dashboard.yml
  ├─ warehouse_id, dataset_catalog, dataset_schema
  └─ file_path → src/dashboards/<name>.lvdash.json
         │
         ▼
.lvdash.json (datasets SQL + pages / widgets)
         │
         ▼
databricks bundle deploy  →  AI/BI Lakeview dashboards in the workspace
```

Step 1 already declared `warehouse_id` and `include: resources/*.yml`. Because of that include, **every** `*.dashboard.yml` deploys with `bundle deploy`. There is no separate dashboard deploy command.

### Two files per dashboard

| Piece | Path | Role |
|-------|------|------|
| Resource YAML | `resources/<name>.dashboard.yml` | Registers the dashboard; sets warehouse + default catalog/schema |
| Lakeview JSON | `src/dashboards/<name>.lvdash.json` | Datasets (SQL), pages, filters, KPIs, charts, tables |

Example resource ([`catastrophe_events.dashboard.yml`](../resources/catastrophe_events.dashboard.yml)):

```yaml
resources:
  dashboards:
    catastrophe_events:
      display_name: "Catastrophe Event Losses"
      file_path: ../src/dashboards/catastrophe_events.lvdash.json
      warehouse_id: ${var.warehouse_id}
      dataset_catalog: ${var.catalog}
      dataset_schema: ${var.gold_schema}
```

| Field | Role |
|-------|------|
| Resource key (`catastrophe_events`) | Bundle resource name — used by `bundle open` / `bundle generate` |
| `display_name` | Title shown in the workspace UI |
| `file_path` | Path to the Lakeview JSON definition (relative to the YAML) |
| `warehouse_id` | SQL warehouse that runs dashboard queries (`${var.warehouse_id}`) |
| `dataset_catalog` | Default catalog for unqualified table names (`${var.catalog}`) |
| `dataset_schema` | Default schema for unqualified table names (`${var.gold_schema}` → `sample`) |

Catalog/schema are injected at deploy time so JSON stays portable across targets. Do **not** hardcode `actuarial.sample.…` inside `.lvdash.json`.

### Lakeview JSON anatomy

Each [`src/dashboards/*.lvdash.json`](../src/dashboards/) file is a Lakeview document with two top-level parts:

1. **`datasets`** — named SQL queries (the data sources). Table names are **unqualified**.
2. **`pages` → `layout`** — widgets (title text, filters, KPIs, charts, tables) that reference datasets via `datasetName`.

Concrete example from [`catastrophe_events.lvdash.json`](../src/dashboards/catastrophe_events.lvdash.json):

```json
{
  "datasets": [
    {
      "name": "event_loss",
      "displayName": "Event loss summary",
      "query": "SELECT ... FROM event_loss_summary"
    }
  ],
  "pages": [
    {
      "name": "main",
      "displayName": "Catastrophe Events",
      "layout": [
        {
          "widget": {
            "queries": [{ "query": { "datasetName": "event_loss", ... } }],
            "spec": { "widgetType": "filter-multi-select", ... }
          }
        }
      ]
    }
  ]
}
```

Copy the finished definitions from [`src/dashboards/`](../src/dashboards/) and [`resources/*.dashboard.yml`](../resources/) as the reference rather than hand-authoring every widget from scratch.

### Inventory to create

| Resource key | Audience | Unqualified gold / log sources in `.lvdash.json` |
|--------------|----------|---------------------------------------------------|
| `catastrophe_events` | Cat / reinsurance | `event_loss_summary` |
| `underwriting_portfolio` | Underwriting | `policy_loss_ratio`, `risk_band_performance`, `portfolio_exposure` |
| `claims_operations` | Claims ops | `claims_summary` |
| `claims_development` | Actuarial | `claims_development` |
| `pipeline_monitoring` | Data eng | `medallion_inventory` |
| `pipeline_event_log` | Data eng / ops | `claims_lff_bronze_event_log` ∪ silver ∪ gold event logs |

Business dashboards sit on **gold marts**. Ops dashboards sit on **`medallion_inventory`** and the **published event logs** from Step 8.

### Authoring rules

1. **Unqualified table names** in dataset SQL (`FROM event_loss_summary`, not `actuarial.sample.event_loss_summary`). Catalog/schema come from `dataset_catalog` / `dataset_schema` at deploy time.
2. **Gold only** for business dashboards (LFF gold-serving practice). Monitoring uses `medallion_inventory` plus published event logs in the same schema.
3. After editing a dashboard in the UI, sync JSON back into the repo:

   ```bash
   databricks bundle generate dashboard --resource underwriting_portfolio --force
   ```

### Lifecycle (author → view)

1. **Author** — create/edit `src/dashboards/<name>.lvdash.json` (or edit in the UI and `bundle generate` back).
2. **Wire** — add `resources/<name>.dashboard.yml` pointing at the JSON with warehouse + catalog/schema.
3. **Deploy** — `databricks bundle deploy --target dev` (Step 10) publishes all six Lakeview dashboards.
4. **Run data** — `databricks bundle run claims_pipeline_job` so gold marts and event-log tables are populated.
5. **View** — `databricks bundle open <resource_key> --target dev` (commands listed in Step 10).

[`tests/test_bundle_config.py`](../tests/test_bundle_config.py) guards the contract: `warehouse_id` on both targets, `dataset_catalog` / `dataset_schema` substitutions, matching `.lvdash.json` files, unqualified SQL (no hardcoded `actuarial.sample.`), and expected gold / event-log needles.

---

## Step 10: Deploy and verify

```bash
cd claims_pipeline_lakeflowframework_sample

# Schema must exist before volume deploy (Step 3)
databricks bundle validate --target dev
databricks bundle deploy --target dev   # pipelines, job, volume, AND dashboards

databricks bundle run claims_pipeline_job
```

Override framework path if your deploy location differs:

```bash
databricks bundle deploy -t dev \
  --var="framework_source_path=/Workspace/Users/<user>/.bundle/lakeflow_framework/dev/current/files/src"
```

### Open dashboards

After the job succeeds (gold + event logs populated):

```bash
databricks bundle open catastrophe_events --target dev
databricks bundle open underwriting_portfolio --target dev
databricks bundle open claims_operations --target dev
databricks bundle open claims_development --target dev
databricks bundle open pipeline_monitoring --target dev
databricks bundle open pipeline_event_log --target dev
```

CI also deploys dashboards as part of sample `bundle deploy` — see [README → CI / GitHub Actions](../README.md#ci--github-actions).

### Expected counts

| Check | Expected |
|-------|----------|
| Bronze / silver claim snapshots | ~2,829 |
| `actuarial.sample.claims_current` | ~1,533 |
| `actuarial.sample.policies` | 5,000 |
| `actuarial.sample.risk_zones` | 19 |
| `actuarial.sample.cyclone_events` | 6 |
| `actuarial.sample.event_loss_summary` | > 6 (category × event × region × peril) |
| Other gold marts (`claims_summary`, `medallion_inventory`, …) | Non-empty after gold refresh |

```sql
SELECT 'bronze_claims' AS t, COUNT(*) AS n FROM actuarial.sample.claims_bordereau
UNION ALL SELECT 'silver_snapshots', COUNT(*) FROM actuarial.sample.claims_snapshots
UNION ALL SELECT 'silver_current', COUNT(*) FROM actuarial.sample.claims_current
UNION ALL SELECT 'silver_policies', COUNT(*) FROM actuarial.sample.policies
UNION ALL SELECT 'gold_event_loss', COUNT(*) FROM actuarial.sample.event_loss_summary
UNION ALL SELECT 'gold_claims_summary', COUNT(*) FROM actuarial.sample.claims_summary
UNION ALL SELECT 'gold_inventory', COUNT(*) FROM actuarial.sample.medallion_inventory;

SELECT * FROM actuarial.sample.event_loss_summary ORDER BY total_incurred DESC;
SELECT * FROM actuarial.sample.medallion_inventory ORDER BY layer, table_name;
```

Landing files:

```sql
LIST '/Volumes/actuarial/sample/landing/claims';
```

---

## Reference map

| Concept | Path in this repo |
|---------|-------------------|
| Bundle / targets / `warehouse_id` | [`databricks.yml`](../databricks.yml) |
| Spec format | [`src/pipeline_configs/global.json`](../src/pipeline_configs/global.json) |
| Substitutions | [`src/pipeline_configs/dev_substitutions.yaml`](../src/pipeline_configs/dev_substitutions.yaml) |
| Landing volume | [`resources/landing.volume.yml`](../resources/landing.volume.yml) |
| Setup / land notebooks | [`src/notebooks/`](../src/notebooks/) |
| Bronze cloudFiles specs | [`src/dataflows/bronze/dataflowspec/`](../src/dataflows/bronze/dataflowspec/) |
| Silver SCD1 + MV | [`src/dataflows/silver/`](../src/dataflows/silver/) |
| Gold mart SQL + MV | [`src/dataflows/gold/`](../src/dataflows/gold/) |
| Bronze / silver / gold pipelines (+ `event_log`) | [`resources/claims_*_pipeline.yml`](../resources/) |
| Orchestrating job | [`resources/claims_pipeline_job.job.yml`](../resources/claims_pipeline_job.job.yml) |
| Lakeview dashboard resources | [`resources/*.dashboard.yml`](../resources/) |
| Lakeview JSON definitions | [`src/dashboards/*.lvdash.json`](../src/dashboards/) |
| Bundle / dashboard contract tests | [`tests/test_bundle_config.py`](../tests/test_bundle_config.py) |
| Framework runtime (deploy separately) | [`../../lakeflow_framework`](../../lakeflow_framework/) |

### Common pitfalls

1. **Framework not deployed** — pipeline library path 404s; deploy `lakeflow_framework` first.
2. **Catalog/schema missing at deploy** — volume resource fails; create `actuarial.sample` before `bundle deploy`.
3. **Wrong `dataFlowGroupFilter`** — specs silently skipped; group names must match (`claims_bronze` / `claims_silver` / `claims_gold`).
4. **`live.` vs FQN** — use `live.` only for tables in the **same** pipeline; gold reads silver via `{silver_schema}.…`.
5. **File vs target schema** — Auto Loader source schema must not require `_ingest_ts` / `_source_file` columns from the CSV.
6. **Hardcoded catalog.schema in `.lvdash.json`** — use unqualified names; set `dataset_catalog` / `dataset_schema` on the dashboard resource.
7. **Empty event-log dashboard** — publish `event_log` on each pipeline and refresh before opening; do not delete the UC event log tables.
8. **Missing `warehouse_id`** — Lakeview resources fail validation/deploy without a SQL warehouse ID on the target.
