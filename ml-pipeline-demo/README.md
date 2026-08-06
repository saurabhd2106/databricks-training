# ml-pipeline-demo

Early **claim severity** machine learning demo on Databricks.

At first report of a cyclone-related claim, predict the **ultimate closed incurred amount** using first-snapshot claim fields plus policy risk attributes. The bundle lands sample CSVs, builds a leakage-safe feature table, trains with MLflow, registers the model in Unity Catalog, and batch-scores predictions.

**Trainer walkthrough (start here for teaching):** [`docs/TRAINING_GUIDE.md`](docs/TRAINING_GUIDE.md)

Infrastructure for the workspace, Shared all-purpose cluster, and `actuarial` catalog comes from [`deploy-databricks-azure`](../deploy-databricks-azure/). Sample CSVs are copied from repo [`sample-data`](../sample-data/) into `fixtures/sample-data/`.

## Flow

```text
fixtures/sample-data/*.csv
        │
        ▼
 Job: setup → land → build_features          (Shared cluster)
        │
        ▼
 {catalog}.{schema}.claim_severity_features
        │
        ▼
 Job: train_and_evaluate → batch_score       (Dedicated ML Runtime)
        │
        ├── MLflow experiment
        ├── UC model {catalog}.{schema}.ultimate_claim_severity
        └── {catalog}.{schema}.claim_severity_predictions

Dev mode prefixes the schema (e.g. actuarial.dev_databricks_np_ml).
Prod uses actuarial.ml.
```

## Prerequisites

1. Databricks CLI authenticated to `https://adb-7405611775215693.13.azuredatabricks.net`
2. Unity Catalog catalog `actuarial` ([`deploy-databricks-azure`](../deploy-databricks-azure/))
3. Shared all-purpose cluster ID in `databricks.yml` → `cluster_id` (default matches sibling projects)
4. **Dedicated ML Runtime** single-node cluster (access mode **Dedicated**, your user):
   - Compute → Create compute → Runtime **…-ml-…** → Access mode **Dedicated** → single node
   - Copy the cluster ID into `databricks.yml` → `ml_cluster_id` (replace `REPLACE_WITH_DEDICATED_ML_CLUSTER_ID`)
5. Ability to create schema/volume/tables/models under `actuarial.ml` (setup notebook grants `CREATE MODEL` when you have rights)

## Deploy and run

```bash
cd ml-pipeline-demo

# Edit databricks.yml: set ml_cluster_id to your Dedicated ML cluster

databricks bundle validate -t dev
databricks bundle deploy -t dev

# Start Shared + ML clusters in the UI, then:
databricks bundle run ml_pipeline_job -t dev
```

### GitHub Actions (CI/CD)

Workflow: [`.github/workflows/ml-pipeline-demo.yml`](../.github/workflows/ml-pipeline-demo.yml)

1. Repo secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
2. Start the Shared and Dedicated ML Runtime clusters named in `databricks.yml`
3. Actions → **ML pipeline demo** → Run workflow
   - **target:** `prod` (schema `actuarial.ml`) or `dev` (prefixed schema)
   - **run_job:** train / register / batch-score after deploy (default on)

Pipeline: pytest → `bundle validate` → `bundle deploy` → `bundle run ml_pipeline_job` (registers `ultimate_claim_severity` and writes predictions).
## Verify

```sql
-- Dev target example (schema is prefixed). Prod uses schema `ml`.
SELECT COUNT(*) FROM actuarial.dev_databricks_np_ml.claim_severity_features;  -- ~1156
SELECT * FROM actuarial.dev_databricks_np_ml.claim_severity_predictions
ORDER BY uplift_vs_first DESC
LIMIT 20;
```

- **Experiments:** `/Shared/ml-pipeline-demo/ultimate_claim_severity`
- **Model:** Catalog Explorer → `actuarial` → `dev_databricks_np_ml` (dev) or `ml` (prod) → Models → `ultimate_claim_severity`

## Local tests

```bash
cd ml-pipeline-demo
pip install -e ".[dev]"
pytest -q
```

## Repository layout

```text
ml-pipeline-demo/
├── README.md
├── docs/TRAINING_GUIDE.md
├── databricks.yml
├── fixtures/sample-data/
├── resources/                 # schema, volume, job
├── src/
│   ├── 00_setup.ipynb … 04_batch_score.ipynb
│   └── ml_pipeline_demo/      # feature helpers
└── tests/
```

## Demo script (short)

1. `databricks bundle deploy -t dev`
2. Start both clusters; `databricks bundle run ml_pipeline_job -t dev`
3. Show `claim_severity_features` row count (~1.1k)
4. Open the MLflow experiment → MAE / RMSE / R² + model artifact
5. Catalog Explorer → model `actuarial.ml.ultimate_claim_severity`
6. Query predictions; highlight rows where `predicted_ultimate` ≫ `first_incurred`

Full facilitator script: [`docs/TRAINING_GUIDE.md`](docs/TRAINING_GUIDE.md) §12.
