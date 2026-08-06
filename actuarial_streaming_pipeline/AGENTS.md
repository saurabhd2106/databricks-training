# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (DABs) for deployment. Add project-specific instructions below.

## For AI Agents: Use Databricks AI Tools

**BEFORE any other action, read the `databricks-core` skill.**

It sets you up to work with this project reliably: CLI authentication, profile
selection, data discovery, and the bundle deployment workflow. Without it,
results are often slower and less accurate.

If this skill is not available (Databricks AI Tools are not installed), you can install them for your coding agent in seconds:

```bash
databricks aitools install
```

If the CLI is not installed, see: https://docs.databricks.com/dev-tools/cli/install

---

## Project Instructions

- Target UC location: `actuarial.dev` with managed volume `landing`
  (`/Volumes/actuarial/dev/landing`).
- Job `actuarial_streaming_job` (serverless): `setup` → `land_sample_data` →
  `refresh_pipeline`. Default `claims_batch` is `all`.
- Pipeline `actuarial_streaming_etl` (serverless, triggered) demonstrates:
  - **Streaming Tables** — bronze raw / clean / quarantine via Auto Loader
  - **Temporary views** — `v_claims_typed`, `v_premiums_typed` (pipeline DAG only)
  - **Materialized Views** — silver cleanses (`silver_*`)
- Do **not** set `cloudFiles.schemaLocation` or checkpoint paths — Lakeflow
  manages them.
- One dataset definition per file under
  `src/actuarial_streaming_etl/transformations/`.
- Use `actuarial_streaming_pipeline.pipeline_decorators` for `temporary_view` /
  `materialized_view` with runtime fallbacks.
- Shared helpers live in the wheel package
  `src/actuarial_streaming_pipeline/` (built via `artifacts.python_artifact`).
- Dataset functions must return a DataFrame only — no `save` / `collect` side
  effects. File landing belongs in the land notebook, not the pipeline.
- Bronze keys for quarantine: claims=`claim_id`, premiums=`policy_id`,
  risk_zones=`postcode`, cyclone=`event_id`.
