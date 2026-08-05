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

- Demonstrates all three Lakeflow dataset types in `actuarial.streaming`:
  - **Streaming Tables** — bronze raw / clean / quarantine via Auto Loader
  - **Temporary views** — `v_claims_typed`, `v_premiums_typed` (pipeline DAG only; not UC tables)
  - **Materialized Views** — silver cleanses + gold actuarial marts
- Do **not** set `cloudFiles.schemaLocation` or checkpoint paths — Lakeflow manages them.
- Claims landing is append-only (`claims_batch_0N.csv`); do not overwrite already-ingested batch filenames for the incremental demo.
- Use `actuarial_claim_streaming_pipeline.pipeline_decorators` for `temporary_view` / `materialized_view` with runtime fallbacks.
- CI: `.github/workflows/actuarial-claim-streaming-pipeline.yml` (`workflow_dispatch`).
