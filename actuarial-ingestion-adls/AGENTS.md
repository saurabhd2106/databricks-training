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

- This bundle demonstrates **batch incremental bronze ingest** from ADLS via Auto Loader (`cloudFiles`) + `.trigger(availableNow=True)`.
- Publish target: Unity Catalog `actuarial.ingestion` (isolated from `actuarial.dev`/`prod` overwrite jobs and `actuarial.streaming` Lakeflow tables).
- Landing and Autoloader state use real **`abfss://`** paths under the `actuarial-uc-location` external location — set `landing_path` and `autoloader_state_path` in `databricks.yml`.
- **Do** set `cloudFiles.schemaLocation` and `checkpointLocation` (unlike Lakeflow Streaming Tables, where the pipeline owns them).
- Use `trigger(availableNow=True)` so each job run processes new files then stops — do not leave continuous streams running.
- Claims landing is append-only (`claims_batch_0N.csv`); do not overwrite already-ingested batch filenames for the incremental demo.
- Reset ingest state by clearing the dataset dirs under `autoloader_state_path` (and optionally dropping bronze tables), not by re-running with overwrite.
