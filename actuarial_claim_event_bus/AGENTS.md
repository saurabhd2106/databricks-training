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

- Bronze-only Event Hubs (Kafka protocol) ingest into `actuarial.event_bus`.
- Lakeflow **Streaming Tables** only: bronze raw / clean / quarantine (no silver/gold).
- Pipeline is **continuous** (`continuous: true`) and serverless.
- Do **not** set Kafka checkpoint paths — Lakeflow manages Streaming Table state.
- Auth via pipeline config `eh_jaas_config` (Databricks secret `{{secrets/actuarial-event-bus/eh-jaas}}`).
- Shared helpers live in `actuarial_claim_event_bus` (`kafka_source`, `schemas`, `quarantine`).
- CI: `.github/workflows/actuarial-claim-event-bus.yml` (`workflow_dispatch`).
