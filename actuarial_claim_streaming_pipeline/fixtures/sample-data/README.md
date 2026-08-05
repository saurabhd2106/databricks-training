# Sample data for streaming bronze demo

Prepared from the repo-root [`sample-data`](../../../sample-data/) folder.

## Layout

| Path | Contents |
|------|----------|
| `claims/claims_batch_01.csv` … `_03.csv` | Claims bordereau split into 3 equal chunks (944 rows each) |
| `premiums/premium_bordereau.csv` | Full premium bordereau (~5,003 policies) |
| `risk_zones/risk_zone_lookup.csv` | Postcode → region / wind risk band |
| `cyclone_events/cyclone_events.csv` | Named cyclone events |

## `claims_batch` job / notebook widget

| Value | What gets landed into `{landing_path}/claims/` |
|-------|-----------------------------------------------|
| `01` (default) | `claims_batch_01.csv` only |
| `02` | `claims_batch_02.csv` only |
| `03` | `claims_batch_03.csv` only |
| `all` | All three claim batch files |

Premiums, risk zones, and cyclone events are always landed (stable filenames). Claims use **new filenames per batch** so Auto Loader can demonstrate incremental ingest. Do not overwrite an already-ingested `claims_batch_0N.csv` mid-demo.

See the project [README](../../README.md) for the full streaming walkthrough.
