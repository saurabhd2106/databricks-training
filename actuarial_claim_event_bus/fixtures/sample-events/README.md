# sample-events

JSON Lines fixtures for seeding Azure Event Hubs topics (one JSON object per line).

| Directory | Topic (default) | Notes |
|-----------|-----------------|-------|
| `claims/` | `actuarial.claims` | Subset of streaming `claims_batch_01` + bad rows |
| `premiums/` | `actuarial.premiums` | First 25 premium rows |
| `risk_zones/` | `actuarial.risk_zones` | Full risk zone lookup |
| `cyclone_events/` | `actuarial.cyclone_events` | Full cyclone events |

Use `seed_event_hubs.ipynb` (or the job task) to publish these lines to Event Hubs.
