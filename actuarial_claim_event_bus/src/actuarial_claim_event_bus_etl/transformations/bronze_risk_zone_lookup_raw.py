from pyspark import pipelines as dp

from actuarial_claim_event_bus.kafka_source import read_event_hub_json


@dp.expect("_parse_error_null", "_parse_error IS NULL")
@dp.table(
    name="bronze_risk_zone_lookup_raw",
    comment="Raw Event Hubs risk-zone ingest (no row drops; quarantine captures failures).",
    cluster_by_auto=True,
)
def bronze_risk_zone_lookup_raw():
    return read_event_hub_json(spark, "risk_zones")
