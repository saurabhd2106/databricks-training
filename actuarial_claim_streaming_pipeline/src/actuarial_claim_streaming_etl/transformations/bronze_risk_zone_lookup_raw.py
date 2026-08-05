from pyspark import pipelines as dp

from actuarial_claim_streaming_pipeline.auto_loader import RISK_ZONES_SCHEMA_HINTS, read_landing_csv


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.table(
    name="bronze_risk_zone_lookup_raw",
    comment="Raw Auto Loader risk zone ingest (no row drops; quarantine captures failures).",
    cluster_by_auto=True,
)
def bronze_risk_zone_lookup_raw():
    return read_landing_csv(spark, "risk_zones", schema_hints=RISK_ZONES_SCHEMA_HINTS)
