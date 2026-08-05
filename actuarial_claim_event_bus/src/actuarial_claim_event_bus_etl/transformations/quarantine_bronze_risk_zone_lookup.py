from pyspark import pipelines as dp

from actuarial_claim_event_bus.quarantine import quarantine_from_raw


@dp.table(
    name="quarantine_bronze_risk_zone_lookup",
    comment="Risk-zone rows failing bronze expectations (parse error or null postcode).",
    cluster_by_auto=True,
)
def quarantine_bronze_risk_zone_lookup():
    return quarantine_from_raw(spark.readStream.table("bronze_risk_zone_lookup_raw"), "postcode")
