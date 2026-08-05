from pyspark import pipelines as dp

from actuarial_claim_event_bus.quarantine import quarantine_from_raw


@dp.table(
    name="quarantine_bronze_premium_bordereau",
    comment="Premium rows failing bronze expectations (parse error or null policy_id).",
    cluster_by_auto=True,
)
def quarantine_bronze_premium_bordereau():
    return quarantine_from_raw(spark.readStream.table("bronze_premium_bordereau_raw"), "policy_id")
