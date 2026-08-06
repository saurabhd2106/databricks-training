from pyspark import pipelines as dp

from actuarial_streaming_pipeline.auto_loader import quarantine_from_raw


@dp.table(
    name="quarantine_bronze_claims_bordereau",
    comment="Claims rows failing bronze expectations (rescued_data or null claim_id).",
    cluster_by_auto=True,
)
def quarantine_bronze_claims_bordereau():
    return quarantine_from_raw(spark.readStream.table("bronze_claims_bordereau_raw"), "claim_id")
