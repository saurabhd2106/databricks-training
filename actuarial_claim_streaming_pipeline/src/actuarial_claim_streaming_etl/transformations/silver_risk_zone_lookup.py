from pyspark import pipelines as dp

from actuarial_claim_streaming_pipeline.silver import transform_silver_risk_zones

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@dp.expect_or_drop("postcode_not_null", "postcode IS NOT NULL")
@materialized_view(
    name="silver_risk_zone_lookup",
    comment="Deduped postcode risk zones (Materialized View from clean bronze).",
    cluster_by_auto=True,
)
def silver_risk_zone_lookup():
    return transform_silver_risk_zones(spark.read.table("bronze_risk_zone_lookup"))
