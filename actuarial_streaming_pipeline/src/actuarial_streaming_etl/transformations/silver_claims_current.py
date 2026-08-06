from actuarial_streaming_pipeline.pipeline_decorators import materialized_view
from actuarial_streaming_pipeline.silver import transform_silver_claims_current


@materialized_view(
    name="silver_claims_current",
    comment="Latest claim snapshot per claim_id (Materialized View).",
    cluster_by_auto=True,
)
def silver_claims_current():
    return transform_silver_claims_current(spark.read.table("silver_claims_bordereau"))
