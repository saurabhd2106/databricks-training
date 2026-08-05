from actuarial_claim_streaming_pipeline.gold import build_gold_claims_development

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@materialized_view(
    name="gold_claims_development",
    comment="Reporting lag and reserve development from claim snapshots.",
    cluster_by_auto=True,
)
def gold_claims_development():
    claims = spark.read.table("silver_claims_bordereau")
    premiums = spark.read.table("silver_premium_bordereau")
    return build_gold_claims_development(claims, premiums)
