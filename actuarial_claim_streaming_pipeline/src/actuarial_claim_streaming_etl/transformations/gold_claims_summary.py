from actuarial_claim_streaming_pipeline.gold import build_gold_claims_summary

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@materialized_view(
    name="gold_claims_summary",
    comment="Claims frequency/severity/settlement by peril, status, and risk dims.",
    cluster_by_auto=True,
)
def gold_claims_summary():
    claims = spark.read.table("silver_claims_current")
    premiums = spark.read.table("silver_premium_bordereau")
    return build_gold_claims_summary(claims, premiums)
