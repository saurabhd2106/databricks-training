from actuarial_claim_streaming_pipeline.gold import build_gold_loss_ratio_by_risk

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@materialized_view(
    name="gold_loss_ratio_by_risk",
    comment="Loss ratio by insurer, region, wind risk band, building type, mitigation.",
    cluster_by_auto=True,
)
def gold_loss_ratio_by_risk():
    premiums = spark.read.table("silver_premium_bordereau")
    claims = spark.read.table("silver_claims_current")
    return build_gold_loss_ratio_by_risk(premiums, claims)
