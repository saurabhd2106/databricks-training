from actuarial_claim_streaming_pipeline.gold import build_gold_portfolio_exposure

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@materialized_view(
    name="gold_portfolio_exposure",
    comment="Portfolio sum insured and premium exposure by risk dims.",
    cluster_by_auto=True,
)
def gold_portfolio_exposure():
    premiums = spark.read.table("silver_premium_bordereau")
    risk_zones = spark.read.table("silver_risk_zone_lookup")
    return build_gold_portfolio_exposure(premiums, risk_zones)
