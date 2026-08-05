from actuarial_claim_streaming_pipeline.gold import build_gold_event_loss_summary

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@materialized_view(
    name="gold_event_loss_summary",
    comment="Cat vs non-cat loss summary by cyclone event and region.",
    cluster_by_auto=True,
)
def gold_event_loss_summary():
    claims = spark.read.table("silver_claims_current")
    events = spark.read.table("silver_cyclone_events")
    premiums = spark.read.table("silver_premium_bordereau")
    return build_gold_event_loss_summary(claims, events, premiums)
