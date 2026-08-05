from pyspark import pipelines as dp

from actuarial_claim_streaming_pipeline.silver import apply_claims_quality

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@dp.expect_or_drop("claim_id_not_null", "claim_id IS NOT NULL")
@dp.expect_or_drop("policy_id_not_null", "policy_id IS NOT NULL")
@dp.expect("reported_on_or_after_loss", "reported_date >= date_of_loss")
@dp.expect_or_drop("incurred_gte_paid", "incurred_amount >= paid_to_date")
@materialized_view(
    name="silver_claims_bordereau",
    comment="Typed, quality-filtered claim snapshots (Materialized View from v_claims_typed).",
    cluster_by_auto=True,
)
def silver_claims_bordereau():
    return apply_claims_quality(spark.read.table("v_claims_typed"))
