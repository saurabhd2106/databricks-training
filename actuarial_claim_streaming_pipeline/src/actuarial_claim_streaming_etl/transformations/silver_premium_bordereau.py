from pyspark import pipelines as dp

from actuarial_claim_streaming_pipeline.silver import apply_premiums_quality

from actuarial_claim_streaming_pipeline.pipeline_decorators import materialized_view


@dp.expect_or_drop("policy_id_not_null", "policy_id IS NOT NULL")
@dp.expect("positive_sum_insured", "sum_insured > 0")
@dp.expect("positive_annual_premium", "annual_premium > 0")
@materialized_view(
    name="silver_premium_bordereau",
    comment="Typed, quality-filtered policies (Materialized View from v_premiums_typed).",
    cluster_by_auto=True,
)
def silver_premium_bordereau():
    return apply_premiums_quality(spark.read.table("v_premiums_typed"))
