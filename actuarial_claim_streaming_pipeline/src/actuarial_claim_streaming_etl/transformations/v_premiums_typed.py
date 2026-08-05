from actuarial_claim_streaming_pipeline.silver import transform_typed_premiums

from actuarial_claim_streaming_pipeline.pipeline_decorators import temporary_view


@temporary_view(name="v_premiums_typed")
def v_premiums_typed():
    """Pipeline-scoped typed premiums (not persisted to Unity Catalog)."""
    return transform_typed_premiums(spark.read.table("bronze_premium_bordereau"))
