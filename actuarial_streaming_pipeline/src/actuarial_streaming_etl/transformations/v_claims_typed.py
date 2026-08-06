from actuarial_streaming_pipeline.pipeline_decorators import temporary_view
from actuarial_streaming_pipeline.silver import transform_typed_claims


@temporary_view(name="v_claims_typed")
def v_claims_typed():
    """Pipeline-scoped typed claims (not persisted to Unity Catalog)."""
    return transform_typed_claims(spark.read.table("bronze_claims_bordereau"))
