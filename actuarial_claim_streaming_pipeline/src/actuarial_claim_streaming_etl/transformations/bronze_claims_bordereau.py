from pyspark import pipelines as dp


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.expect_or_drop("claim_id_not_null", "claim_id IS NOT NULL")
@dp.table(
    name="bronze_claims_bordereau",
    comment="Clean streaming bronze claims (null claim_id dropped; see quarantine_bronze_claims_bordereau).",
    cluster_by_auto=True,
)
def bronze_claims_bordereau():
    return spark.readStream.table("bronze_claims_bordereau_raw")
