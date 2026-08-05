from pyspark import pipelines as dp


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.expect_or_drop("policy_id_not_null", "policy_id IS NOT NULL")
@dp.table(
    name="bronze_premium_bordereau",
    comment="Clean streaming bronze premiums (null policy_id dropped; see quarantine_bronze_premium_bordereau).",
    cluster_by_auto=True,
)
def bronze_premium_bordereau():
    return spark.readStream.table("bronze_premium_bordereau_raw")
