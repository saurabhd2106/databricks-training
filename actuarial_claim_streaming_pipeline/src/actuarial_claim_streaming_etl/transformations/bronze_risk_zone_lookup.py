from pyspark import pipelines as dp


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.expect_or_drop("postcode_not_null", "postcode IS NOT NULL")
@dp.table(
    name="bronze_risk_zone_lookup",
    comment="Clean streaming bronze risk zones (null postcode dropped; see quarantine_bronze_risk_zone_lookup).",
    cluster_by_auto=True,
)
def bronze_risk_zone_lookup():
    return spark.readStream.table("bronze_risk_zone_lookup_raw")
