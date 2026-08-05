"""Unit tests for gold mart builders (Databricks Connect)."""

from actuarial_claim_streaming_pipeline.gold import (
    build_gold_claims_summary,
    build_gold_portfolio_exposure,
)
from actuarial_claim_streaming_pipeline.silver import (
    apply_claims_quality,
    apply_premiums_quality,
    transform_silver_risk_zones,
    transform_typed_claims,
    transform_typed_premiums,
)
from helpers import (
    CLAIMS_SCHEMA,
    PREMIUMS_SCHEMA,
    RISK_ZONE_SCHEMA,
    claim_row,
    premium_row,
    risk_zone_row,
)


def test_gold_claims_summary_non_empty(spark):
    claims = apply_claims_quality(
        transform_typed_claims(spark.createDataFrame([claim_row()], schema=CLAIMS_SCHEMA))
    )
    premiums = apply_premiums_quality(
        transform_typed_premiums(spark.createDataFrame([premium_row()], schema=PREMIUMS_SCHEMA))
    )
    gold = build_gold_claims_summary(claims, premiums)
    assert gold.count() >= 1
    assert "claim_count" in gold.columns
    assert "gold_ingestion_timestamp" in gold.columns


def test_gold_portfolio_exposure_inner_join(spark):
    premiums = apply_premiums_quality(
        transform_typed_premiums(
            spark.createDataFrame(
                [
                    premium_row(policy_id="POL-1", postcode=4870),
                    premium_row(policy_id="POL-2", postcode=9999),
                ],
                schema=PREMIUMS_SCHEMA,
            )
        )
    )
    zones = transform_silver_risk_zones(
        spark.createDataFrame([risk_zone_row(postcode=4870)], schema=RISK_ZONE_SCHEMA)
    )
    gold = build_gold_portfolio_exposure(premiums, zones)
    assert gold.count() >= 1
    assert gold.selectExpr("sum(policy_count) as n").collect()[0].n == 1
