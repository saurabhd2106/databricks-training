"""Unit tests for bronze quarantine helpers (Databricks Connect)."""

import pytest
from helpers import (
    CLAIMS_SCHEMA,
    EVENTS_SCHEMA,
    PREMIUMS_SCHEMA,
    RISK_ZONE_SCHEMA,
    claim_row,
    event_row,
    premium_row,
    risk_zone_row,
)

from actuarial_streaming_pipeline.auto_loader import quarantine_from_raw


@pytest.mark.parametrize(
    ("key_col", "schema", "ok_row", "null_key_row", "rescued_row", "id_attr"),
    [
        (
            "claim_id",
            CLAIMS_SCHEMA,
            claim_row(claim_id="CLM-OK"),
            claim_row(claim_id=None, policy_id="POL-X"),
            claim_row(claim_id="CLM-RESCUED", _rescued_data='{"bad":1}'),
            "claim_id",
        ),
        (
            "policy_id",
            PREMIUMS_SCHEMA,
            premium_row(policy_id="POL-OK"),
            premium_row(policy_id=None),
            premium_row(policy_id="POL-RESCUED", _rescued_data='{"bad":1}'),
            "policy_id",
        ),
        (
            "postcode",
            RISK_ZONE_SCHEMA,
            risk_zone_row(postcode=4870),
            risk_zone_row(postcode=None),
            risk_zone_row(postcode=4000, _rescued_data='{"bad":1}'),
            "postcode",
        ),
        (
            "event_id",
            EVENTS_SCHEMA,
            event_row(event_id="CYC-OK"),
            event_row(event_id=None),
            event_row(event_id="CYC-RESCUED", _rescued_data='{"bad":1}'),
            "event_id",
        ),
    ],
)
def test_quarantine_captures_null_key_and_rescued(
    spark, key_col, schema, ok_row, null_key_row, rescued_row, id_attr
):
    raw = spark.createDataFrame([ok_row, null_key_row, rescued_row], schema=schema)
    quarantined = quarantine_from_raw(raw, key_col)
    rows = quarantined.collect()
    assert len(rows) == 2
    assert "quarantine_reason" in quarantined.columns
    assert "_quarantine_ts" in quarantined.columns

    by_id = {getattr(r, id_attr): r.quarantine_reason for r in rows}
    assert f"{key_col}_null" in (by_id[None] or "")
    assert "rescued_data" in by_id[rescued_row[id_attr]]


def test_quarantine_combined_reason_when_rescued_and_null_key(spark):
    raw = spark.createDataFrame(
        [claim_row(claim_id=None, _rescued_data='{"bad":1}')],
        schema=CLAIMS_SCHEMA,
    )
    quarantined = quarantine_from_raw(raw, "claim_id")
    rows = quarantined.collect()
    assert len(rows) == 1
    reason = rows[0].quarantine_reason
    assert "rescued_data" in reason
    assert "claim_id_null" in reason
    assert reason == "rescued_data,claim_id_null"
