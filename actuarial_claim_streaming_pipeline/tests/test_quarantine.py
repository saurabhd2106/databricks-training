"""Unit tests for bronze quarantine helpers (Databricks Connect)."""

from actuarial_claim_streaming_pipeline.auto_loader import quarantine_from_raw
from helpers import CLAIMS_SCHEMA, claim_row


def test_quarantine_captures_null_key_and_rescued(spark):
    raw = spark.createDataFrame(
        [
            claim_row(claim_id="CLM-OK"),
            claim_row(claim_id=None, policy_id="POL-X"),
            claim_row(claim_id="CLM-RESCUED", _rescued_data='{"bad":1}'),
        ],
        schema=CLAIMS_SCHEMA,
    )
    quarantined = quarantine_from_raw(raw, "claim_id")
    rows = quarantined.collect()
    assert len(rows) == 2
    assert "quarantine_reason" in quarantined.columns
    assert "_quarantine_ts" in quarantined.columns
    reasons = {r.claim_id: r.quarantine_reason for r in rows}
    assert "claim_id_null" in (reasons[None] or "")
    assert "rescued_data" in reasons["CLM-RESCUED"]
