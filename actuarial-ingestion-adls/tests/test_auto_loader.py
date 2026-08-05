"""Unit tests for Auto Loader path helpers and dataset registry (no Spark)."""

from actuarial_ingestion_adls.auto_loader import (
    CLAIMS_SCHEMA_HINTS,
    CYCLONE_EVENTS_SCHEMA_HINTS,
    DATASETS,
    PREMIUMS_SCHEMA_HINTS,
    RISK_ZONES_SCHEMA_HINTS,
    dataset_checkpoint_location,
    dataset_schema_location,
    dataset_source_path,
    full_table_name,
    normalize_path,
)


def test_normalize_path_strips_trailing_slash():
    assert normalize_path("abfss://x/y/") == "abfss://x/y"
    assert normalize_path("abfss://x/y") == "abfss://x/y"


def test_dataset_paths():
    landing = "abfss://metastore@acct.dfs.core.windows.net/actuarial/ingestion/landing/"
    state = "abfss://metastore@acct.dfs.core.windows.net/actuarial/ingestion/_autoloader/"
    assert dataset_source_path(landing, "claims") == (
        "abfss://metastore@acct.dfs.core.windows.net/actuarial/ingestion/landing/claims"
    )
    assert dataset_schema_location(state, "claims") == (
        "abfss://metastore@acct.dfs.core.windows.net/actuarial/ingestion/_autoloader/claims/schema"
    )
    assert dataset_checkpoint_location(state, "claims") == (
        "abfss://metastore@acct.dfs.core.windows.net/actuarial/ingestion/_autoloader/claims/checkpoints"
    )


def test_full_table_name():
    assert full_table_name("actuarial", "ingestion", "bronze_claims_bordereau") == (
        "actuarial.ingestion.bronze_claims_bordereau"
    )


def test_datasets_cover_four_bronze_tables():
    by_subdir = {d.subdir: d for d in DATASETS}
    assert set(by_subdir) == {"claims", "premiums", "risk_zones", "cyclone_events"}
    assert by_subdir["claims"].table_name == "bronze_claims_bordereau"
    assert by_subdir["premiums"].table_name == "bronze_premium_bordereau"
    assert by_subdir["risk_zones"].table_name == "bronze_risk_zone_lookup"
    assert by_subdir["cyclone_events"].table_name == "bronze_cyclone_events"


def test_schema_hints_include_key_columns():
    assert "claim_id STRING" in CLAIMS_SCHEMA_HINTS
    assert "policy_id STRING" in PREMIUMS_SCHEMA_HINTS
    assert "postcode INT" in RISK_ZONES_SCHEMA_HINTS
    assert "event_id STRING" in CYCLONE_EVENTS_SCHEMA_HINTS
