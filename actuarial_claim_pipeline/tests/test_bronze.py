"""Unit tests for bronze ingest helpers."""

from types import SimpleNamespace

import pytest

from actuarial_claim_pipeline.bronze import (
    bronze_table_name,
    filter_csv_files,
    ingest_volume_csvs,
)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("claims_bordereau.csv", "bronze_claims_bordereau"),
        ("Claims Bordereau.csv", "bronze_claims_bordereau"),
        ("claims-bordereau.CSV", "bronze_claims_bordereau"),
        ("risk_zone_lookup.csv", "bronze_risk_zone_lookup"),
    ],
)
def test_bronze_table_name(filename, expected):
    assert bronze_table_name(filename) == expected


def test_filter_csv_files_keeps_only_csv():
    files = [
        SimpleNamespace(name="claims_bordereau.csv", path="/v/claims_bordereau.csv"),
        SimpleNamespace(name="readme.txt", path="/v/readme.txt"),
        SimpleNamespace(name="NOTES.CSV", path="/v/NOTES.CSV"),
        SimpleNamespace(name="image.png", path="/v/image.png"),
    ]
    csv_files = filter_csv_files(files)
    assert [f.name for f in csv_files] == ["claims_bordereau.csv", "NOTES.CSV"]


def test_ingest_empty_volume_raises(spark):
    class EmptyFs:
        @staticmethod
        def ls(_path):
            return [SimpleNamespace(name="readme.txt", path="/vol/readme.txt")]

    dbutils = SimpleNamespace(fs=EmptyFs())

    with pytest.raises(FileNotFoundError, match="No CSV files found"):
        ingest_volume_csvs(
            spark,
            dbutils,
            catalog="actuarial",
            schema="dev",
            volume_path="/Volumes/actuarial/dev/raw_files",
        )


def test_load_fixture_claims_row_fidelity(load_fixture):
    df = load_fixture("claims_bordereau.csv")
    assert df.count() == 10
    assert "claim_id" in df.columns
    assert "policy_id" in df.columns
    assert df.filter("claim_id = 'CLM-001'").count() == 1
